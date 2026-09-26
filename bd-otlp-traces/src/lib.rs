// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

//! Builds an OTLP/HTTP JSON payload for a single completed, traced HTTP request.
//!
//! This is intentionally the *only* thing this crate does: given a flat, platform-neutral set of
//! span/resource fields (already gathered by the platform -- trace ID, span ID, timing, status,
//! and OS-specific attributes it already collects for other purposes), produce the exact OTLP
//! wire bytes an OTLP/HTTP collector (e.g. ClickStack/HyperDX) expects, as `Vec<u8>`.
//!
//! What this crate deliberately does *not* do, and why:
//! - It does not perform the HTTP POST itself, and holds no configuration (endpoint, auth header).
//!   The mobile SDK's Rust core never vendors an HTTP/TLS client into the shipped binary -- every
//!   platform already has its own native HTTP stack (OkHttp/URLSession) and performs the actual
//!   request itself. See `.plans/otel-span-export-plan.md` (shared-core) /
//!   `docs/agent-tasks/otel-span-export-plan.md` (capture-sdk) for the full design rationale.
//! - It does not queue, retry, or persist anything. A span is only useful while its trace is still
//!   assemblable by the observability backend -- unlike a log line, a span delivered late (e.g.
//!   after the device reconnects) has effectively no value, so there is nothing here to make
//!   "offline-resilient" the way `bd-buffer`'s log pipeline is. One attempt; the caller drops the
//!   payload on failure.
//! - It emits OTLP's *JSON* encoding by hand (via `serde`), not real Protobuf, and not the generic
//!   `opentelemetry`/`opentelemetry-otlp` crates already used elsewhere in this workspace (see
//!   `bd-log/src/otel.rs`) -- those are built for bitdrift's own Rust services to emit their own
//!   traces asynchronously over a real network client (`reqwest`/`tonic`), which is exactly the
//!   dependency shape the mobile binary avoids.
//!
//!   `traceId`/`spanId` are encoded here as plain hex strings, and `kind`/`status.code` as raw
//!   integers rather than enum name strings. This is not a workaround for collector leniency --
//!   it is what the OTLP JSON specification itself requires. OTLP's JSON Protobuf Encoding
//!   (<https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md>)
//!   explicitly overrides generic protobuf-JSON mapping in exactly these ways: `traceId`/`spanId`
//!   are "case-insensitive hex-encoded strings" (not the base64 a naive/generic protobuf-JSON
//!   serializer would produce for a `bytes` field), and enum fields "MUST be encoded as integer
//!   values," never as name strings. A hand-written encoder that gets this right isn't cutting a
//!   corner relative to a "real" Protobuf/`protobuf-json-mapping`-based implementation -- a
//!   generic protobuf-JSON serializer applied naively to OTLP's own `.proto` definitions would
//!   need this exact same special-casing to be spec-compliant. Field names use lowerCamelCase
//!   (also spec-mandated), and 64-bit integer fields (`startTimeUnixNano`/`endTimeUnixNano`, and
//!   integer attribute values) are encoded as strings, which *is* unmodified standard proto3 JSON
//!   mapping (JSON numbers can't safely round-trip a full 64-bit integer) -- OTLP does not
//!   override that rule.

#![deny(
  clippy::expect_used,
  clippy::panic,
  clippy::todo,
  clippy::unimplemented,
  clippy::unreachable,
  clippy::unwrap_used
)]

use serde::Serialize;

//
// AttributeValue
//

/// One OTLP `AnyValue` -- deliberately a small, closed set matching what a platform can already
/// derive from primitive fields (strings, integers, booleans). Extend this if a future attribute
/// needs a shape not covered here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributeValue {
  String(String),
  Int(i64),
  Bool(bool),
}

//
// Attribute
//

/// One OTLP `KeyValue` -- used for both span attributes and resource attributes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribute {
  pub key: String,
  pub value: AttributeValue,
}

impl Attribute {
  #[must_use]
  pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
    Self {
      key: key.into(),
      value: AttributeValue::String(value.into()),
    }
  }

  #[must_use]
  pub fn int(key: impl Into<String>, value: i64) -> Self {
    Self {
      key: key.into(),
      value: AttributeValue::Int(value),
    }
  }

  #[must_use]
  pub fn bool(key: impl Into<String>, value: bool) -> Self {
    Self {
      key: key.into(),
      value: AttributeValue::Bool(value),
    }
  }
}

//
// SpanKind
//

/// OTLP's `Span.SpanKind` enum. Every traced network request this crate builds a payload for is
/// `Client`, but the full enum is modeled (rather than a single hardcoded constant) since the
/// numeric values are part of the wire contract and cheap to get right once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpanKind {
  Unspecified,
  Internal,
  Server,
  Client,
  Producer,
  Consumer,
}

impl SpanKind {
  const fn wire_value(self) -> i32 {
    match self {
      Self::Unspecified => 0,
      Self::Internal => 1,
      Self::Server => 2,
      Self::Client => 3,
      Self::Producer => 4,
      Self::Consumer => 5,
    }
  }
}

//
// StatusCode
//

/// OTLP's `Status.StatusCode` enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusCode {
  Unset,
  Ok,
  Error,
}

impl StatusCode {
  const fn wire_value(self) -> i32 {
    match self {
      Self::Unset => 0,
      Self::Ok => 1,
      Self::Error => 2,
    }
  }
}

//
// SpanExportRequest
//

/// Everything needed to build one span's OTLP payload, already gathered by the calling platform.
/// Every field here is platform-neutral: trace/span ID generation, and OS-specific attribute
/// gathering (network type, carrier, foreground state, device info, etc.) all happen on the
/// platform side, exactly as they do today -- this struct is just the handoff point.
#[derive(Clone, Debug)]
pub struct SpanExportRequest {
  /// 32 lowercase hex characters (16 bytes). Not validated here -- the platform already generated
  /// this value itself (e.g. via `SecureRandom`/`SecRandomCopyBytes`) and knows it's well-formed.
  pub trace_id_hex: String,
  /// 16 lowercase hex characters (8 bytes).
  pub span_id_hex: String,
  /// The instrumentation scope name (`OTel`'s `InstrumentationScope.name`) -- typically a constant
  /// identifying the bitdrift SDK itself, passed in rather than hardcoded here so this crate makes
  /// no assumptions about either platform's naming.
  pub scope_name: String,
  pub name: String,
  pub kind: SpanKind,
  pub start_time_unix_nano: u64,
  pub end_time_unix_nano: u64,
  pub status_code: StatusCode,
  pub status_message: Option<String>,
  pub attributes: Vec<Attribute>,
  pub resource_attributes: Vec<Attribute>,
}

//
// Wire format (private; OTLP/HTTP JSON shape only, never parsed, only produced)
//

#[derive(Serialize)]
struct WirePayload {
  #[serde(rename = "resourceSpans")]
  resource_spans: [WireResourceSpans; 1],
}

#[derive(Serialize)]
struct WireResourceSpans {
  resource: WireResource,
  #[serde(rename = "scopeSpans")]
  scope_spans: [WireScopeSpans; 1],
}

#[derive(Serialize)]
struct WireResource {
  attributes: Vec<WireKeyValue>,
}

#[derive(Serialize)]
struct WireScopeSpans {
  scope: WireScope,
  spans: [WireSpan; 1],
}

#[derive(Serialize)]
struct WireScope {
  name: String,
}

#[derive(Serialize)]
struct WireSpan {
  #[serde(rename = "traceId")]
  trace_id: String,
  #[serde(rename = "spanId")]
  span_id: String,
  name: String,
  kind: i32,
  // OTLP JSON encodes int64/fixed64 fields as strings, since JSON numbers aren't guaranteed to
  // round-trip 64-bit integers precisely.
  #[serde(rename = "startTimeUnixNano")]
  start_time_unix_nano: String,
  #[serde(rename = "endTimeUnixNano")]
  end_time_unix_nano: String,
  attributes: Vec<WireKeyValue>,
  status: WireStatus,
}

#[derive(Serialize)]
struct WireStatus {
  code: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  message: Option<String>,
}

#[derive(Serialize)]
struct WireKeyValue {
  key: String,
  value: WireAnyValue,
}

#[derive(Serialize)]
#[serde(untagged)]
enum WireAnyValue {
  String {
    #[serde(rename = "stringValue")]
    string_value: String,
  },
  Int {
    // Same int64-as-string rule as the span timestamps above.
    #[serde(rename = "intValue")]
    int_value: String,
  },
  Bool {
    #[serde(rename = "boolValue")]
    bool_value: bool,
  },
}

impl From<&Attribute> for WireKeyValue {
  fn from(attribute: &Attribute) -> Self {
    let value = match &attribute.value {
      AttributeValue::String(value) => WireAnyValue::String {
        string_value: value.clone(),
      },
      AttributeValue::Int(value) => WireAnyValue::Int {
        int_value: value.to_string(),
      },
      AttributeValue::Bool(value) => WireAnyValue::Bool { bool_value: *value },
    };

    Self {
      key: attribute.key.clone(),
      value,
    }
  }
}

/// Builds the OTLP/HTTP JSON payload for one span, ready to be `POSTed` as-is (with a
/// `content-type: application/json` header) to an OTLP/HTTP `/v1/traces` endpoint.
///
/// This function is pure and synchronous: it performs no I/O, holds no state between calls, and
/// cannot fail -- every input is already a valid, owned value the caller assembled, so there is
/// nothing left here that can go wrong beyond a logic bug (which would be a bug in this function,
/// not a runtime error worth propagating as a `Result`).
#[must_use]
pub fn build_span_payload(request: &SpanExportRequest) -> Vec<u8> {
  let span = WireSpan {
    trace_id: request.trace_id_hex.clone(),
    span_id: request.span_id_hex.clone(),
    name: request.name.clone(),
    kind: request.kind.wire_value(),
    start_time_unix_nano: request.start_time_unix_nano.to_string(),
    end_time_unix_nano: request.end_time_unix_nano.to_string(),
    attributes: request.attributes.iter().map(WireKeyValue::from).collect(),
    status: WireStatus {
      code: request.status_code.wire_value(),
      message: request.status_message.clone(),
    },
  };

  let payload = WirePayload {
    resource_spans: [WireResourceSpans {
      resource: WireResource {
        attributes: request
          .resource_attributes
          .iter()
          .map(WireKeyValue::from)
          .collect(),
      },
      scope_spans: [WireScopeSpans {
        scope: WireScope {
          name: request.scope_name.clone(),
        },
        spans: [span],
      }],
    }],
  };

  // `WirePayload`'s fields are all owned, JSON-safe types (String/i32/bool/Vec/array) --
  // serialization to bytes cannot fail for this shape, so this is intentionally not propagated as
  // a `Result` to callers who would have nothing actionable to do with that error anyway.
  serde_json::to_vec(&payload).unwrap_or_default()
}
