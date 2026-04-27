// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Compares regex-lite (used by bd-client-regex) against full regex to quantify the perf
// trade-off for binary size savings.
//
// Run with: cargo bench -p bd-client-regex

use criterion::{Criterion, black_box, criterion_group, criterion_main};

// Patterns representative of real workflow/matcher usage.
const PATTERNS: &[(&str, &str)] = &[
  ("catch_all", ".*"),
  ("prefix", "^HTTPRequest.*"),
  ("error_code", r"error_code=\d+"),
  ("url_path", r"^/api/v\d+/[a-zA-Z]+"),
  ("log_level", r"^(DEBUG|INFO|WARNING|ERROR):\s"),
  ("json_field", r#""session_id"\s*:\s*"[a-f0-9-]+""#),
];

// Haystacks representative of real log messages and field values.
const HAYSTACKS: &[(&str, &str)] = &[
  ("short_match", "HTTPRequest GET /api/v1/users"),
  ("short_miss", "SceneDidActivate"),
  (
    "medium_log",
    "ERROR: connection timeout after 30s for host api.example.com:443 error_code=504",
  ),
  (
    "json_payload",
    r#"{"session_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "event": "app_launch"}"#,
  ),
];

fn bench_compile(c: &mut Criterion) {
  let mut group = c.benchmark_group("compile");
  group.sample_size(20);
  for &(name, pattern) in PATTERNS {
    group.bench_function(format!("lite/{name}"), |b| {
      b.iter(|| regex_lite::Regex::new(black_box(pattern)).unwrap());
    });
    group.bench_function(format!("full/{name}"), |b| {
      b.iter(|| regex::Regex::new(black_box(pattern)).unwrap());
    });
  }
  group.finish();
}

fn bench_is_match(c: &mut Criterion) {
  let mut group = c.benchmark_group("is_match");
  group.sample_size(20);
  for &(pat_name, pattern) in PATTERNS {
    let lite_re = regex_lite::Regex::new(pattern).unwrap();
    let full_re = regex::Regex::new(pattern).unwrap();

    for &(hay_name, haystack) in HAYSTACKS {
      let id = format!("{pat_name}/{hay_name}");
      group.bench_function(format!("lite/{id}"), |b| {
        b.iter(|| lite_re.is_match(black_box(haystack)));
      });
      group.bench_function(format!("full/{id}"), |b| {
        b.iter(|| full_re.is_match(black_box(haystack)));
      });
    }
  }
  group.finish();
}

fn bench_captures(c: &mut Criterion) {
  let mut group = c.benchmark_group("captures");
  group.sample_size(20);

  // Simulates save_field regex capture extraction (bd-workflows/src/config.rs).
  let capture_patterns: &[(&str, &str, &str)] = &[
    (
      "error_code",
      r"error_code=(\d+)",
      "ERROR: connection timeout error_code=504",
    ),
    (
      "session_id",
      r#""session_id"\s*:\s*"([a-f0-9-]+)""#,
      r#"{"session_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}"#,
    ),
    (
      "version",
      r"^v(\d+)\.(\d+)\.(\d+)",
      "v2.14.3-beta+build.123",
    ),
  ];

  for &(name, pattern, haystack) in capture_patterns {
    let lite_re = regex_lite::Regex::new(pattern).unwrap();
    let full_re = regex::Regex::new(pattern).unwrap();

    group.bench_function(format!("lite/{name}"), |b| {
      b.iter(|| lite_re.captures(black_box(haystack)));
    });
    group.bench_function(format!("full/{name}"), |b| {
      b.iter(|| full_re.captures(black_box(haystack)));
    });
  }
  group.finish();
}

fn bench_replace_all(c: &mut Criterion) {
  let mut group = c.benchmark_group("replace_all");
  group.sample_size(20);

  // Simulates log filter regex substitution (bd-log-filter/src/lib.rs).
  let replace_cases: &[(&str, &str, &str, &str)] = &[
    (
      "redact_email",
      r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
      "User user@example.com logged in from admin@corp.io",
      "[REDACTED]",
    ),
    (
      "mask_token",
      r"Bearer [a-zA-Z0-9._-]+",
      "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
      "Bearer [MASKED]",
    ),
  ];

  for &(name, pattern, haystack, replacement) in replace_cases {
    let lite_re = regex_lite::Regex::new(pattern).unwrap();
    let full_re = regex::Regex::new(pattern).unwrap();

    group.bench_function(format!("lite/{name}"), |b| {
      b.iter(|| lite_re.replace_all(black_box(haystack), replacement));
    });
    group.bench_function(format!("full/{name}"), |b| {
      b.iter(|| full_re.replace_all(black_box(haystack), replacement));
    });
  }
  group.finish();
}

fn bench_escape(c: &mut Criterion) {
  let mut group = c.benchmark_group("escape");
  group.sample_size(20);

  // Simulates PREFIX match compilation (bd-log-matcher/src/matcher.rs).
  let values = &[
    ("simple", "HTTPRequest"),
    ("special_chars", "foo.bar[0]+baz"),
  ];

  for &(name, value) in values {
    group.bench_function(format!("lite/{name}"), |b| {
      b.iter(|| regex_lite::escape(black_box(value)));
    });
    group.bench_function(format!("full/{name}"), |b| {
      b.iter(|| regex::escape(black_box(value)));
    });
  }
  group.finish();
}

criterion_group!(
  benches,
  bench_compile,
  bench_is_match,
  bench_captures,
  bench_replace_all,
  bench_escape,
);
criterion_main!(benches);
