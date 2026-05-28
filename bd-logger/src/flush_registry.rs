// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./flush_registry_test.rs"]
mod flush_registry_test;

use bd_api::TriggerUploadSource;
use base64::Engine as _;
use std::sync::Arc;

static PENDING_TRIGGER_UPLOADS_KEY: bd_key_value::Key<String> =
  bd_key_value::Key::new("logger.pending_trigger_uploads.1");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PersistedTriggerUploadLifecycle {
  ReadyToUpload,
  Uploading,
}

impl PersistedTriggerUploadLifecycle {
  fn encode(self) -> &'static str {
    match self {
      Self::ReadyToUpload => "ready",
      Self::Uploading => "uploading",
    }
  }

  fn decode(value: &str) -> Option<Self> {
    match value {
      "ready" => Some(Self::ReadyToUpload),
      "uploading" => Some(Self::Uploading),
      _ => None,
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PersistedTriggerUploadSource {
  WorkflowAction(String),
  ExplicitSessionCapture(String),
  RemoteCommand(String),
}

impl From<&TriggerUploadSource> for PersistedTriggerUploadSource {
  fn from(value: &TriggerUploadSource) -> Self {
    match value {
      TriggerUploadSource::WorkflowAction(id) => Self::WorkflowAction(id.clone()),
      TriggerUploadSource::ExplicitSessionCapture(id) => Self::ExplicitSessionCapture(id.clone()),
      TriggerUploadSource::RemoteCommand(id) => Self::RemoteCommand(id.clone()),
    }
  }
}

impl PersistedTriggerUploadSource {
  pub(crate) fn to_trigger_upload_source(&self) -> TriggerUploadSource {
    match self {
      Self::WorkflowAction(id) => TriggerUploadSource::WorkflowAction(id.clone()),
      Self::ExplicitSessionCapture(id) => TriggerUploadSource::ExplicitSessionCapture(id.clone()),
      Self::RemoteCommand(id) => TriggerUploadSource::RemoteCommand(id.clone()),
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersistedTriggerUpload {
  pub(crate) id: String,
  pub(crate) source: PersistedTriggerUploadSource,
  pub(crate) buffer_ids: Vec<String>,
  pub(crate) has_streaming: bool,
  pub(crate) lifecycle: PersistedTriggerUploadLifecycle,
}

impl PersistedTriggerUploadSource {
  fn encode(&self) -> (&'static str, &str) {
    match self {
      Self::WorkflowAction(id) => ("workflow", id),
      Self::ExplicitSessionCapture(id) => ("session_capture", id),
      Self::RemoteCommand(id) => ("remote", id),
    }
  }

  fn decode(kind: &str, id: String) -> Option<Self> {
    match kind {
      "workflow" => Some(Self::WorkflowAction(id)),
      "session_capture" => Some(Self::ExplicitSessionCapture(id)),
      "remote" => Some(Self::RemoteCommand(id)),
      _ => None,
    }
  }
}

impl PersistedTriggerUpload {
  fn encode(&self) -> String {
    let (source_kind, source_id) = self.source.encode();
    let mut parts = vec![
      encode_component(&self.id),
      encode_component(source_kind),
      encode_component(source_id),
      if self.has_streaming { "1" } else { "0" }.to_string(),
      self.lifecycle.encode().to_string(),
      self.buffer_ids.len().to_string(),
    ];
    parts.extend(self.buffer_ids.iter().map(|buffer_id| encode_component(buffer_id)));
    parts.join("\t")
  }

  fn decode(line: &str) -> Option<Self> {
    let parts = line.split('\t').collect::<Vec<_>>();
    if parts.len() < 6 {
      return None;
    }

    let buffer_count = parts[5].parse::<usize>().ok()?;
    if parts.len() != 6 + buffer_count {
      return None;
    }

    let id = decode_component(parts[0])?;
    let source_kind = decode_component(parts[1])?;
    let source_id = decode_component(parts[2])?;
    let has_streaming = match parts[3] {
      "0" => false,
      "1" => true,
      _ => return None,
    };
    let lifecycle = PersistedTriggerUploadLifecycle::decode(parts[4])?;
    let buffer_ids = parts[6 ..]
      .iter()
      .map(|buffer_id| decode_component(buffer_id))
      .collect::<Option<Vec<_>>>()?;

    Some(Self {
      id,
      source: PersistedTriggerUploadSource::decode(&source_kind, source_id)?,
      buffer_ids,
      has_streaming,
      lifecycle,
    })
  }
}

fn encode_component(value: &str) -> String {
  base64::engine::general_purpose::STANDARD.encode(value.as_bytes())
}

fn decode_component(value: &str) -> Option<String> {
  let bytes = base64::engine::general_purpose::STANDARD.decode(value).ok()?;
  String::from_utf8(bytes).ok()
}

fn encode_uploads(uploads: &[PersistedTriggerUpload]) -> String {
  uploads
    .iter()
    .map(PersistedTriggerUpload::encode)
    .collect::<Vec<_>>()
    .join("\n")
}

fn decode_uploads(value: &str) -> Vec<PersistedTriggerUpload> {
  value
    .lines()
    .filter(|line| !line.is_empty())
    .filter_map(PersistedTriggerUpload::decode)
    .collect()
}

#[derive(Clone)]
pub(crate) struct PendingTriggerUploadsStore {
  store: Arc<bd_key_value::Store>,
}

impl PendingTriggerUploadsStore {
  pub(crate) fn new(store: Arc<bd_key_value::Store>) -> Self {
    Self { store }
  }

  pub(crate) fn upsert(&self, upload: PersistedTriggerUpload) {
    let mut uploads = self.pending_uploads();
    uploads.retain(|existing| existing.id != upload.id);
    uploads.push(upload);
    self.store.set_string(&PENDING_TRIGGER_UPLOADS_KEY, &encode_uploads(&uploads));
  }

  pub(crate) fn remove(&self, id: &str) {
    let mut uploads = self.pending_uploads();
    uploads.retain(|existing| existing.id != id);
    self.store.set_string(&PENDING_TRIGGER_UPLOADS_KEY, &encode_uploads(&uploads));
  }

  pub(crate) fn mark_uploading(&self, id: &str) {
    let mut uploads = self.pending_uploads();
    for upload in &mut uploads {
      if upload.id == id {
        upload.lifecycle = PersistedTriggerUploadLifecycle::Uploading;
      }
    }
    self.store.set_string(&PENDING_TRIGGER_UPLOADS_KEY, &encode_uploads(&uploads));
  }

  pub(crate) fn pending_uploads(&self) -> Vec<PersistedTriggerUpload> {
    self
      .store
      .get_string(&PENDING_TRIGGER_UPLOADS_KEY)
      .map_or_else(Vec::new, |value| decode_uploads(&value))
  }
}
