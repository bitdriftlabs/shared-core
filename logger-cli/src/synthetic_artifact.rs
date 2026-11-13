// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./synthetic_artifact_test.rs"]
mod tests;

use bd_artifact_upload::{Client, PERFETTO_CONTENT_TYPE, ReportAttribution, UploadSource};
use bd_client_common::file_system::{OwnedFile, open_regular_file_async};
use bd_logger::{
  CommandAttachment,
  CommandError,
  CommandInvocation,
  CommandResult,
  RegisteredCommandHandler,
};
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::Report;
use bd_proto::protos::client::api::CrashProfileMetadata;
use bd_proto::protos::logging::payload::Data;
use bd_proto::protos::logging::payload::data::Data_type;
use bd_runtime::runtime::ConfigLoader;
use bd_shutdown::ComponentShutdown;
use clap::ValueEnum;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};
use time::OffsetDateTime;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const COMMAND_NAME: &str = "synthetic_attachment";
const MAX_BYTES_INPUT: u64 = 1024 * 1024;

//
// ArtifactInput
//

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
  #[default]
  Path,
  File,
  Bytes,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArtifactInput {
  pub file: String,
  pub source: SourceKind,
  pub offset_bytes: u64,
  pub content_type: String,
}

impl ArtifactInput {
  fn from_arguments(arguments: &HashMap<String, Data>) -> anyhow::Result<Self> {
    let mut values = HashMap::new();
    for (name, value) in arguments {
      if !["file", "source", "offset_bytes", "content_type"].contains(&name.as_str()) {
        anyhow::bail!("unknown synthetic attachment argument: {name}");
      }
      let Some(Data_type::StringData(value)) = &value.data_type else {
        anyhow::bail!("synthetic attachment argument {name} must be a string");
      };
      values.insert(name.as_str(), value.as_str());
    }
    let file = values
      .get("file")
      .filter(|file| !file.is_empty())
      .ok_or_else(|| anyhow::anyhow!("file is required"))?
      .to_string();
    let source = match values.get("source").copied().unwrap_or("path") {
      "path" => SourceKind::Path,
      "file" => SourceKind::File,
      "bytes" => SourceKind::Bytes,
      other => anyhow::bail!("invalid attachment source: {other}"),
    };
    Ok(Self {
      file,
      source,
      offset_bytes: values.get("offset_bytes").copied().unwrap_or("0").parse()?,
      content_type: values
        .get("content_type")
        .copied()
        .unwrap_or(PERFETTO_CONTENT_TYPE)
        .to_string(),
    })
  }
}

//
// FixtureSource
//

pub struct FixtureSource {
  root: PathBuf,
  owned: PathBuf,
}

impl FixtureSource {
  pub async fn new(root: &Path, sdk_directory: &Path) -> anyhow::Result<Self> {
    let root = tokio::fs::canonicalize(root).await?;
    if !tokio::fs::metadata(&root).await?.is_dir() {
      anyhow::bail!("attachment fixture root must be a directory");
    }
    let owned = tokio::fs::canonicalize(sdk_directory)
      .await?
      .join("synthetic-inputs");
    tokio::fs::create_dir_all(&owned).await?;
    Ok(Self { root, owned })
  }

  pub async fn prepare(&self, input: &ArtifactInput) -> anyhow::Result<(UploadSource, u64)> {
    let path = Path::new(&input.file);
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
      anyhow::bail!("fixture file must be a single filename inside the configured fixture root");
    }
    let mut file = open_regular_file_async(&self.root.join(path)).await?;
    let size = file.metadata().await?.len();
    let remaining = size
      .checked_sub(input.offset_bytes)
      .ok_or_else(|| anyhow::anyhow!("file offset exceeds fixture length"))?;
    if input.offset_bytes != 0 && !matches!(input.source, SourceKind::File) {
      anyhow::bail!("nonzero offsets require a file-handle source");
    }
    file
      .seek(std::io::SeekFrom::Start(input.offset_bytes))
      .await?;
    let source = match input.source {
      SourceKind::File => UploadSource::File(file.into_std().await),
      SourceKind::Bytes => {
        if remaining > MAX_BYTES_INPUT {
          anyhow::bail!("Bytes fixtures must fit the normal 1 MiB submission budget; use a file");
        }
        let mut bytes = Vec::new();
        file
          .take(MAX_BYTES_INPUT + 1)
          .read_to_end(&mut bytes)
          .await?;
        if bytes.len() as u64 != remaining {
          anyhow::bail!("fixture changed while reading");
        }
        UploadSource::Bytes(bytes)
      },
      SourceKind::Path => {
        let path = self.owned.join(Uuid::new_v4().to_string());
        let owned = OwnedFile::new(&path)
          .ok_or_else(|| anyhow::anyhow!("owned fixture path must be absolute"))?;
        let mut output = OpenOptions::new()
          .write(true)
          .create_new(true)
          .open(&path)
          .await?;
        let copied = tokio::io::copy(&mut file.take(size + 1), &mut output).await?;
        if copied != size {
          anyhow::bail!("fixture changed while copying");
        }
        output.flush().await?;
        output.sync_all().await?;
        owned.disarm();
        UploadSource::Path(path)
      },
    };
    Ok((source, remaining))
  }
}

//
// SyntheticAttachmentCommand
//

pub struct SyntheticAttachmentCommand(pub Arc<FixtureSource>);

#[async_trait::async_trait]
impl RegisteredCommandHandler for SyntheticAttachmentCommand {
  async fn execute(&self, invocation: CommandInvocation) -> CommandResult {
    let input = match ArtifactInput::from_arguments(&invocation.arguments) {
      Ok(input) => input,
      Err(error) => {
        return CommandResult::Failed {
          error: CommandError::InvalidArguments(error.to_string()),
          fields: [].into(),
        };
      },
    };
    match self.0.prepare(&input).await {
      Ok((source, size)) => {
        log::info!(
          "synthetic attachment ready: command={:?} session={} source={:?} logical_bytes={size}",
          invocation.command_id,
          invocation.session_id,
          input.source
        );
        CommandResult::Completed {
          fields: [].into(),
          attachment: Some(CommandAttachment {
            source,
            content_type: Some(input.content_type),
          }),
        }
      },
      Err(error) => CommandResult::Failed {
        error: CommandError::HandlerFailed(error.to_string()),
        fields: [].into(),
      },
    }
  }
}

//
// SyntheticProfiles
//

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProfileParent {
  pub report_id: String,
  pub session_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProfileSubmission {
  pub artifact_id: String,
  pub logical_bytes: u64,
  pub session_id: String,
  pub report_ids: Vec<String>,
}

pub struct SyntheticProfiles {
  source: Arc<FixtureSource>,
  client: OnceLock<Arc<dyn Client>>,
  reports: Mutex<BTreeMap<Uuid, ProfileParent>>,
}

impl SyntheticProfiles {
  #[must_use]
  pub fn new(source: Arc<FixtureSource>) -> Self {
    Self {
      source,
      client: OnceLock::new(),
      reports: Mutex::default(),
    }
  }

  pub fn reports(&self) -> Vec<ProfileParent> {
    self.reports.lock().values().cloned().collect()
  }

  pub async fn submit(
    &self,
    input: ArtifactInput,
    report_ids: Vec<String>,
  ) -> anyhow::Result<ProfileSubmission> {
    let session_id = self.parent_session(&report_ids)?;
    let client = self
      .client
      .get()
      .ok_or_else(|| anyhow::anyhow!("SDK profile uploader has not started"))?;
    let (source, logical_bytes) = self.source.prepare(&input).await?;
    let (tx, rx) = oneshot::channel();
    let id = client.enqueue_crash_profile(
      source,
      OffsetDateTime::now_utc(),
      session_id.clone(),
      input.content_type,
      CrashProfileMetadata {
        report_ids: report_ids.clone(),
        ..Default::default()
      },
      Some(tx),
    )?;
    log::info!("synthetic profile queued: artifact={id} parents={report_ids:?}");
    rx.await??;
    log::info!("synthetic profile durably staged: artifact={id} logical_bytes={logical_bytes}");
    Ok(ProfileSubmission {
      artifact_id: id.to_string(),
      logical_bytes,
      session_id,
      report_ids,
    })
  }

  fn parent_session(&self, report_ids: &[String]) -> anyhow::Result<String> {
    if report_ids.is_empty() || report_ids.len() > 16 {
      anyhow::bail!("a synthetic profile requires 1-16 observed parent reports");
    }
    let session_id = {
      let reports = self.reports.lock();
      let mut seen = HashSet::new();
      let mut session = None;
      for id in report_ids {
        let id = Uuid::parse_str(id)?;
        if !seen.insert(id) {
          anyhow::bail!("duplicate parent report ID: {id}");
        }
        let parent = reports
          .get(&id)
          .ok_or_else(|| anyhow::anyhow!("parent report was not observed by this SDK: {id}"))?;
        Uuid::parse_str(&parent.session_id).map_err(|error| {
          anyhow::anyhow!(
            "parent {id} has no valid session ({error}); use enqueue-fake-crash --upload \
             --current-session"
          )
        })?;
        session.get_or_insert_with(|| parent.session_id.clone());
      }
      session.ok_or_else(|| anyhow::anyhow!("missing parent session"))?
    };
    Ok(session_id)
  }
}

impl ReportAttribution for SyntheticProfiles {
  fn add_report(&self, report_id: Uuid, _: &Report<'_>, session_id: &str) {
    log::info!("synthetic profile parent observed: report={report_id} session={session_id}");
    self.reports.lock().insert(
      report_id,
      ProfileParent {
        report_id: report_id.to_string(),
        session_id: session_id.to_string(),
      },
    );
  }

  fn mark_ready(&self) {
    log::debug!("synthetic profile parent startup pass complete");
  }

  fn start(&self, client: Arc<dyn Client>, _: &ConfigLoader, _: ComponentShutdown) {
    if self.client.set(client).is_err() {
      log::error!("synthetic profile uploader started twice");
    }
  }
}
