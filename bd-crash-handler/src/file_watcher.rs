// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ReportOrigin;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct DetectedReport {
  pub origin: ReportOrigin,
  pub path: PathBuf,
}

#[derive(Clone)]
pub struct FileWatcher {
  _watcher: Arc<RecommendedWatcher>,
}

impl FileWatcher {
  pub fn new(
    directory: &Path,
  ) -> anyhow::Result<(Self, tokio::sync::mpsc::Receiver<DetectedReport>)> {
    let (file_updated_tx, file_updated_rx) = tokio::sync::mpsc::channel(1);

    log::debug!(
      "Starting file watcher for directory: {}",
      directory.display()
    );

    let current_dir = directory.join("current_session");
    let previous_dir = directory.join("previous_session");

    let _ = std::fs::create_dir_all(&current_dir);
    let _ = std::fs::create_dir_all(&previous_dir);

    let mut watcher = RecommendedWatcher::new(
      move |result: notify::Result<Event>| {
        let event = match result {
          Ok(event) => event,
          Err(e) => {
            log::warn!("File watcher error: {e}");
            return;
          },
        };


        // kqueue uses Modify events for new files so we need to watch for both Create and Modify
        // events.
        if !matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_)) {
          log::debug!("File watcher ignoring non-creation event: {event:?}");
          return;
        }

        log::debug!("File watcher received event: {event:?}");

        for path in event.paths {
          if path.extension().and_then(OsStr::to_str) != Some("cap") {
            continue;
          }

          let Some(parent_directory) = path.parent() else {
            continue;
          };

          let origin = if parent_directory.file_name() == Some(OsStr::new("current_session")) {
            ReportOrigin::Current
          } else if parent_directory.file_name() == Some(OsStr::new("previous_session")) {
            ReportOrigin::Previous
          } else {
            log::debug!(
              "File watcher ignoring file outside watched directories: {}, does not start with {} \
               or {}",
              path.display(),
              current_dir.display(),
              previous_dir.display()
            );
            continue;
          };

          log::debug!("File watcher detected new report: {}", path.display());

          file_updated_tx
            .try_send(DetectedReport { origin, path })
            .ok();
        }
      },
      Config::default(),
    )?;

    watcher.watch(directory, RecursiveMode::Recursive)?;

    Ok((
      Self {
        _watcher: watcher.into(),
      },
      file_updated_rx,
    ))
  }
}
