// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::handle_unexpected;
use crate::reporter::{
  ERROR_HANDLER,
  MetadataErrorReporter,
  Reporter,
  SessionProvider,
  UnexpectedErrorHandler,
};
use anyhow::anyhow;
use bd_client_stats_store::Collector;
use bd_metadata::Metadata;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Default)]
struct EmptyMetadata(HashMap<String, String>);

impl Metadata for EmptyMetadata {
  fn sdk_version(&self) -> &'static str {
    "1.2.3"
  }

  fn platform(&self) -> &bd_metadata::Platform {
    &bd_metadata::Platform::Apple
  }

  fn os(&self) -> String {
    "ios".to_string()
  }

  fn collect_inner(&self) -> HashMap<String, String> {
    self.0.clone()
  }

  fn device_id(&self) -> String {
    "device_id".to_string()
  }
}

#[derive(Default)]
struct TestReporter {
  message: Mutex<Option<String>>,
  fields: Mutex<Option<HashMap<String, String>>>,
}

impl super::Reporter for TestReporter {
  fn report(
    &self,
    message: &str,
    _detail: &Option<String>,
    fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    *self.message.lock().unwrap() = Some(message.to_string());
    *self.fields.lock().unwrap() = Some(
      fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect(),
    );
  }
}

struct MockSessionProvider {
  session_id: Arc<str>,
}

impl MockSessionProvider {
  fn new() -> Self {
    Self {
      session_id: Uuid::new_v4().to_string().into(),
    }
  }
}

impl SessionProvider for MockSessionProvider {
  fn session_id(&self) -> anyhow::Result<Arc<str>> {
    Ok(self.session_id.clone())
  }
}

#[test]
fn attach_error_handler() {
  let reporter = Arc::new(TestReporter::default());
  let session_strategy: Arc<dyn SessionProvider + Send + Sync> =
    Arc::new(MockSessionProvider::new());

  let error_reporter = Arc::new(MetadataErrorReporter::new(
    reporter.clone(),
    session_strategy.clone(),
    Arc::new(EmptyMetadata::default()),
  ));

  UnexpectedErrorHandler::set_reporter(error_reporter);

  UnexpectedErrorHandler::register_stats(&Collector::default().scope(""));

  handle_unexpected::<(), anyhow::Error>(Err(anyhow!("blah")), "test");

  assert_eq!(
    reporter.message.lock().unwrap().as_ref().unwrap(),
    "test: blah"
  );

  let mut fields = reporter.fields.lock().unwrap().as_ref().unwrap().clone();
  assert!(fields.remove("x-config-version").is_some());

  assert_eq!(
    Some(session_strategy.session_id().unwrap().as_ref()),
    fields.get("x-session-id").map(String::as_str)
  );
  fields.remove("x-session-id");
  assert_eq!(
    HashMap::from([
      ("x-sdk-version".to_string(), "1.2.3".to_string()),
      ("x-platform".to_string(), "apple".to_string()),
      ("x-os".to_string(), "ios".to_string()),
      ("x-device-id".to_string(), "device_id".to_string()),
    ]),
    fields
  );

  for i in 0 .. 5 {
    handle_unexpected::<(), anyhow::Error>(Err(anyhow!("{i}")), "test");
  }

  // Verify that we didn't see the last error message (i=4) due to the error limit.
  assert_eq!(
    reporter.message.lock().unwrap().as_ref().unwrap(),
    "test: 3"
  );

  assert_eq!(
    ERROR_HANDLER.lock().dropped_errors.as_ref().unwrap().get(),
    1
  );
}

#[test]
fn sdk_error_headers_preserve_raw_version_without_environment_attribution() {
  let reporter = Arc::new(TestReporter::default());
  let error_reporter = MetadataErrorReporter::new(
    reporter.clone(),
    Arc::new(MockSessionProvider::new()),
    Arc::new(EmptyMetadata(HashMap::from([(
      "app_version".to_string(),
      "01.2".to_string(),
    )]))),
  );
  error_reporter.report(
    "error",
    &None,
    &HashMap::from([("x-app-version".into(), "caller-version".into())]),
  );
  let headers = reporter.fields.lock().unwrap();
  let headers = headers.as_ref().unwrap();
  assert_eq!(
    headers.get("x-app-version").map(String::as_str),
    Some("01.2")
  );
  assert!(!headers.contains_key("x-app-environment"));
}
