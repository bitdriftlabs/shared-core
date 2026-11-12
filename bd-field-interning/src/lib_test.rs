// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::{FieldInterner, FieldSetTable, Resolution};
use bd_client_stats_store::Collector;
use bd_key_value::Store;
use bd_log_filter::ScrubRules;
use bd_log_primitives::{
  AnnotatedLogField,
  AnnotatedLogFields,
  DataValue,
  EncodableLog,
  InternedFields,
  Log,
  LogFieldKey,
  LogFields,
  same_value,
};
use bd_proto::protos::client::api::LogUploadRequest;
use bd_proto::protos::filter::scrub::ScrubConfiguration;
use bd_proto::protos::logging::payload::LogType;
use bd_runtime::runtime::{ConfigLoader, FeatureFlag as _, field_interning};
use bd_test_helpers::filter::macros::regex_match_and_substitute_field;
use bd_test_helpers::filter::scrub_rule;
use bd_test_helpers::runtime::{ValueKind, make_simple_update};
use bd_test_helpers::session::InMemoryStorage;
use bd_test_helpers::test_api_server::log_upload::hydrate_logs;
use bd_time::TestTimeProvider;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use time::macros::datetime;

struct Setup {
  directory: TempDir,
  runtime: Arc<ConfigLoader>,
  store: Arc<Store>,
  time: Arc<TestTimeProvider>,
  collector: Collector,
  persistent: AnnotatedLogFields,
  scrub_rules: Arc<ScrubRules>,
}

impl Setup {
  async fn new(enabled: bool) -> Self {
    let directory = TempDir::new().unwrap();
    let runtime = ConfigLoader::new(directory.path());
    let setup = Self {
      runtime,
      directory,
      store: Arc::new(Store::new(Box::<InMemoryStorage>::default())),
      time: Arc::new(TestTimeProvider::new(datetime!(2026-11-10 00:00 UTC))),
      collector: Collector::default(),
      persistent: AnnotatedLogFields::default(),
      scrub_rules: Arc::new(ScrubRules::default()),
    };
    setup
      .set_runtime(vec![
        (field_interning::Enabled::path(), ValueKind::Bool(enabled)),
        (field_interning::WindowLogs::path(), ValueKind::Int(8)),
      ])
      .await;
    setup
  }

  async fn set_runtime(&self, values: Vec<(&str, ValueKind)>) {
    self
      .runtime
      .update_snapshot(make_simple_update(values))
      .await
      .unwrap();
  }

  fn interner(&self) -> FieldInterner {
    let mut interner = FieldInterner::new(
      self.directory.path(),
      &self.runtime,
      self.store.clone(),
      self.time.clone(),
      &self.collector.scope("field_interning"),
    );
    interner.prepare();
    interner
  }

  fn set(&mut self, interner: &mut FieldInterner, key: &'static str, value: &str) {
    let key: LogFieldKey = key.into();
    let before = self.persistent.insert(
      key.clone(),
      AnnotatedLogField::new_custom(value.to_string()),
    );
    let changed =
      before.is_none_or(|before| !same_value(&before.value, &self.persistent[&key].value));
    interner.record_update(&key, changed, true);
  }

  fn remove(&mut self, interner: &mut FieldInterner, key: &'static str) {
    let key: LogFieldKey = key.into();
    let removed = self.persistent.remove(&key).is_some();
    interner.record_update(&key, removed, false);
  }

  /// A log carrying every persistent field, as normalization produces, plus `extra`.
  fn log(&self, extra: &[(&'static str, &str)]) -> Log {
    let mut fields: LogFields = self
      .persistent
      .iter()
      .map(|(key, field)| (key.clone(), field.value.clone()))
      .collect();
    for (key, value) in extra {
      fields.insert((*key).into(), DataValue::String((*value).to_string()));
    }
    Log {
      log_level: 0,
      log_type: LogType::NORMAL,
      message: "message".into(),
      fields,
      matching_fields: LogFields::default(),
      session_id: "session".into(),
      occurred_at: datetime!(2026-11-10 00:00 UTC),
      command_attachment: None,
      capture_session: None,
    }
  }

  fn intern(&self, interner: &mut FieldInterner, log: &Log) -> Option<InternedFields> {
    interner.intern(log, &self.persistent, &self.scrub_rules)
  }

  fn counter(&self, name: &str) -> u64 {
    // The collector hands out the interner's own counter for the same name.
    self.collector.scope("field_interning").counter(name).get()
  }
}

fn keys(fields: &LogFields) -> Vec<String> {
  let mut keys: Vec<String> = fields.keys().map(ToString::to_string).collect();
  keys.sort();
  keys
}

fn string_fields(
  fields: &[bd_proto::protos::logging::payload::log::Field],
) -> BTreeMap<String, String> {
  fields
    .iter()
    .map(|field| (field.key.clone(), field.value().string_data().to_string()))
    .collect()
}

#[tokio::test]
async fn disabled_interning_writes_logs_inline() {
  let mut setup = Setup::new(false).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "user_id", "abc");
  assert_eq!(setup.intern(&mut interner, &setup.log(&[])), None);
  assert!(!setup.directory.path().join("field_sets").exists());
}

#[tokio::test]
async fn unchanged_fields_are_left_out_and_others_stay_inline() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "user_id", "abc");
  setup.set(&mut interner, "theme", "dark");
  setup.set(&mut interner, "locale", "en");

  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, 1);
  assert_eq!(keys(&interned.field_set), ["locale", "theme", "user_id"]);
  assert_eq!(interned.cleared, [] as [LogFieldKey; 0]);

  // A filter changed `theme` and removed `locale`: the changed value stays inline, the removed key
  // is cleared, and `user_id` is still left out under the same version.
  let mut filtered = setup.log(&[("theme", "light")]);
  filtered.fields.remove("locale");
  let interned = setup.intern(&mut interner, &filtered).unwrap();
  assert_eq!(interned.version, 1);
  assert_eq!(interned.cleared, [LogFieldKey::from("locale")]);

  // A log carrying none of the set's values unchanged is not interned.
  let mut log = setup.log(&[("theme", "light"), ("user_id", "other")]);
  log.fields.remove("locale");
  assert_eq!(setup.intern(&mut interner, &log), None);
}

#[tokio::test]
async fn updates_between_logs_write_one_version() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "a", "1");
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    1
  );

  setup.set(&mut interner, "b", "1");
  setup.set(&mut interner, "b", "2");
  setup.set(&mut interner, "c", "3");
  setup.remove(&mut interner, "a");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, 2);
  assert_eq!(keys(&interned.field_set), ["b", "c"]);
}

#[tokio::test]
async fn field_sets_hold_scrubbed_values() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "email", "a@example.com");
  setup.set(&mut interner, "user_id", "abc");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(
    interned.field_set.get("email"),
    Some(&DataValue::String("a@example.com".to_string()))
  );

  let (rules, failures) = ScrubRules::new(ScrubConfiguration {
    rules: vec![scrub_rule(regex_match_and_substitute_field!(
      "email",
      "@.*",
      "@<redacted>"
    ))],
    ..Default::default()
  });
  assert_eq!(failures, 0);
  setup.scrub_rules = Arc::new(rules);

  // The log is scrubbed before interning, as the pipeline does, and still leaves the scrubbed
  // field out.
  let mut log = setup.log(&[]);
  setup.scrub_rules.process(&mut log);
  let interned = setup.intern(&mut interner, &log).unwrap();
  assert_eq!(interned.version, 2);
  assert_eq!(
    interned.field_set.get("email").and_then(DataValue::as_str),
    Some("a@<redacted>")
  );
  assert_eq!(interned.cleared, [] as [LogFieldKey; 0]);

  // No version in the table holds the unscrubbed value after the new rules.
  let set = interner.table().resolve(2).unwrap();
  assert_eq!(
    string_fields(&set.fields),
    BTreeMap::from([
      ("email".to_string(), "a@<redacted>".to_string()),
      ("user_id".to_string(), "abc".to_string()),
    ])
  );
  // The rule change is not counted as a change of `email`.
  assert!(interner.volatile.is_empty());
}

#[tokio::test]
async fn keys_that_change_too_often_stay_inline_until_they_settle() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "stable", "1");
  // A key set twice before the first log is not volatile.
  setup.set(&mut interner, "churny", "0");
  setup.set(&mut interner, "churny", "1");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(keys(&interned.field_set), ["churny", "stable"]);

  let mut volatile_after = None;
  for change in 2 .. 40 {
    setup.set(&mut interner, "churny", &change.to_string());
    let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
    if !interned.field_set.contains_key("churny") {
      volatile_after = Some(change);
      break;
    }
  }
  // With `W = 8` and `R = 4`, a key changing on every log becomes volatile after a few changes.
  let volatile_after = volatile_after.expect("churny key never became volatile");
  assert!(volatile_after < 10, "{volatile_after}");
  assert!(interner.volatile.contains("churny"));
  let version = interner.current.as_ref().unwrap().version;

  // While volatile, its changes write no versions.
  setup.set(&mut interner, "churny", "volatile");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, version);
  assert!(!interned.field_set.contains_key("churny"));

  // After `W` logs without a change it is interned again.
  for _ in 0 .. 8 {
    setup.intern(&mut interner, &setup.log(&[])).unwrap();
  }
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(
    interned.field_set.get("churny").and_then(DataValue::as_str),
    Some("volatile")
  );
}

#[tokio::test]
async fn a_restart_continues_the_table_above_the_reserved_numbers() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "a", "1");
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    1
  );
  drop(interner);

  // An unchanged set after a restart reuses the stored version.
  let mut interner = setup.interner();
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    1
  );

  // A change after the restart is written as a change, numbered above the first process's block.
  setup.set(&mut interner, "b", "2");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, 65);
  assert_eq!(keys(&interned.field_set), ["a", "b"]);

  // A new process can resolve the table's versions as soon as it is built, before any log, so
  // uploaders reading buffered logs from the earlier run find their sets.
  drop(interner);
  let interner = setup.interner();
  assert!(interner.table().resolve(65).is_some());

  // A lost table does not make numbers restart: the next process starts above the reservation.
  drop(interner);
  std::fs::remove_dir_all(setup.directory.path().join("field_sets")).unwrap();
  let mut interner = setup.interner();
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, 129);
  // Versions lost with the table are missing, not beyond the window.
  let table = interner.table();
  assert_eq!(table.resolution(65), Resolution::Missing);
  table.record_unresolved(&table.resolution(65), 2);
  assert_eq!(setup.counter("logs_beyond_window"), 0);
  assert_eq!(setup.counter("failures"), 0);

  // An unreadable table is discarded and counted; numbers still continue above the reservation.
  drop(interner);
  for entry in std::fs::read_dir(setup.directory.path().join("field_sets")).unwrap() {
    let path = entry.unwrap().path();
    let len = std::fs::metadata(&path).unwrap().len();
    std::fs::write(&path, vec![0xff; usize::try_from(len).unwrap()]).unwrap();
  }
  let mut interner = setup.interner();
  assert_eq!(setup.counter("failures"), 1);
  assert_eq!(interner.table().resolve(129), None);
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(interned.version, 193);
}

#[tokio::test]
async fn a_failed_write_stops_interning_for_the_process() {
  let mut setup = Setup::new(true).await;
  setup
    .set_runtime(vec![
      (field_interning::Enabled::path(), ValueKind::Bool(true)),
      (field_interning::TableMaxBytes::path(), ValueKind::Int(0)),
    ])
    .await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "huge", &"x".repeat(8 * 1024));

  // The table cannot hold the set: the log is written inline and interning stops, even once the
  // set would fit.
  assert_eq!(setup.intern(&mut interner, &setup.log(&[])), None);
  setup.remove(&mut interner, "huge");
  setup.set(&mut interner, "small", "1");
  assert_eq!(setup.intern(&mut interner, &setup.log(&[])), None);
  assert_eq!(setup.counter("failures"), 1);

  // The next process interns again.
  drop(interner);
  let mut interner = setup.interner();
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(keys(&interned.field_set), ["small"]);
}

#[tokio::test]
async fn turning_interning_off_deletes_the_table() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "a", "1");
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    1
  );

  setup
    .set_runtime(vec![(
      field_interning::Enabled::path(),
      ValueKind::Bool(false),
    )])
    .await;
  interner.prepare();
  // No new versions, and the table is gone: buffered interned logs upload without their sets.
  assert_eq!(setup.intern(&mut interner, &setup.log(&[])), None);
  assert_eq!(interner.table().resolve(1), None);
  assert!(!setup.directory.path().join("field_sets").exists());

  // Turning interning back on starts a new table, numbered above every earlier version.
  setup
    .set_runtime(vec![(
      field_interning::Enabled::path(),
      ValueKind::Bool(true),
    )])
    .await;
  interner.prepare();
  setup.set(&mut interner, "a", "2");
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    65
  );
}

#[tokio::test]
async fn a_table_from_an_earlier_run_is_deleted_if_interning_is_off() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "a", "1");
  setup.intern(&mut interner, &setup.log(&[])).unwrap();
  drop(interner);

  setup
    .set_runtime(vec![(
      field_interning::Enabled::path(),
      ValueKind::Bool(false),
    )])
    .await;
  let interner = setup.interner();
  assert_eq!(interner.table().resolve(1), None);
  assert!(!setup.directory.path().join("field_sets").exists());
}

#[tokio::test]
async fn hydrating_an_upload_restores_the_logs() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "user_id", "abc");
  setup.set(&mut interner, "theme", "dark");

  let mut proto_logs = Vec::new();
  let mut versions = Vec::new();
  let mut expected = Vec::new();
  let mut write = |setup: &Setup, interner: &mut FieldInterner, log: Log, compress: bool| {
    let interned = setup.intern(interner, &log);
    versions.push(interned.as_ref().map(|interned| interned.version));
    expected.push(
      log
        .fields
        .iter()
        .map(|(key, value)| (key.to_string(), value.as_str().unwrap().to_string()))
        .collect::<BTreeMap<_, _>>(),
    );
    let mut encodable = EncodableLog::new(log, u64::from(compress));
    encodable.set_interned(interned);
    let size = encodable.compute_size(&[], &[]).unwrap();
    let mut bytes = vec![0; usize::try_from(size).unwrap()];
    encodable.serialize_to_bytes(&[], &[], &mut bytes).unwrap();
    proto_logs.push(bytes);
  };

  write(
    &setup,
    &mut interner,
    setup.log(&[("screen", "home")]),
    false,
  );
  setup.set(&mut interner, "theme", "light");
  // An override stays inline; the log is compressed, to check that hydration expands it.
  write(
    &setup,
    &mut interner,
    setup.log(&[("theme", "override")]),
    true,
  );
  // A filter removed `user_id`.
  let mut removed = setup.log(&[]);
  removed.fields.remove("user_id");
  write(&setup, &mut interner, removed, false);
  assert_eq!(versions, [Some(1), Some(2), Some(2)]);

  let table: FieldSetTable = interner.table();
  let request = LogUploadRequest {
    proto_logs,
    field_sets: [1, 2]
      .into_iter()
      .map(|version| (*table.resolve(version).unwrap()).clone())
      .collect(),
    ..Default::default()
  };
  let hydrated: Vec<_> = hydrate_logs(&request)
    .iter()
    .map(|log| string_fields(&log.fields))
    .collect();
  assert_eq!(hydrated, expected);
}

#[tokio::test]
async fn removed_keys_do_not_keep_change_rates() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  for index in 0 .. 20 {
    let key: &'static str = Box::leak(format!("request_{index}").into_boxed_str());
    setup.set(&mut interner, key, "1");
    setup.intern(&mut interner, &setup.log(&[]));
    setup.remove(&mut interner, key);
  }
  setup.intern(&mut interner, &setup.log(&[]));
  assert_eq!(interner.last_changes.len(), 0);

  // A key that became volatile and was then removed is dropped once it is re-admitted.
  for change in 0 .. 10 {
    setup.set(&mut interner, "churny", &change.to_string());
    setup.intern(&mut interner, &setup.log(&[]));
  }
  assert!(interner.volatile.contains("churny"));
  setup.remove(&mut interner, "churny");
  for _ in 0 .. 10 {
    setup.intern(&mut interner, &setup.log(&[]));
  }
  assert!(!interner.volatile.contains("churny"));
  assert_eq!(interner.last_changes.len(), 0);
}

#[tokio::test]
async fn empty_keys_stay_inline() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "", "value");
  setup.set(&mut interner, "user_id", "abc");
  let interned = setup.intern(&mut interner, &setup.log(&[])).unwrap();
  assert_eq!(keys(&interned.field_set), ["user_id"]);
}

/// Storage whose reads of one key fail, as a platform store might.
#[derive(Default)]
struct FailingReads {
  inner: InMemoryStorage,
}

impl bd_key_value::Storage for FailingReads {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    self.inner.set_string(key, value)
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    if key == super::RESERVED_VERSION_END.key() {
      anyhow::bail!("read failed");
    }
    self.inner.get_string(key)
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self.inner.delete(key)
  }
}

#[tokio::test]
async fn an_unreadable_reservation_stops_interning_but_keeps_the_table() {
  let mut setup = Setup::new(true).await;
  let mut interner = setup.interner();
  setup.set(&mut interner, "user_id", "abc");
  assert_eq!(
    setup
      .intern(&mut interner, &setup.log(&[]))
      .unwrap()
      .version,
    1
  );
  drop(interner);

  // The next process cannot read the reservation, so it might reuse a number. It writes no new
  // versions but still resolves the existing ones for buffered logs.
  setup.store = Arc::new(Store::new(Box::<FailingReads>::default()));
  let mut interner = setup.interner();
  assert_eq!(setup.intern(&mut interner, &setup.log(&[])), None);
  assert!(interner.table().resolve(1).is_some());

  // Changes are not tracked either, so dynamically named keys do not accumulate.
  for index in 0 .. 10 {
    let key: &'static str = Box::leak(format!("request_{index}").into_boxed_str());
    setup.set(&mut interner, key, "1");
    setup.remove(&mut interner, key);
  }
  assert!(interner.dirty.is_empty());
}
