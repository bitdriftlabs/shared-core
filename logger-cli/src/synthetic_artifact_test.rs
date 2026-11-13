// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use tokio::io::AsyncReadExt as _;

fn input(file: &str, source: SourceKind) -> ArtifactInput {
  ArtifactInput {
    file: file.into(),
    source,
    offset_bytes: 0,
    content_type: PERFETTO_CONTENT_TYPE.into(),
  }
}

#[tokio::test]
async fn file_sources_preserve_reference_and_offset() {
  let directory = tempfile::tempdir().unwrap();
  let fixtures = directory.path().join("fixtures");
  let sdk = directory.path().join("sdk");
  tokio::fs::create_dir_all(&fixtures).await.unwrap();
  tokio::fs::create_dir_all(&sdk).await.unwrap();
  tokio::fs::write(fixtures.join("trace"), b"prefix-body")
    .await
    .unwrap();
  let source = FixtureSource::new(&fixtures, &sdk).await.unwrap();
  let (owned, size) = source
    .prepare(&input("trace", SourceKind::Path))
    .await
    .unwrap();
  assert_eq!(size, 11);
  let UploadSource::Path(path) = owned else {
    panic!("expected an owned file");
  };
  assert_ne!(path, fixtures.join("trace"));
  assert_eq!(tokio::fs::read(&path).await.unwrap(), b"prefix-body");
  tokio::fs::remove_file(path).await.unwrap();
  let mut request = input("trace", SourceKind::File);
  request.offset_bytes = 7;
  let (file, size) = source.prepare(&request).await.unwrap();
  assert_eq!(size, 4);
  let UploadSource::File(file) = file else {
    panic!("expected a file handle");
  };
  let mut bytes = Vec::new();
  tokio::fs::File::from_std(file)
    .read_to_end(&mut bytes)
    .await
    .unwrap();
  assert_eq!(bytes, b"body");
  assert_eq!(
    tokio::fs::read(fixtures.join("trace")).await.unwrap(),
    b"prefix-body"
  );
}

#[tokio::test]
async fn fixture_access_rejects_escape_links_and_unbounded_bytes() {
  let directory = tempfile::tempdir().unwrap();
  let fixtures = directory.path().join("fixtures");
  tokio::fs::create_dir(&fixtures).await.unwrap();
  tokio::fs::write(directory.path().join("outside"), b"outside")
    .await
    .unwrap();
  let source = FixtureSource::new(&fixtures, directory.path())
    .await
    .unwrap();
  for file in ["../outside", "/outside", "sub/file"] {
    assert!(
      source
        .prepare(&input(file, SourceKind::Path))
        .await
        .is_err()
    );
  }
  #[cfg(unix)]
  {
    std::os::unix::fs::symlink(directory.path().join("outside"), fixtures.join("link")).unwrap();
    assert!(
      source
        .prepare(&input("link", SourceKind::Path))
        .await
        .is_err()
    );
  }
  tokio::fs::File::create(fixtures.join("large"))
    .await
    .unwrap()
    .set_len(MAX_BYTES_INPUT + 1)
    .await
    .unwrap();
  assert!(
    source
      .prepare(&input("large", SourceKind::Bytes))
      .await
      .is_err()
  );
  assert_eq!(
    tokio::fs::read(directory.path().join("outside"))
      .await
      .unwrap(),
    b"outside"
  );
}

#[test]
fn attachment_arguments_are_typed_and_explicit() {
  let arguments = HashMap::from([
    (
      "file".into(),
      Data {
        data_type: Some(Data_type::StringData("trace".into())),
        ..Default::default()
      },
    ),
    (
      "source".into(),
      Data {
        data_type: Some(Data_type::StringData("file".into())),
        ..Default::default()
      },
    ),
    (
      "offset_bytes".into(),
      Data {
        data_type: Some(Data_type::StringData("7".into())),
        ..Default::default()
      },
    ),
  ]);
  let parsed = ArtifactInput::from_arguments(&arguments).unwrap();
  assert!(matches!(parsed.source, SourceKind::File));
  assert_eq!(parsed.offset_bytes, 7);
  let mut invalid = arguments;
  invalid.insert("unknown".into(), Data::default());
  assert!(ArtifactInput::from_arguments(&invalid).is_err());
}

#[tokio::test]
async fn profiles_require_observed_distinct_parents_with_canonical_sessions() {
  let directory = tempfile::tempdir().unwrap();
  let source = FixtureSource::new(directory.path(), directory.path())
    .await
    .unwrap();
  let profiles = SyntheticProfiles::new(Arc::new(source));
  let first = Uuid::new_v4();
  let second = Uuid::new_v4();
  let session = Uuid::new_v4().to_string();
  for id in [first, second] {
    profiles.reports.lock().insert(
      id,
      ProfileParent {
        report_id: id.to_string(),
        session_id: session.clone(),
      },
    );
  }
  assert_eq!(
    profiles
      .parent_session(&[first.to_string(), second.to_string()])
      .unwrap(),
    session
  );
  assert!(profiles.parent_session(&[]).is_err());
  assert!(
    profiles
      .parent_session(&[Uuid::new_v4().to_string()])
      .is_err()
  );
  assert!(
    profiles
      .parent_session(&[first.to_string(), first.to_string()])
      .is_err()
  );
  profiles
    .reports
    .lock()
    .get_mut(&first)
    .unwrap()
    .session_id
    .clear();
  assert!(
    profiles
      .parent_session(&[first.to_string()])
      .unwrap_err()
      .to_string()
      .contains("--current-session")
  );
}
