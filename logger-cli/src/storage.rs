// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_key_value::Storage;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection, SqliteConnection};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;

pub struct SQLiteStorage {
  path: PathBuf,
}

impl SQLiteStorage {
  #[must_use]
  pub fn new(path: &Path) -> Self {
    Self {
      path: path.to_path_buf(),
    }
  }

  async fn open(path: PathBuf) -> anyhow::Result<SqliteConnection> {
    let options = SqliteConnectOptions::from_str(&path.to_string_lossy())?
      .create_if_missing(true)
      .disable_statement_logging();
    let mut connection = SqliteConnection::connect_with(&options).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS kvstore (key TEXT UNIQUE, value TEXT);")
      .execute(&mut connection)
      .await?;
    Ok(connection)
  }

  fn with_connection<T, F, FutureType>(&self, operation: F) -> anyhow::Result<T>
  where
    T: Send,
    F: FnOnce(SqliteConnection) -> FutureType + Send,
    FutureType: Future<Output = anyhow::Result<T>> + Send,
  {
    let path = self.path.clone();

    // Storage is synchronous but SQLx requires a Tokio runtime. Use an isolated thread so calls
    // remain safe when the logger itself is already running on a current-thread runtime.
    thread::scope(|scope| {
      scope
        .spawn(move || {
          tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move {
              let connection = Self::open(path).await?;
              operation(connection).await
            })
        })
        .join()
        .map_err(|_| anyhow::anyhow!("SQLite worker thread panicked"))?
    })
  }
}

impl Storage for SQLiteStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    let key = key.to_owned();
    let value = value.to_owned();
    self.with_connection(move |mut connection| async move {
      sqlx::query(
        "INSERT INTO kvstore VALUES (?1, ?2) ON CONFLICT DO UPDATE SET value=excluded.value",
      )
      .bind(key)
      .bind(value)
      .execute(&mut connection)
      .await?;
      Ok(())
    })
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    let key = key.to_owned();
    self.with_connection(move |mut connection| async move {
      Ok(
        sqlx::query_scalar("SELECT value FROM kvstore WHERE key = ?1")
          .bind(key)
          .fetch_optional(&mut connection)
          .await?,
      )
    })
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    let key = key.to_owned();
    self.with_connection(move |mut connection| async move {
      sqlx::query("DELETE FROM kvstore WHERE key = ?1")
        .bind(key)
        .execute(&mut connection)
        .await?;
      Ok(())
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn crud_test() {
    let file = NamedTempFile::new().unwrap();
    let storage = SQLiteStorage::new(file.path());
    assert!(storage.set_string("key", "value").is_ok());
    assert_eq!(
      Some("value".to_string()),
      storage.get_string("key").unwrap()
    );
    assert!(storage.delete("key").is_ok());
    assert_eq!(None, storage.get_string("key").unwrap());
  }

  #[test]
  fn override_test() {
    let file = NamedTempFile::new().unwrap();
    let storage = SQLiteStorage::new(file.path());
    assert!(storage.set_string("k", "value").is_ok());
    assert!(storage.set_string("k", "valooooo").is_ok());
    assert_eq!(
      Some("valooooo".to_string()),
      storage.get_string("k").unwrap()
    );
  }
}
