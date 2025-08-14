// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_key_value::Storage;
use sqlite::{Connection, State};
use std::cell::RefCell;
use std::path::{Path, PathBuf};

thread_local! {
    static CONNECTION: RefCell<Option<Connection>> = const { RefCell::new(None) };
}

pub struct SQLiteStorage {
  path: PathBuf,
}

impl SQLiteStorage {
  pub fn new(path: &Path) -> Self {
    Self {
      path: path.to_path_buf(),
    }
  }

  fn open(&self) -> Result<sqlite::Connection, sqlite::Error> {
    sqlite::open(self.path.clone()).and_then(|conn| {
      let query = "CREATE TABLE IF NOT EXISTS kvstore (key TEXT UNIQUE, value TEXT);";
      conn.execute(query).map(|()| conn)
    })
  }

  fn with_connection<F, T>(&self, f: F) -> Result<T, sqlite::Error>
  where
    F: FnOnce(&Connection) -> T,
  {
    let conn = match CONNECTION.take() {
      Some(conn) => conn,
      None => self.open()?,
    };
    let result = f(&conn);
    CONNECTION.set(Some(conn));
    Ok(result)
  }
}

impl Storage for SQLiteStorage {
  fn set_string(&self, key: &str, value: &str) -> anyhow::Result<()> {
    let query =
      "INSERT INTO kvstore VALUES (:key, :value) ON CONFLICT DO UPDATE SET value=excluded.value";
    self
      .with_connection(|conn| {
        conn.prepare(query).and_then(|mut stmt| {
          stmt.bind(&[(":key", key), (":value", value)][..])?;
          stmt.next()?;
          Ok(())
        })
      })
      .flatten()
      .map_err(|e| anyhow::anyhow!("failed to insert: {e}"))
  }

  fn get_string(&self, key: &str) -> anyhow::Result<Option<String>> {
    self
      .with_connection(|conn| {
        conn
          .prepare("SELECT value FROM kvstore WHERE key = :key")
          .and_then(|mut stmt| {
            stmt.bind(&[(":key", key)][..])?;
            if State::Row == stmt.next()? {
              Ok(Some(stmt.read::<String, _>("value")?))
            } else {
              Ok(None)
            }
          })
      })
      .flatten()
      .map_err(|e| anyhow::anyhow!("failed to select: {e}"))
  }

  fn delete(&self, key: &str) -> anyhow::Result<()> {
    self
      .with_connection(|conn| {
        conn
          .prepare("DELETE FROM kvstore where key = :key")
          .and_then(|mut stmt| {
            stmt.bind(&[(":key", key)][..])?;
            stmt.next()?;
            Ok(())
          })
      })
      .flatten()
      .map_err(|e| anyhow::anyhow!("failed to delete: {e}"))
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
