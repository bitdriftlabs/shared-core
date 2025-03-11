#[cfg(test)]
#[path = "./json_extractor_test.rs"]
mod tests;

use itertools::Itertools as _;
use std::collections::HashMap;
use tinyjson::JsonValue;

/// A JSON path is a sequence of keys that can be used to traverse a JSON object. For example,
/// given the JSON object `{"a": {"b": {"c": "value"}}}` the path `a.b.c` would yield the string
/// `value`.
#[derive(Debug)]
pub struct JsonPath {
  key: String,
  path: Vec<String>,
}

impl JsonPath {
  pub fn parse(s: &str) -> Option<Self> {
    let parts = s.split('.').map(|part| part.to_string()).collect_vec();

    if parts.len() == 1 {
      if parts[0].is_empty() {
        return None;
      }

      return Some(Self {
        key: parts[0].clone(),
        path: Vec::new(),
      });
    }

    let mut parts = parts.into_iter();
    let len = parts.len();
    let path = (&mut parts).take(len - 1).collect_vec();
    let key = parts.last().unwrap().clone();

    Some(Self { key, path })
  }
}

/// A JSON extractor is a utility that can be used to extract values from a JSON object using a
/// JSON path. This provides a mechanism to provide a single lookup key that can be used to access
/// data deeply nested within a JSON object.
#[derive(Debug)]
pub struct JsonExtractor {
  root: HashMap<String, JsonValue>,
}

impl JsonExtractor {
  pub fn new(json: &str) -> anyhow::Result<Self> {
    let root = tinyjson::JsonParser::new(json.chars())
      .parse()?
      .get::<HashMap<String, JsonValue>>()
      .ok_or_else(|| anyhow::anyhow!("expected JSON object at the top level"))?
      .clone();

    Ok(Self { root })
  }

  pub fn extract(&self, path: &JsonPath) -> Option<&String> {
    // Given a path like `["a", "b", "c"]` we want to look for a key `c` in the object at the end
    // after traversing the path `a.b`. If we find a string value at that key we'll use it as the
    // crash reason.

    let mut ptr = &self.root;

    for key in &path.path {
      ptr = ptr.get(&(*key).to_string())?.get()?;
    }

    let value = ptr.get(&path.key)?;

    if let JsonValue::String(value) = value {
      return Some(value);
    }

    None
  }
}
