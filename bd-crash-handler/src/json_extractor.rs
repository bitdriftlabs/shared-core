#[cfg(test)]
#[path = "./json_extractor_test.rs"]
mod tests;

use itertools::Itertools as _;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use tinyjson::JsonValue;

const ARRAY_ACCESS_REGEX: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"(\w+)\[(\d+)\]").unwrap());

#[derive(Debug, PartialEq, Eq)]
enum PathPart {
  Key(String),
  KeyAndIndex(String, usize),
}

impl PathPart {
  fn parse(s: impl AsRef<str>) -> Self {
    let captures = ARRAY_ACCESS_REGEX.captures(s.as_ref());
    if let Some(captures) = captures {
      let key = captures.get(1).unwrap().as_str().to_string();
      let index = captures.get(2).unwrap().as_str().parse::<usize>().unwrap();

      PathPart::KeyAndIndex(key, index)
    } else {
      PathPart::Key(s.as_ref().to_string())
    }
  }
}

/// A JSON path is a sequence of keys that can be used to traverse a JSON object. For example,
/// given the JSON object `{"a": {"b": {"c": "value"}}}` the path `a.b.c` would yield the string
/// `value`.
#[derive(Debug)]
pub struct JsonPath {
  key: PathPart,
  path: Vec<PathPart>,
}

impl JsonPath {
  pub fn parse(s: &str) -> Option<Self> {
    let parts = s.split('.').map(|part| part.to_string()).collect_vec();

    if parts.len() == 1 {
      if parts[0].is_empty() {
        return None;
      }

      return Some(Self {
        key: PathPart::parse(&parts[0]),
        path: Vec::new(),
      });
    }

    let mut parts = parts.into_iter();
    let len = parts.len();
    let path = (&mut parts).take(len - 1).collect_vec();
    let key = PathPart::parse(parts.last().unwrap().as_str());

    let path = path.into_iter().map(PathPart::parse).collect_vec();

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
      ptr = Self::get(ptr, key)?.get()?;
    }

    let value = Self::get(ptr, &path.key)?;

    if let JsonValue::String(value) = value {
      return Some(value);
    }

    None
  }

  fn get<'a>(object: &'a HashMap<String, JsonValue>, key: &PathPart) -> Option<&'a JsonValue> {
    match key {
      PathPart::Key(key) => object.get(key),
      PathPart::KeyAndIndex(key, index) => {
        let array = object.get(key)?.get::<Vec<JsonValue>>()?;
        array.get(*index)
      },
    }
  }
}
