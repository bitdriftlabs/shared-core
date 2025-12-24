// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

// The purpose of these data structures are to have a small map/set like structures which are
// efficient for small sizes (up to ~5 items) and use Vec as a backing store which is more
// efficient than HashMap/BTreeMap for small sizes.

//
// TinySet
//

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct TinySet<T> {
  inner: TinyMap<T, ()>,
}

impl<T: PartialEq> TinySet<T> {
  pub fn iter(&self) -> impl Iterator<Item = &T> {
    self.inner.items.iter().map(|(k, ())| k)
  }

  #[must_use]
  pub fn first(&self) -> Option<&T> {
    self.inner.items.first().map(|(k, ())| k)
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.inner.items.is_empty()
  }

  pub fn remove(&mut self, value: &T) {
    self.inner.remove(value);
  }

  pub fn insert(&mut self, value: T) {
    self.inner.insert(value, ());
  }

  pub fn extend<I>(&mut self, iter: I)
  where
    I: IntoIterator<Item = T>,
  {
    self.inner.extend(iter.into_iter().map(|item| (item, ())));
  }

  #[must_use]
  pub fn len(&self) -> usize {
    self.inner.items.len()
  }

  pub fn intersection(&self, other: &Self) -> impl Iterator<Item = &T> {
    self
      .iter()
      .filter(move |item| other.inner.get(item).is_some())
  }

  #[must_use]
  pub fn is_disjoint(&self, other: &Self) -> bool {
    self.iter().all(|item| other.inner.get(item).is_none())
  }

  pub fn contains<Q>(&self, value: &Q) -> bool
  where
    T: Borrow<Q>,
    Q: PartialEq + ?Sized,
  {
    self.inner.get(value).is_some()
  }

  pub fn retain<F>(&mut self, f: F)
  where
    F: Fn(&T) -> bool,
  {
    self.inner.items.retain(|(k, ())| f(k));
  }
}

impl<T> Default for TinySet<T> {
  fn default() -> Self {
    Self {
      inner: TinyMap::default(),
    }
  }
}

impl<T: PartialEq> FromIterator<T> for TinySet<T> {
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    Self {
      inner: iter.into_iter().map(|item| (item, ())).collect(),
    }
  }
}

impl<T: PartialEq> Extend<T> for TinySet<T> {
  fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
    self.inner.extend(iter.into_iter().map(|item| (item, ())));
  }
}

impl<T: PartialEq> Extend<Self> for TinySet<T> {
  fn extend<I: IntoIterator<Item = Self>>(&mut self, iter: I) {
    for set in iter {
      self.inner.extend(set.inner);
    }
  }
}

impl<T: PartialEq, const N: usize> From<[T; N]> for TinySet<T> {
  fn from(arr: [T; N]) -> Self {
    Self {
      inner: TinyMap::from(arr.map(|item| (item, ()))),
    }
  }
}

impl<T> IntoIterator for TinySet<T> {
  type Item = T;
  type IntoIter = std::iter::Map<std::vec::IntoIter<(T, ())>, fn((T, ())) -> T>;

  fn into_iter(self) -> Self::IntoIter {
    fn take_key<T>((k, ()): (T, ())) -> T {
      k
    }
    self.inner.into_iter().map(take_key::<T>)
  }
}

impl<'a, T> IntoIterator for &'a TinySet<T> {
  type Item = &'a T;
  type IntoIter = std::iter::Map<std::slice::Iter<'a, (T, ())>, fn(&'a (T, ())) -> &'a T>;

  fn into_iter(self) -> Self::IntoIter {
    fn map_ref<T>((k, ()): &(T, ())) -> &T {
      k
    }
    self.inner.items.iter().map(map_ref)
  }
}

//
// TinyMap
//

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct TinyMap<K, V> {
  items: Vec<(K, V)>,
}

impl<K: PartialEq, V> TinyMap<K, V> {
  pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
    self.items.iter().map(|(k, v)| (k, v))
  }

  pub fn get<Q>(&self, key: &Q) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: PartialEq + ?Sized,
  {
    self
      .items
      .iter()
      .find_map(|(k, v)| if k.borrow() == key { Some(v) } else { None })
  }

  pub fn get_mut_or_insert_default(&mut self, key: K) -> &mut V
  where
    V: Default,
  {
    if let Some(pos) = self.items.iter().position(|(k, _)| k == &key) {
      return &mut self.items[pos].1;
    }

    debug_assert!(self.items.len() <= 5, "TinyMap should be small");
    self.items.push((key, V::default()));
    #[allow(clippy::unwrap_used)]
    &mut self.items.last_mut().unwrap().1
  }

  pub fn insert(&mut self, key: K, value: V) {
    if let Some((_, v)) = self.items.iter_mut().find(|(k, _)| k == &key) {
      *v = value;
    } else {
      debug_assert!(self.items.len() <= 5, "TinyMap should be small");
      self.items.push((key, value));
    }
  }

  pub fn extend<I>(&mut self, iter: I)
  where
    I: IntoIterator<Item = (K, V)>,
  {
    for (k, v) in iter {
      self.insert(k, v);
    }
  }

  pub fn append(&mut self, other: &mut Self) {
    for (k, v) in other.items.drain(..) {
      self.insert(k, v);
    }
  }

  pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
  where
    K: Borrow<Q>,
    Q: PartialEq + ?Sized,
  {
    if let Some(pos) = self.items.iter().position(|(k, _)| k.borrow() == key) {
      Some(self.items.swap_remove(pos).1)
    } else {
      None
    }
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.items.is_empty()
  }

  pub fn into_values(self) -> impl Iterator<Item = V> {
    self.items.into_iter().map(|(_, v)| v)
  }
}

impl<K, V> IntoIterator for TinyMap<K, V> {
  type Item = (K, V);
  type IntoIter = std::vec::IntoIter<(K, V)>;

  fn into_iter(self) -> Self::IntoIter {
    self.items.into_iter()
  }
}

impl<'a, K, V> IntoIterator for &'a TinyMap<K, V> {
  type Item = (&'a K, &'a V);
  type IntoIter = std::iter::Map<std::slice::Iter<'a, (K, V)>, fn(&'a (K, V)) -> (&'a K, &'a V)>;

  fn into_iter(self) -> Self::IntoIter {
    fn map_ref<K, V>((k, v): &(K, V)) -> (&K, &V) {
      (k, v)
    }
    self.items.iter().map(map_ref)
  }
}

impl<K: PartialEq, V> FromIterator<(K, V)> for TinyMap<K, V> {
  fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
    let mut new = Self::default();
    new.extend(iter);
    new
  }
}

impl<K: PartialEq, V, const N: usize> From<[(K, V); N]> for TinyMap<K, V> {
  fn from(arr: [(K, V); N]) -> Self {
    let mut new = Self::default();
    new.extend(arr);
    new
  }
}

impl<K: PartialEq, V> Extend<(K, V)> for TinyMap<K, V> {
  fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
    for (k, v) in iter {
      self.insert(k, v);
    }
  }
}

impl<K: PartialEq, V> Extend<Self> for TinyMap<K, V> {
  fn extend<I: IntoIterator<Item = Self>>(&mut self, iter: I) {
    for map in iter {
      for (k, v) in map {
        self.insert(k, v);
      }
    }
  }
}

impl<K, V> Default for TinyMap<K, V> {
  fn default() -> Self {
    Self { items: Vec::new() }
  }
}

// ProtoFieldSerialize/ProtoFieldDeserialize implementations for TinySet<T>
// TinySet is serialized as a repeated field in protobuf.
bd_proto_util::impl_proto_repeated!(TinySet<T>, T, PartialEq);

// ProtoFieldSerialize/ProtoFieldDeserialize implementations for TinyMap
// TinyMap is serialized as a map field (repeated key-value pairs) in protobuf.
impl<K, V> bd_proto_util::serialization::ProtoType for TinyMap<K, V> {
  fn wire_type() -> protobuf::rt::WireType {
    protobuf::rt::WireType::LengthDelimited
  }
}

impl<K, V> bd_proto_util::serialization::ProtoFieldSerialize for TinyMap<K, V>
where
  K: bd_proto_util::serialization::ProtoFieldSerialize + PartialEq,
  V: bd_proto_util::serialization::ProtoFieldSerialize,
{
  fn compute_size(&self, field_number: u32) -> u64 {
    bd_proto_util::serialization::compute_map_size(self, field_number)
  }

  fn serialize(
    &self,
    field_number: u32,
    os: &mut protobuf::CodedOutputStream<'_>,
  ) -> anyhow::Result<()> {
    bd_proto_util::serialization::serialize_map(self, field_number, os)
  }
}

impl<K, V> bd_proto_util::serialization::ProtoFieldDeserialize for TinyMap<K, V>
where
  K: bd_proto_util::serialization::ProtoFieldDeserialize + PartialEq + Default,
  V: bd_proto_util::serialization::ProtoFieldDeserialize + Default,
{
  fn deserialize(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self> {
    let (key, value) = bd_proto_util::serialization::deserialize_map_entry(is)?;
    let mut map = Self::default();
    map.extend(std::iter::once((key, value)));
    Ok(map)
  }
}

impl<K, V> bd_proto_util::serialization::RepeatedFieldDeserialize for TinyMap<K, V>
where
  K: bd_proto_util::serialization::ProtoFieldDeserialize + PartialEq + Default,
  V: bd_proto_util::serialization::ProtoFieldDeserialize + Default,
{
  type Element = (K, V);

  fn deserialize_element(is: &mut protobuf::CodedInputStream<'_>) -> anyhow::Result<Self::Element> {
    bd_proto_util::serialization::deserialize_map_entry(is)
  }

  fn add_element(&mut self, (key, value): Self::Element) {
    self.extend(std::iter::once((key, value)));
  }
}
