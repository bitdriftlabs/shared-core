// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./proto_test.rs"]
mod tests;

use base64ct::Encoding;
use itertools::Itertools;
use protobuf::MessageFull;
use protobuf::reflect::ReflectValueRef;
use sha2::Digest;
use std::hash::Hash;
use std::ops::Deref;

//
// ProtoHashWrapper
//

// This is a wrapper that makes a proto message hashable. There are quite a few caveats about this:
// 1) This will not work correctly if the protos contain floats, and especially if the float can
//    contain NaN. The messages will never be equal in that case.
// The wrapper should only be used in cases in which false positives are acceptable or enough is
// known about the proto and surrounding environment to guarantee that it will work.
#[derive(Debug, Clone)]
pub struct ProtoHashWrapper<MessageType: MessageFull> {
  pub message: MessageType,
}

impl<MessageType: MessageFull> ProtoHashWrapper<MessageType> {
  pub const fn new(message: MessageType) -> Self {
    Self { message }
  }

  pub fn message_hash(&self) -> String {
    let mut hasher = Sha256Hasher(sha2::Sha256::new());
    hash_message(&mut hasher, &self.message);

    base64ct::Base64::encode_string(&hasher.0.finalize())
  }
}

impl<MessageType: MessageFull> PartialEq for ProtoHashWrapper<MessageType> {
  fn eq(&self, other: &Self) -> bool {
    // TODO(mattklein123): Debug assert there are no floats in the message?
    self.message == other.message
  }
}

impl<MessageType: MessageFull> Eq for ProtoHashWrapper<MessageType> {}

impl<MessageType: MessageFull> Hash for ProtoHashWrapper<MessageType> {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    hash_message(state, &self.message);
  }
}

impl<MessageType: MessageFull> Deref for ProtoHashWrapper<MessageType> {
  type Target = MessageType;

  fn deref(&self) -> &Self::Target {
    &self.message
  }
}

// Sha256 doesnt implment Hasher, so use this + the wrapper below to implement a unified interface
// so we can both compute a sha256 as well as a std hash.
pub trait ProtoHasher {
  fn hash(&mut self, data: &[u8]);
}

pub struct Sha256Hasher(pub sha2::Sha256);

impl ProtoHasher for Sha256Hasher {
  fn hash(&mut self, data: &[u8]) {
    self.0.update(data);
  }
}

impl<H: std::hash::Hasher> ProtoHasher for H {
  fn hash(&mut self, data: &[u8]) {
    self.write(data);
  }
}

pub fn hash_message(hasher: &mut impl ProtoHasher, m: &dyn protobuf::MessageDyn) {
  let message_descriptor = m.descriptor_dyn();

  for field in message_descriptor.fields() {
    match field.get_reflect(m) {
      protobuf::reflect::ReflectFieldRef::Optional(optional) => {
        let Some(value) = optional.value() else {
          continue;
        };

        // If the object only contains an empty message we end up writing nothing, so encode the
        // field id if it's set.
        hasher.hash(&field.number().to_le_bytes());

        hash_value(hasher, &value);
      },
      protobuf::reflect::ReflectFieldRef::Repeated(repeated) => {
        // If the list has a list of empty messages we end up writing, so encode the number of
        // elements.
        if !repeated.is_empty() {
          hasher.hash(&repeated.len().to_le_bytes());
        }

        for value in repeated {
          hash_value(hasher, &value);
        }
      },
      protobuf::reflect::ReflectFieldRef::Map(map) => {
        // Compare the map values using a consistent ordering. Since there can be many
        // different key types we just call to_string() on it to get a consistent type to
        // compare. The actual ordering doesn't matter.
        map
          .into_iter()
          .sorted_by_key(|(k, _v)| k.to_string())
          .for_each(|(k, v)| {
            hash_value(hasher, &k);
            hash_value(hasher, &v);
          });
      },
    }
  }
}

fn hash_value(hasher: &mut impl ProtoHasher, value: &ReflectValueRef<'_>) {
  match value {
    ReflectValueRef::U32(v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::U64(v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::I64(v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::F32(v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::F64(v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::Bool(v) => hasher.hash(if *v { &[1] } else { &[0] }),
    ReflectValueRef::String(v) => hasher.hash(&v.bytes().collect::<Vec<_>>()),
    ReflectValueRef::Bytes(v) => hasher.hash(v),
    ReflectValueRef::I32(v) | ReflectValueRef::Enum(_, v) => hasher.hash(&v.to_le_bytes()),
    ReflectValueRef::Message(m) => hash_message(hasher, &**m),
  }
}
