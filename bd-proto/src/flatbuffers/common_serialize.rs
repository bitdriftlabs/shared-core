use crate::common_generated::common::v_1::{BinaryData, Data, Field, StringData, Timestamp};
use crate::flatbuffers::serialize_enum;
extern crate serde;
use self::serde::ser::{Serialize, SerializeStruct, Serializer};

serialize_enum!(Data);

impl Serialize for StringData<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("StringData", 1)?;
    s.serialize_field("data", &self.data())?;
    s.end()
  }
}

impl Serialize for BinaryData<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("BinaryData", 2)?;
    if let Some(f) = self.data_type() {
      s.serialize_field("data_type", &f)?;
    } else {
      s.skip_field("data_type")?;
    }
    s.serialize_field("data", &self.data())?;
    s.end()
  }
}

impl Serialize for Field<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Field", 3)?;
    s.serialize_field("key", &self.key())?;
    s.serialize_field("value_type", &self.value_type())?;
    match self.value_type() {
      Data::NONE => (),
      Data::string_data => {
        let f = self
          .value_as_string_data()
          .expect("Invalid union table, expected `Data::string_data`.");
        s.serialize_field("value", &f)?;
      },
      Data::binary_data => {
        let f = self
          .value_as_binary_data()
          .expect("Invalid union table, expected `Data::binary_data`.");
        s.serialize_field("value", &f)?;
      },
      _ => unimplemented!(),
    }
    s.end()
  }
}

impl Serialize for Timestamp<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Timestamp", 2)?;
    s.serialize_field("seconds", &self.seconds())?;
    s.serialize_field("nanos", &self.nanos())?;
    s.end()
  }
}
