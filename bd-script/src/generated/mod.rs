// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::ScriptValue;
use crate::input::{PathError, Scriptable};
use bd_proto::flatbuffers::common::bitdrift_public::fbs::common::v_1::{
  BinaryData,
  Data,
  Field,
  StringData,
  Timestamp as CommonTimestamp,
};
#[allow(clippy::wildcard_imports)]
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::{
  Timestamp as ReportTimestamp,
  *,
};
use std::collections::BTreeMap;
use vrl::core::Value;
use vrl::path::{OwnedSegment, OwnedValuePath};
use vrl::prelude::Collection;
use vrl::value::{KeyString, Kind};


impl From<Data> for ScriptValue {
  fn from(value: Data) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for Data {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<ReportType> for ScriptValue {
  fn from(value: ReportType) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for ReportType {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<Platform> for ScriptValue {
  fn from(value: Platform) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for Platform {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<Architecture> for ScriptValue {
  fn from(value: Architecture) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for Architecture {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<FrameType> for ScriptValue {
  fn from(value: FrameType) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for FrameType {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<ErrorRelation> for ScriptValue {
  fn from(value: ErrorRelation) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for ErrorRelation {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<PowerState> for ScriptValue {
  fn from(value: PowerState) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for PowerState {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<NetworkState> for ScriptValue {
  fn from(value: NetworkState) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for NetworkState {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<JavaScriptEngine> for ScriptValue {
  fn from(value: JavaScriptEngine) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for JavaScriptEngine {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<Rotation> for ScriptValue {
  fn from(value: Rotation) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for Rotation {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<FrameStatus> for ScriptValue {
  fn from(value: FrameStatus) -> Self {
    value.variant_name().map_or(Value::Null.into(), Into::into)
  }
}

impl Scriptable for FrameStatus {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      Ok(Some((*self).into()))
    } else {
      Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ))
    }
  }

  fn schema() -> Kind {
    Kind::bytes()
  }
}

impl From<StringData<'_>> for ScriptValue {
  fn from(value: StringData<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![("data", value.data().into())];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for StringData<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "data" => self.data().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(Collection::empty().with_known("data", Kind::bytes()))
  }
}

impl Scriptable for &StringData<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    StringData::schema()
  }
}


impl From<BinaryData<'_>> for ScriptValue {
  fn from(value: BinaryData<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("data_type", value.data_type().into()),
      ("data", value.data().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for BinaryData<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "data_type" => self
        .data_type()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "data" => self.data().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("data_type", Kind::bytes())
        .with_known(
          "data",
          Kind::array(Collection::empty().with_unknown(Kind::integer())),
        ),
    )
  }
}

impl Scriptable for &BinaryData<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    BinaryData::schema()
  }
}


impl From<Field<'_>> for ScriptValue {
  fn from(value: Field<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("key", value.key().into()),
      ("value_type", value.value_type().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Field<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "key" => self.key().resolve(&path[1 ..]),
      "value_type" => self.value_type().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("key", Kind::bytes())
        .with_known("value_type", Kind::bytes()),
    )
  }
}

impl Scriptable for &Field<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Field::schema()
  }
}


impl From<CommonTimestamp<'_>> for ScriptValue {
  fn from(value: CommonTimestamp<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("seconds", value.seconds().into()),
      ("nanos", value.nanos().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl From<&CommonTimestamp<'_>> for ScriptValue {
  fn from(value: &CommonTimestamp<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("seconds", value.seconds().into()),
      ("nanos", value.nanos().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for CommonTimestamp<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "seconds" => self.seconds().resolve(&path[1 ..]),
      "nanos" => self.nanos().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("seconds", Kind::integer())
        .with_known("nanos", Kind::integer()),
    )
  }
}

impl Scriptable for &CommonTimestamp<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    CommonTimestamp::schema()
  }
}


impl From<ReportTimestamp> for ScriptValue {
  fn from(value: ReportTimestamp) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("seconds", value.seconds().into()),
      ("nanos", value.nanos().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl From<&ReportTimestamp> for ScriptValue {
  fn from(value: &ReportTimestamp) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("seconds", value.seconds().into()),
      ("nanos", value.nanos().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for ReportTimestamp {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "seconds" => self.seconds().resolve(&path[1 ..]),
      "nanos" => self.nanos().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("seconds", Kind::integer())
        .with_known("nanos", Kind::integer()),
    )
  }
}

impl Scriptable for &ReportTimestamp {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    ReportTimestamp::schema()
  }
}


impl From<Memory> for ScriptValue {
  fn from(value: Memory) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("total", value.total().into()),
      ("free", value.free().into()),
      ("used", value.used().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl From<&Memory> for ScriptValue {
  fn from(value: &Memory) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("total", value.total().into()),
      ("free", value.free().into()),
      ("used", value.used().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Memory {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "total" => self.total().resolve(&path[1 ..]),
      "free" => self.free().resolve(&path[1 ..]),
      "used" => self.used().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("total", Kind::integer())
        .with_known("free", Kind::integer())
        .with_known("used", Kind::integer()),
    )
  }
}

impl Scriptable for &Memory {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Memory::schema()
  }
}


impl From<AppBuildNumber<'_>> for ScriptValue {
  fn from(value: AppBuildNumber<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("version_code", value.version_code().into()),
      ("cf_bundle_version", value.cf_bundle_version().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for AppBuildNumber<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "version_code" => self.version_code().resolve(&path[1 ..]),
      "cf_bundle_version" => self
        .cf_bundle_version()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("version_code", Kind::integer())
        .with_known("cf_bundle_version", Kind::bytes()),
    )
  }
}

impl Scriptable for &AppBuildNumber<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    AppBuildNumber::schema()
  }
}


impl From<ProcessorUsage<'_>> for ScriptValue {
  fn from(value: ProcessorUsage<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("duration_seconds", value.duration_seconds().into()),
      ("used_percent", value.used_percent().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for ProcessorUsage<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "duration_seconds" => self.duration_seconds().resolve(&path[1 ..]),
      "used_percent" => self.used_percent().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("duration_seconds", Kind::integer())
        .with_known("used_percent", Kind::integer()),
    )
  }
}

impl Scriptable for &ProcessorUsage<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    ProcessorUsage::schema()
  }
}


impl From<AppMetrics<'_>> for ScriptValue {
  fn from(value: AppMetrics<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("app_id", value.app_id().into()),
      ("memory", value.memory().into()),
      ("version", value.version().into()),
      ("build_number", value.build_number().into()),
      ("running_state", value.running_state().into()),
      ("process_id", value.process_id().into()),
      ("region_format", value.region_format().into()),
      ("cpu_usage", value.cpu_usage().into()),
      ("lifecycle_event", value.lifecycle_event().into()),
      ("javascript_engine", value.javascript_engine().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for AppMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "app_id" => self
        .app_id()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "memory" => self.memory().resolve(&path[1 ..]),
      "version" => self
        .version()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "build_number" => self.build_number().resolve(&path[1 ..]),
      "running_state" => self
        .running_state()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "process_id" => self.process_id().resolve(&path[1 ..]),
      "region_format" => self
        .region_format()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "cpu_usage" => self.cpu_usage().resolve(&path[1 ..]),
      "lifecycle_event" => self
        .lifecycle_event()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "javascript_engine" => self.javascript_engine().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("app_id", Kind::bytes())
        .with_known("memory", Memory::schema())
        .with_known("version", Kind::bytes())
        .with_known("build_number", AppBuildNumber::schema())
        .with_known("running_state", Kind::bytes())
        .with_known("process_id", Kind::integer())
        .with_known("region_format", Kind::bytes())
        .with_known("cpu_usage", ProcessorUsage::schema())
        .with_known("lifecycle_event", Kind::bytes())
        .with_known("javascript_engine", Kind::bytes()),
    )
  }
}

impl Scriptable for &AppMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    AppMetrics::schema()
  }
}


impl From<OSBuild<'_>> for ScriptValue {
  fn from(value: OSBuild<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("version", value.version().into()),
      ("brand", value.brand().into()),
      ("fingerprint", value.fingerprint().into()),
      ("kern_osversion", value.kern_osversion().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for OSBuild<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "version" => self
        .version()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "brand" => self
        .brand()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "fingerprint" => self
        .fingerprint()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "kern_osversion" => self
        .kern_osversion()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("version", Kind::bytes())
        .with_known("brand", Kind::bytes())
        .with_known("fingerprint", Kind::bytes())
        .with_known("kern_osversion", Kind::bytes()),
    )
  }
}

impl Scriptable for &OSBuild<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    OSBuild::schema()
  }
}


impl From<PowerMetrics<'_>> for ScriptValue {
  fn from(value: PowerMetrics<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("power_state", value.power_state().into()),
      ("charge_percent", value.charge_percent().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for PowerMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "power_state" => self.power_state().resolve(&path[1 ..]),
      "charge_percent" => self.charge_percent().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("power_state", Kind::bytes())
        .with_known("charge_percent", Kind::integer()),
    )
  }
}

impl Scriptable for &PowerMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    PowerMetrics::schema()
  }
}


impl From<Display<'_>> for ScriptValue {
  fn from(value: Display<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("height", value.height().into()),
      ("width", value.width().into()),
      ("density_dpi", value.density_dpi().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Display<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "height" => self.height().resolve(&path[1 ..]),
      "width" => self.width().resolve(&path[1 ..]),
      "density_dpi" => self.density_dpi().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("height", Kind::integer())
        .with_known("width", Kind::integer())
        .with_known("density_dpi", Kind::integer()),
    )
  }
}

impl Scriptable for &Display<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Display::schema()
  }
}


impl From<DeviceMetrics<'_>> for ScriptValue {
  fn from(value: DeviceMetrics<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("time", value.time().into()),
      ("timezone", value.timezone().into()),
      ("power_metrics", value.power_metrics().into()),
      ("network_state", value.network_state().into()),
      ("rotation", value.rotation().into()),
      ("arch", value.arch().into()),
      ("display", value.display().into()),
      ("manufacturer", value.manufacturer().into()),
      ("model", value.model().into()),
      ("os_build", value.os_build().into()),
      ("platform", value.platform().into()),
      ("cpu_abis", value.cpu_abis().into()),
      (
        "low_power_mode_enabled",
        value.low_power_mode_enabled().into(),
      ),
      ("cpu_usage", value.cpu_usage().into()),
      ("thermal_state", value.thermal_state().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for DeviceMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "time" => self.time().resolve(&path[1 ..]),
      "timezone" => self
        .timezone()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "power_metrics" => self.power_metrics().resolve(&path[1 ..]),
      "network_state" => self.network_state().resolve(&path[1 ..]),
      "rotation" => self.rotation().resolve(&path[1 ..]),
      "arch" => self.arch().resolve(&path[1 ..]),
      "display" => self.display().resolve(&path[1 ..]),
      "manufacturer" => self
        .manufacturer()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "model" => self
        .model()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "os_build" => self.os_build().resolve(&path[1 ..]),
      "platform" => self.platform().resolve(&path[1 ..]),
      "cpu_abis" => {
        let Some(values) = self.cpu_abis() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "low_power_mode_enabled" => self.low_power_mode_enabled().resolve(&path[1 ..]),
      "cpu_usage" => self.cpu_usage().resolve(&path[1 ..]),
      "thermal_state" => self.thermal_state().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("time", ReportTimestamp::schema())
        .with_known("timezone", Kind::bytes())
        .with_known("power_metrics", PowerMetrics::schema())
        .with_known("network_state", Kind::bytes())
        .with_known("rotation", Kind::bytes())
        .with_known("arch", Kind::bytes())
        .with_known("display", Display::schema())
        .with_known("manufacturer", Kind::bytes())
        .with_known("model", Kind::bytes())
        .with_known("os_build", OSBuild::schema())
        .with_known("platform", Kind::bytes())
        .with_known(
          "cpu_abis",
          Kind::array(Collection::empty().with_unknown(Kind::bytes())),
        )
        .with_known("low_power_mode_enabled", Kind::boolean())
        .with_known("cpu_usage", ProcessorUsage::schema())
        .with_known("thermal_state", Kind::integer()),
    )
  }
}

impl Scriptable for &DeviceMetrics<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    DeviceMetrics::schema()
  }
}


impl From<SourceFile<'_>> for ScriptValue {
  fn from(value: SourceFile<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("path", value.path().into()),
      ("line", value.line().into()),
      ("column", value.column().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for SourceFile<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "path" => self
        .path()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "line" => self.line().resolve(&path[1 ..]),
      "column" => self.column().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("path", Kind::bytes())
        .with_known("line", Kind::integer())
        .with_known("column", Kind::integer()),
    )
  }
}

impl Scriptable for &SourceFile<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    SourceFile::schema()
  }
}


impl From<CPURegister<'_>> for ScriptValue {
  fn from(value: CPURegister<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("name", value.name().into()),
      ("value", value.value().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for CPURegister<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "name" => self
        .name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "value" => self.value().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known("value", Kind::integer()),
    )
  }
}

impl Scriptable for &CPURegister<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    CPURegister::schema()
  }
}


impl From<Frame<'_>> for ScriptValue {
  fn from(value: Frame<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("type", value.type_().into()),
      ("class_name", value.class_name().into()),
      ("symbol_name", value.symbol_name().into()),
      ("source_file", value.source_file().into()),
      ("image_id", value.image_id().into()),
      ("frame_address", value.frame_address().into()),
      ("symbol_address", value.symbol_address().into()),
      ("registers", value.registers().into()),
      ("state", value.state().into()),
      ("frame_status", value.frame_status().into()),
      ("original_index", value.original_index().into()),
      ("in_app", value.in_app().into()),
      ("symbolicated_name", value.symbolicated_name().into()),
      ("js_bundle_path", value.js_bundle_path().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Frame<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "type" => self.type_().resolve(&path[1 ..]),
      "class_name" => self
        .class_name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "symbol_name" => self
        .symbol_name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "source_file" => self.source_file().resolve(&path[1 ..]),
      "image_id" => self
        .image_id()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "frame_address" => self.frame_address().resolve(&path[1 ..]),
      "symbol_address" => self.symbol_address().resolve(&path[1 ..]),
      "registers" => {
        let Some(values) = self.registers() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "state" => {
        let Some(values) = self.state() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "frame_status" => self.frame_status().resolve(&path[1 ..]),
      "original_index" => self.original_index().resolve(&path[1 ..]),
      "in_app" => self.in_app().resolve(&path[1 ..]),
      "symbolicated_name" => self
        .symbolicated_name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "js_bundle_path" => self
        .js_bundle_path()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("type", Kind::bytes())
        .with_known("class_name", Kind::bytes())
        .with_known("symbol_name", Kind::bytes())
        .with_known("source_file", SourceFile::schema())
        .with_known("image_id", Kind::bytes())
        .with_known("frame_address", Kind::integer())
        .with_known("symbol_address", Kind::integer())
        .with_known(
          "registers",
          Kind::array(Collection::empty().with_unknown(CPURegister::schema())),
        )
        .with_known(
          "state",
          Kind::array(Collection::empty().with_unknown(Kind::bytes())),
        )
        .with_known("frame_status", Kind::bytes())
        .with_known("original_index", Kind::integer())
        .with_known("in_app", Kind::boolean())
        .with_known("symbolicated_name", Kind::bytes())
        .with_known("js_bundle_path", Kind::bytes()),
    )
  }
}

impl Scriptable for &Frame<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Frame::schema()
  }
}


impl From<Thread<'_>> for ScriptValue {
  fn from(value: Thread<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("name", value.name().into()),
      ("active", value.active().into()),
      ("index", value.index().into()),
      ("state", value.state().into()),
      ("priority", value.priority().into()),
      ("quality_of_service", value.quality_of_service().into()),
      ("stack_trace", value.stack_trace().into()),
      ("summary", value.summary().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Thread<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "name" => self
        .name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "active" => self.active().resolve(&path[1 ..]),
      "index" => self.index().resolve(&path[1 ..]),
      "state" => self
        .state()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "priority" => self.priority().resolve(&path[1 ..]),
      "quality_of_service" => self.quality_of_service().resolve(&path[1 ..]),
      "stack_trace" => {
        let Some(values) = self.stack_trace() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "summary" => self
        .summary()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known("active", Kind::boolean())
        .with_known("index", Kind::integer())
        .with_known("state", Kind::bytes())
        .with_known("priority", Kind::float())
        .with_known("quality_of_service", Kind::integer())
        .with_known(
          "stack_trace",
          Kind::array(Collection::empty().with_unknown(Frame::schema())),
        )
        .with_known("summary", Kind::bytes()),
    )
  }
}

impl Scriptable for &Thread<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Thread::schema()
  }
}


impl From<ThreadDetails<'_>> for ScriptValue {
  fn from(value: ThreadDetails<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("count", value.count().into()),
      ("threads", value.threads().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for ThreadDetails<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "count" => self.count().resolve(&path[1 ..]),
      "threads" => {
        let Some(values) = self.threads() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("count", Kind::integer())
        .with_known(
          "threads",
          Kind::array(Collection::empty().with_unknown(Thread::schema())),
        ),
    )
  }
}

impl Scriptable for &ThreadDetails<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    ThreadDetails::schema()
  }
}


impl From<Error<'_>> for ScriptValue {
  fn from(value: Error<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("name", value.name().into()),
      ("reason", value.reason().into()),
      ("stack_trace", value.stack_trace().into()),
      ("relation_to_next", value.relation_to_next().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Error<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "name" => self
        .name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "reason" => self
        .reason()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "stack_trace" => {
        let Some(values) = self.stack_trace() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "relation_to_next" => self.relation_to_next().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known("reason", Kind::bytes())
        .with_known(
          "stack_trace",
          Kind::array(Collection::empty().with_unknown(Frame::schema())),
        )
        .with_known("relation_to_next", Kind::bytes()),
    )
  }
}

impl Scriptable for &Error<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Error::schema()
  }
}


impl From<BinaryImage<'_>> for ScriptValue {
  fn from(value: BinaryImage<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("id", value.id().into()),
      ("path", value.path().into()),
      ("load_address", value.load_address().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for BinaryImage<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "id" => self
        .id()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "path" => self
        .path()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "load_address" => self.load_address().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("id", Kind::bytes())
        .with_known("path", Kind::bytes())
        .with_known("load_address", Kind::integer()),
    )
  }
}

impl Scriptable for &BinaryImage<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    BinaryImage::schema()
  }
}


impl From<SDKInfo<'_>> for ScriptValue {
  fn from(value: SDKInfo<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("id", value.id().into()),
      ("version", value.version().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for SDKInfo<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "id" => self
        .id()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "version" => self
        .version()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("id", Kind::bytes())
        .with_known("version", Kind::bytes()),
    )
  }
}

impl Scriptable for &SDKInfo<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    SDKInfo::schema()
  }
}


impl From<FeatureFlag<'_>> for ScriptValue {
  fn from(value: FeatureFlag<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("name", value.name().into()),
      ("value", value.value().into()),
      ("timestamp", value.timestamp().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for FeatureFlag<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "name" => self
        .name()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "value" => self
        .value()
        .map_or(Ok(None), |value| value.resolve(&path[1 ..])),
      "timestamp" => self.timestamp().resolve(&path[1 ..]),
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("name", Kind::bytes())
        .with_known("value", Kind::bytes())
        .with_known("timestamp", CommonTimestamp::schema()),
    )
  }
}

impl Scriptable for &FeatureFlag<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    FeatureFlag::schema()
  }
}


impl From<Report<'_>> for ScriptValue {
  fn from(value: Report<'_>) -> Self {
    let script_values: Vec<(&str, Self)> = vec![
      ("sdk", value.sdk().into()),
      ("type", value.type_().into()),
      ("app_metrics", value.app_metrics().into()),
      ("device_metrics", value.device_metrics().into()),
      ("errors", value.errors().into()),
      ("thread_details", value.thread_details().into()),
      ("binary_images", value.binary_images().into()),
      ("state", value.state().into()),
      ("feature_flags", value.feature_flags().into()),
    ];
    Value::Object(
      script_values
        .iter()
        .map(|(key, value)| (key.to_string().into(), value.0.clone()))
        .collect::<BTreeMap<KeyString, Value>>(),
    )
    .into()
  }
}

impl Scriptable for Report<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    if path.is_empty() {
      return Ok(Some((*self).into()));
    }
    let Some(OwnedSegment::Field(base)) = path.first() else {
      return Err(PathError::NotAnArray(
        OwnedValuePath::from(path.to_vec()).to_string(),
      ));
    };

    match base.as_str() {
      "sdk" => self.sdk().resolve(&path[1 ..]),
      "type" => self.type_().resolve(&path[1 ..]),
      "app_metrics" => self.app_metrics().resolve(&path[1 ..]),
      "device_metrics" => self.device_metrics().resolve(&path[1 ..]),
      "errors" => {
        let Some(values) = self.errors() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "thread_details" => self.thread_details().resolve(&path[1 ..]),
      "binary_images" => {
        let Some(values) = self.binary_images() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "state" => {
        let Some(values) = self.state() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      "feature_flags" => {
        let Some(values) = self.feature_flags() else {
          return Ok(None);
        };
        values.resolve(&path[1 ..])
      },
      _ => Err(PathError::UnknownKey(
        OwnedValuePath::from(path.to_vec()).into(),
      )),
    }
  }

  fn schema() -> Kind {
    Kind::object(
      Collection::empty()
        .with_known("sdk", SDKInfo::schema())
        .with_known("type", Kind::bytes())
        .with_known("app_metrics", AppMetrics::schema())
        .with_known("device_metrics", DeviceMetrics::schema())
        .with_known(
          "errors",
          Kind::array(Collection::empty().with_unknown(Error::schema())),
        )
        .with_known("thread_details", ThreadDetails::schema())
        .with_known(
          "binary_images",
          Kind::array(Collection::empty().with_unknown(BinaryImage::schema())),
        )
        .with_known(
          "state",
          Kind::array(Collection::empty().with_unknown(Field::schema())),
        )
        .with_known(
          "feature_flags",
          Kind::array(Collection::empty().with_unknown(FeatureFlag::schema())),
        ),
    )
  }
}

impl Scriptable for &Report<'_> {
  fn resolve(&self, path: &[OwnedSegment]) -> Result<Option<ScriptValue>, PathError> {
    (*self).resolve(path)
  }

  fn schema() -> Kind {
    Report::schema()
  }
}
