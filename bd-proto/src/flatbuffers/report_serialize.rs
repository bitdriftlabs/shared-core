// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./report_serialize_test.rs"]
mod test;

use crate::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::*;
use crate::flatbuffers::serialize_enum;
extern crate serde;
use self::serde::ser::{Serialize, SerializeStruct, Serializer};

serialize_enum!(ReportType);
serialize_enum!(Platform);
serialize_enum!(Architecture);
serialize_enum!(FrameType);
serialize_enum!(ErrorRelation);
serialize_enum!(CrashReporterScope);
serialize_enum!(CrashReporter);
serialize_enum!(PowerState);
serialize_enum!(NetworkState);
serialize_enum!(JavaScriptEngine);
serialize_enum!(MemoryPressureLevel);
serialize_enum!(Rotation);
serialize_enum!(FrameStatus);
serialize_enum!(CrashInfoDetails);

impl Serialize for Timestamp {
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

impl Serialize for Memory {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Memory", 3)?;
    s.serialize_field("total", &self.total())?;
    s.serialize_field("free", &self.free())?;
    s.serialize_field("used", &self.used())?;
    s.end()
  }
}

impl Serialize for AppBuildNumber<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("AppBuildNumber", 2)?;
    if let Some(f) = self.cf_bundle_version() {
      s.skip_field("version_code")?;
      s.serialize_field("cf_bundle_version", &f)?;
    } else {
      s.serialize_field("version_code", &self.version_code())?;
      s.skip_field("cf_bundle_version")?;
    }
    s.end()
  }
}

impl Serialize for ProcessorUsage<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("ProcessorUsage", 2)?;
    s.serialize_field("duration_seconds", &self.duration_seconds())?;
    s.serialize_field("used_percent", &self.used_percent())?;
    s.end()
  }
}
impl Serialize for AppMetrics<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("AppMetrics", 11)?;
    if let Some(f) = self.app_id() {
      s.serialize_field("app_id", &f)?;
    } else {
      s.skip_field("app_id")?;
    }
    if let Some(f) = self.memory() {
      s.serialize_field("memory", &f)?;
    } else {
      s.skip_field("memory")?;
    }
    if let Some(f) = self.version() {
      s.serialize_field("version", &f)?;
    } else {
      s.skip_field("version")?;
    }
    if let Some(f) = self.build_number() {
      s.serialize_field("build_number", &f)?;
    } else {
      s.skip_field("build_number")?;
    }
    if let Some(f) = self.running_state() {
      s.serialize_field("running_state", &f)?;
    } else {
      s.skip_field("running_state")?;
    }
    s.serialize_field("process_id", &self.process_id())?;
    if let Some(f) = self.region_format() {
      s.serialize_field("region_format", &f)?;
    } else {
      s.skip_field("region_format")?;
    }
    if let Some(f) = self.cpu_usage() {
      s.serialize_field("cpu_usage", &f)?;
    } else {
      s.skip_field("cpu_usage")?;
    }
    if let Some(f) = self.lifecycle_event() {
      s.serialize_field("lifecycle_event", &f)?;
    } else {
      s.skip_field("lifecycle_event")?;
    }
    s.serialize_field("javascript_engine", &self.javascript_engine())?;
    s.serialize_field("memory_pressure_level", &self.memory_pressure_level())?;
    s.end()
  }
}

impl Serialize for OSBuild<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("OSBuild", 4)?;
    if let Some(f) = self.version() {
      s.serialize_field("version", &f)?;
    } else {
      s.skip_field("version")?;
    }
    if let Some(f) = self.brand() {
      s.serialize_field("brand", &f)?;
    } else {
      s.skip_field("brand")?;
    }
    if let Some(f) = self.fingerprint() {
      s.serialize_field("fingerprint", &f)?;
    } else {
      s.skip_field("fingerprint")?;
    }
    if let Some(f) = self.kern_osversion() {
      s.serialize_field("kern_osversion", &f)?;
    } else {
      s.skip_field("kern_osversion")?;
    }
    s.end()
  }
}

impl Serialize for PowerMetrics<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("PowerMetrics", 2)?;
    s.serialize_field("power_state", &self.power_state())?;
    s.serialize_field("charge_percent", &self.charge_percent())?;
    s.end()
  }
}

impl Serialize for Display<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Display", 3)?;
    s.serialize_field("height", &self.height())?;
    s.serialize_field("width", &self.width())?;
    s.serialize_field("density_dpi", &self.density_dpi())?;
    s.end()
  }
}

impl Serialize for DeviceMetrics<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("DeviceMetrics", 15)?;
    if let Some(f) = self.time() {
      s.serialize_field("time", &f)?;
    } else {
      s.skip_field("time")?;
    }
    if let Some(f) = self.timezone() {
      s.serialize_field("timezone", &f)?;
    } else {
      s.skip_field("timezone")?;
    }
    if let Some(f) = self.power_metrics() {
      s.serialize_field("power_metrics", &f)?;
    } else {
      s.skip_field("power_metrics")?;
    }
    s.serialize_field("network_state", &self.network_state())?;
    s.serialize_field("rotation", &self.rotation())?;
    s.serialize_field("arch", &self.arch())?;
    if let Some(f) = self.display() {
      s.serialize_field("display", &f)?;
    } else {
      s.skip_field("display")?;
    }
    if let Some(f) = self.manufacturer() {
      s.serialize_field("manufacturer", &f)?;
    } else {
      s.skip_field("manufacturer")?;
    }
    if let Some(f) = self.model() {
      s.serialize_field("model", &f)?;
    } else {
      s.skip_field("model")?;
    }
    if let Some(f) = self.os_build() {
      s.serialize_field("os_build", &f)?;
    } else {
      s.skip_field("os_build")?;
    }
    s.serialize_field("platform", &self.platform())?;
    if let Some(f) = self.cpu_abis() {
      s.serialize_field("cpu_abis", &f)?;
    } else {
      s.skip_field("cpu_abis")?;
    }
    s.serialize_field("low_power_mode_enabled", &self.low_power_mode_enabled())?;
    if let Some(f) = self.cpu_usage() {
      s.serialize_field("cpu_usage", &f)?;
    } else {
      s.skip_field("cpu_usage")?;
    }
    s.serialize_field("thermal_state", &self.thermal_state())?;
    s.end()
  }
}

impl Serialize for SourceFile<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("SourceFile", 3)?;
    if let Some(f) = self.path() {
      s.serialize_field("path", &f)?;
    } else {
      s.skip_field("path")?;
    }
    s.serialize_field("line", &self.line())?;
    s.serialize_field("column", &self.column())?;
    s.end()
  }
}

impl Serialize for CPURegister<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("CPURegister", 2)?;
    if let Some(f) = self.name() {
      s.serialize_field("name", &f)?;
    } else {
      s.skip_field("name")?;
    }
    s.serialize_field("value", &self.value())?;
    s.end()
  }
}

impl Serialize for Frame<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Frame", 14)?;
    s.serialize_field("type", &self.type_())?;
    if let Some(f) = self.class_name() {
      s.serialize_field("class_name", &f)?;
    } else {
      s.skip_field("class_name")?;
    }
    if let Some(f) = self.symbol_name() {
      s.serialize_field("symbol_name", &f)?;
    } else {
      s.skip_field("symbol_name")?;
    }
    if let Some(f) = self.source_file() {
      s.serialize_field("source_file", &f)?;
    } else {
      s.skip_field("source_file")?;
    }
    if let Some(f) = self.image_id() {
      s.serialize_field("image_id", &f)?;
    } else {
      s.skip_field("image_id")?;
    }
    s.serialize_field("frame_address", &self.frame_address())?;
    s.serialize_field("symbol_address", &self.symbol_address())?;
    if let Some(f) = self.registers() {
      s.serialize_field("registers", &f)?;
    } else {
      s.skip_field("registers")?;
    }
    if let Some(f) = self.state() {
      s.serialize_field("state", &f)?;
    } else {
      s.skip_field("state")?;
    }
    s.serialize_field("frame_status", &self.frame_status())?;
    s.serialize_field("original_index", &self.original_index())?;
    s.serialize_field("in_app", &self.in_app())?;
    if let Some(f) = self.symbolicated_name() {
      s.serialize_field("symbolicated_name", &f)?;
    } else {
      s.skip_field("symbolicated_name")?;
    }
    if let Some(f) = self.js_bundle_path() {
      s.serialize_field("js_bundle_path", &f)?;
    } else {
      s.skip_field("js_bundle_path")?;
    }
    s.end()
  }
}

impl Serialize for Thread<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Thread", 8)?;
    if let Some(f) = self.name() {
      s.serialize_field("name", &f)?;
    } else {
      s.skip_field("name")?;
    }
    s.serialize_field("active", &self.active())?;
    s.serialize_field("index", &self.index())?;
    if let Some(f) = self.state() {
      s.serialize_field("state", &f)?;
    } else {
      s.skip_field("state")?;
    }
    s.serialize_field("priority", &self.priority())?;
    s.serialize_field("quality_of_service", &self.quality_of_service())?;
    if let Some(f) = self.stack_trace() {
      s.serialize_field("stack_trace", &f)?;
    } else {
      s.skip_field("stack_trace")?;
    }
    if let Some(f) = self.summary() {
      s.serialize_field("summary", &f)?;
    } else {
      s.skip_field("summary")?;
    }
    s.end()
  }
}

impl Serialize for ThreadDetails<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("ThreadDetails", 2)?;
    s.serialize_field("count", &self.count())?;
    if let Some(f) = self.threads() {
      s.serialize_field("threads", &f)?;
    } else {
      s.skip_field("threads")?;
    }
    s.end()
  }
}

impl Serialize for Error<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Error", 4)?;
    if let Some(f) = self.name() {
      s.serialize_field("name", &f)?;
    } else {
      s.skip_field("name")?;
    }
    if let Some(f) = self.reason() {
      s.serialize_field("reason", &f)?;
    } else {
      s.skip_field("reason")?;
    }
    if let Some(f) = self.stack_trace() {
      s.serialize_field("stack_trace", &f)?;
    } else {
      s.skip_field("stack_trace")?;
    }
    s.serialize_field("relation_to_next", &self.relation_to_next())?;
    s.end()
  }
}

impl Serialize for NSException<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("NSException", 3)?;
    if let Some(f) = self.name() {
      s.serialize_field("name", &f)?;
    } else {
      s.skip_field("name")?;
    }
    if let Some(f) = self.reason() {
      s.serialize_field("reason", &f)?;
    } else {
      s.skip_field("reason")?;
    }
    if let Some(f) = self.user_info() {
      s.serialize_field("user_info", &f)?;
    } else {
      s.skip_field("user_info")?;
    }
    s.end()
  }
}

impl Serialize for MachException<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("MachException", 3)?;
    s.serialize_field("type", &self.type_())?;
    s.serialize_field("code", &self.code())?;
    s.serialize_field("subcode", &self.subcode())?;
    s.end()
  }
}

impl Serialize for PosixSignal<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("PosixSignal", 5)?;
    s.serialize_field("number", &self.number())?;
    s.serialize_field("code", &self.code())?;
    s.serialize_field("errno", &self.errno())?;
    s.serialize_field("has_fault_address", &self.has_fault_address())?;
    s.serialize_field("fault_address", &self.fault_address())?;
    s.end()
  }
}

impl Serialize for AppleTermination<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("AppleTermination", 7)?;
    if let Some(f) = self.domain() {
      s.serialize_field("domain", &f)?;
    } else {
      s.skip_field("domain")?;
    }
    if let Some(f) = self.code() {
      s.serialize_field("code", &f)?;
    } else {
      s.skip_field("code")?;
    }
    if let Some(f) = self.explanation() {
      s.serialize_field("explanation", &f)?;
    } else {
      s.skip_field("explanation")?;
    }
    if let Some(f) = self.process_visibility() {
      s.serialize_field("process_visibility", &f)?;
    } else {
      s.skip_field("process_visibility")?;
    }
    if let Some(f) = self.process_state() {
      s.serialize_field("process_state", &f)?;
    } else {
      s.skip_field("process_state")?;
    }
    if let Some(f) = self.watchdog_event() {
      s.serialize_field("watchdog_event", &f)?;
    } else {
      s.skip_field("watchdog_event")?;
    }
    if let Some(f) = self.watchdog_visibility() {
      s.serialize_field("watchdog_visibility", &f)?;
    } else {
      s.skip_field("watchdog_visibility")?;
    }
    s.end()
  }
}

impl Serialize for AppleCrashDetails<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("AppleCrashDetails", 4)?;
    if let Some(f) = self.nsexception() {
      s.serialize_field("nsexception", &f)?;
    } else {
      s.skip_field("nsexception")?;
    }
    if let Some(f) = self.mach_exception() {
      s.serialize_field("mach_exception", &f)?;
    } else {
      s.skip_field("mach_exception")?;
    }
    if let Some(f) = self.posix_signal() {
      s.serialize_field("posix_signal", &f)?;
    } else {
      s.skip_field("posix_signal")?;
    }
    if let Some(f) = self.termination() {
      s.serialize_field("termination", &f)?;
    } else {
      s.skip_field("termination")?;
    }
    s.end()
  }
}

impl Serialize for CrashInfo<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("CrashInfo", 6)?;
    s.serialize_field("reporter_scope", &self.reporter_scope())?;
    s.serialize_field("reporter", &self.reporter())?;
    if let Some(f) = self.occurred_at() {
      s.serialize_field("occurred_at", &f)?;
    } else {
      s.skip_field("occurred_at")?;
    }
    s.serialize_field("details_type", &self.details_type())?;
    match self.details_type() {
      CrashInfoDetails::AppleCrashDetails => {
        if let Some(f) = self.details_as_apple_crash_details() {
          s.serialize_field("details", &f)?;
        } else {
          s.skip_field("details")?;
        }
      },
      _ => s.skip_field("details")?,
    }
    if let Some(f) = self.thread_details() {
      s.serialize_field("thread_details", &f)?;
    } else {
      s.skip_field("thread_details")?;
    }
    s.end()
  }
}

impl Serialize for BinaryImage<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("BinaryImage", 3)?;
    if let Some(f) = self.id() {
      s.serialize_field("id", &f)?;
    } else {
      s.skip_field("id")?;
    }
    if let Some(f) = self.path() {
      s.serialize_field("path", &f)?;
    } else {
      s.skip_field("path")?;
    }
    s.serialize_field("load_address", &self.load_address())?;
    s.end()
  }
}

impl Serialize for SDKInfo<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("SDKInfo", 2)?;
    if let Some(f) = self.id() {
      s.serialize_field("id", &f)?;
    } else {
      s.skip_field("id")?;
    }
    if let Some(f) = self.version() {
      s.serialize_field("version", &f)?;
    } else {
      s.skip_field("version")?;
    }
    s.end()
  }
}

impl Serialize for FeatureFlag<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("FeatureFlag", 3)?;
    if let Some(f) = self.name() {
      s.serialize_field("name", &f)?;
    } else {
      s.skip_field("name")?;
    }
    if let Some(f) = self.value() {
      s.serialize_field("value", &f)?;
    } else {
      s.skip_field("value")?;
    }
    if let Some(f) = self.timestamp() {
      s.serialize_field("timestamp", &f)?;
    } else {
      s.skip_field("timestamp")?;
    }
    s.end()
  }
}

impl Serialize for ProcessingResult<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("ProcessingResult", 1)?;
    s.serialize_field("grouping_key", &self.grouping_key())?;
    s.end()
  }
}

impl Serialize for Report<'_> {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut s = serializer.serialize_struct("Report", 11)?;
    if let Some(f) = self.sdk() {
      s.serialize_field("sdk", &f)?;
    } else {
      s.skip_field("sdk")?;
    }
    s.serialize_field("type", &self.type_())?;
    if let Some(f) = self.app_metrics() {
      s.serialize_field("app_metrics", &f)?;
    } else {
      s.skip_field("app_metrics")?;
    }
    if let Some(f) = self.device_metrics() {
      s.serialize_field("device_metrics", &f)?;
    } else {
      s.skip_field("device_metrics")?;
    }
    if let Some(f) = self.errors() {
      s.serialize_field("errors", &f)?;
    } else {
      s.skip_field("errors")?;
    }
    if let Some(f) = self.thread_details() {
      s.serialize_field("thread_details", &f)?;
    } else {
      s.skip_field("thread_details")?;
    }
    if let Some(f) = self.binary_images() {
      s.serialize_field("binary_images", &f)?;
    } else {
      s.skip_field("binary_images")?;
    }
    if let Some(f) = self.fields() {
      s.serialize_field("fields", &f)?;
    } else {
      s.skip_field("fields")?;
    }
    if let Some(f) = self.feature_flags() {
      s.serialize_field("feature_flags", &f)?;
    } else {
      s.skip_field("feature_flags")?;
    }
    if let Some(f) = self.processing_result() {
      s.serialize_field("processing_result", &f)?;
    } else {
      s.skip_field("processing_result")?;
    }
    if let Some(f) = self.crash_info() {
      s.serialize_field("crash_info", &f)?;
    } else {
      s.skip_field("crash_info")?;
    }
    s.end()
  }
}
