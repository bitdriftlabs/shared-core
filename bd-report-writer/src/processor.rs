// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Wildcard imports seem reasonable once importing 20+ components of a package
#[allow(clippy::wildcard_imports)]
use bd_proto::flatbuffers::report::bitdrift_public::fbs::issue_reporting::v_1::*;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub struct ReportProcessor<'a> {
  pub(crate) builder: FlatBufferBuilder<'a>,
  binary_images: Vec<WIPOffset<BinaryImage<'a>>>,
  pub system_thread_count: u16,
  threads: Vec<WIPOffset<Thread<'a>>>,
  errors: Vec<WIPOffset<Error<'a>>>,
  report_type: ReportType,
  sdk: WIPOffset<SDKInfo<'a>>,
  app: Option<WIPOffset<AppMetrics<'a>>>,
  device: Option<WIPOffset<DeviceMetrics<'a>>>,
}

impl ReportProcessor<'_> {
  #[must_use]
  pub fn new(report_type: ReportType, sdk_id: Option<&str>, sdk_version: Option<&str>) -> Self {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let id = sdk_id.map(|id| builder.create_string(id));
    let version = sdk_version.map(|version| builder.create_string(version));
    let sdk = SDKInfo::create(&mut builder, &SDKInfoArgs { id, version });
    Self {
      builder,
      sdk,
      report_type,
      binary_images: vec![],
      system_thread_count: 0,
      threads: vec![],
      errors: vec![],
      app: None,
      device: None,
    }
  }

  pub fn finish(&mut self) -> &[u8] {
    let binary_images = self.builder.create_vector(self.binary_images.as_slice());
    let threads = self.builder.create_vector(self.threads.as_slice());
    let thread_details = if self.system_thread_count > 0 || !self.threads.is_empty() {
      Some(ThreadDetails::create(
        &mut self.builder,
        &ThreadDetailsArgs {
          count: self.system_thread_count,
          threads: Some(threads),
        },
      ))
    } else {
      None
    };
    let errors = self.builder.create_vector(self.errors.as_slice());
    let report = Report::create(
      &mut self.builder,
      &ReportArgs {
        binary_images: Some(binary_images),
        sdk: Some(self.sdk),
        type_: self.report_type,
        app_metrics: self.app,
        device_metrics: self.device,
        errors: Some(errors),
        thread_details,
        ..Default::default()
      },
    );
    self.builder.finish(report, None);
    self.builder.finished_data()
  }

  pub fn add_binary_image(&mut self, id: Option<&str>, path: &str, load_address: u64) {
    let id = id.map(|id| self.builder.create_string(id));
    let path = Some(self.builder.create_string(path));
    let image = BinaryImage::create(
      &mut self.builder,
      &BinaryImageArgs {
        id,
        path,
        load_address,
      },
    );
    self.binary_images.push(image);
  }

  pub fn add_thread(&mut self, args: &ThreadArgs<'_>) {
    let thread = Thread::create(&mut self.builder, args);
    self.threads.push(thread);
  }

  pub fn add_error(&mut self, args: &ErrorArgs<'_>) {
    let error = Error::create(&mut self.builder, args);
    self.errors.push(error);
  }

  pub fn set_device(&mut self, args: &DeviceMetricsArgs<'_>) {
    let metrics = DeviceMetrics::create(&mut self.builder, args);
    self.device = Some(metrics);
  }

  pub fn set_app(&mut self, args: &AppMetricsArgs<'_>) {
    let metrics = AppMetrics::create(&mut self.builder, args);
    self.app = Some(metrics);
  }
}
