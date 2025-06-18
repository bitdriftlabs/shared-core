// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf::MessageFull;
use protobuf::reflect::FileDescriptor;
use std::marker::PhantomData;

//
// ServiceMethod
//

// Wraps a gRPC service method after confirming the path matches the proto file.
pub struct ServiceMethod<OutgoingType: MessageFull, IncomingType: MessageFull> {
  service: String,
  method: String,
  outgoing_type: PhantomData<OutgoingType>,
  incoming_type: PhantomData<IncomingType>,
}

impl<OutgoingType: MessageFull, IncomingType: MessageFull>
  ServiceMethod<OutgoingType, IncomingType>
{
  // Create a new service method given the service name and the method name.
  #[must_use]
  pub fn new(service_name: &str, method_name: &str) -> Self {
    let message_descriptor = OutgoingType::descriptor();
    let file_descriptor = message_descriptor.file_descriptor();

    Self::new_with_fd(service_name, method_name, file_descriptor)
  }

  // Create a new service method given the service name and the method name. Useful when we cannot
  // infer the file descriptor of the service via the request/response types.
  #[must_use]
  pub fn new_with_fd(
    service_name: &str,
    method_name: &str,
    file_descriptor: &FileDescriptor,
  ) -> Self {
    let mut service_descriptor = None;
    let mut method_descriptor = None;
    for service in file_descriptor.services() {
      if service.proto().name() != service_name {
        continue;
      }

      service_descriptor = Some(service);
      for method in service_descriptor.as_ref().unwrap().methods() {
        if method.proto().name() == method_name {
          method_descriptor = Some(method);
          break;
        }
      }

      if method_descriptor.is_some() {
        break;
      }
    }

    let service_descriptor =
      service_descriptor.unwrap_or_else(|| panic!("could not find service: {service_name}"));
    let method_descriptor =
      method_descriptor.unwrap_or_else(|| panic!("could not find method: {method_name}"));
    assert!(
      method_descriptor.input_type().full_name() == OutgoingType::descriptor().full_name(),
      "service method outgoing type mismatch: {} != {}",
      method_descriptor.input_type().full_name(),
      OutgoingType::descriptor().full_name()
    );
    assert!(
      method_descriptor.output_type().full_name() == IncomingType::descriptor().full_name(),
      "service method incoming type mismatch: {} != {}",
      method_descriptor.output_type().full_name(),
      IncomingType::descriptor().full_name()
    );

    Self {
      service: format!(
        "{}.{}",
        file_descriptor.package(),
        service_descriptor.proto().name(),
      ),
      method: method_descriptor.proto().name().to_string(),
      outgoing_type: PhantomData,
      incoming_type: PhantomData,
    }
  }

  #[must_use]
  pub fn service_name(&self) -> &str {
    &self.service
  }

  #[must_use]
  pub fn method_name(&self) -> &str {
    &self.method
  }

  #[must_use]
  pub fn full_path(&self) -> String {
    format!("/{}/{}", self.service, self.method)
  }
}
