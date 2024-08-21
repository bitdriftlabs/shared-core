// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./opaque_test.rs"]
mod opaque_test;

use crate::{ConfigurationUpdate, FromResponse, IntoRequest};
use bd_client_common::debugger::{debug, DEBUGGER};
use bd_client_common::error::{Error, Result};
use bd_proto::protos::client::api::configuration_update_ack::Nack;
use bd_proto::protos::client::api::{
  ConfigurationUpdateAck,
  HandshakeRequest,
  OpaqueConfigurationUpdate,
  OpaqueConfigurationUpdateAck,
};
use protobuf::well_known_types::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

type UnpackFn = dyn Fn(&Any) -> Result<Box<dyn protobuf::MessageDyn>> + Send + Sync;

//
// ConfigUpdate
//

#[derive(Debug)]
pub struct TypedConfigUpdate<T> {
  config: T,
  tx: tokio::sync::oneshot::Sender<Result<(), String>>,
}

impl<T> TypedConfigUpdate<T> {
  pub fn new(config: T) -> (Self, tokio::sync::oneshot::Receiver<Option<String>>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (Self { config, tx }, rx)
  }

  pub fn into_parts(self) -> (T, tokio::sync::oneshot::Sender<Option<String>>) {
    (self.config, self.tx)
  }
}

type RawConfigUpdate = TypedConfigUpdate<Box<dyn protobuf::MessageDyn>>;

//
// RegisteredConfiguration
//

struct RegisteredConfiguration {
  active_version: String,
  tx: tokio::sync::mpsc::Sender<RawConfigUpdate>,
  unpacker: Box<UnpackFn>,
}

impl RegisteredConfiguration {
  fn unpack(&self, wrapper: &Any) -> Result<Box<dyn protobuf::MessageDyn>> {
    (*self.unpacker)(wrapper)
  }
}

//
// RegistryHandle
//

/// A handle that can be used to retrieve the active configuration and to wait for changes.
pub struct RegistryHandle<T: protobuf::MessageDyn> {
  rx: tokio::sync::mpsc::Receiver<RawConfigUpdate>,
  _type: PhantomData<T>,
}

impl<T: protobuf::MessageDyn + protobuf::MessageFull> RegistryHandle<T> {
  fn new(rx: tokio::sync::mpsc::Receiver<RawConfigUpdate>) -> Self {
    Self {
      rx,
      _type: PhantomData,
    }
  }

  pub async fn next(&mut self) -> Option<TypedConfigUpdate<T>> {
    let (config, tx) = self.rx.recv().await?.into_parts();

    // At this point we have validated that the type matches, so we can just unwrap.
    Some(TypedConfigUpdate {
      config: *config.downcast_box().unwrap(),
      tx,
    })
  }
}


//
// OpaqueConfigurationRegistry
//

/// The configuration registry used to keep track of the types of configuration that we are
/// interested in getting from the server.
#[derive(Default)]
pub struct OpaqueConfigurationRegistry {
  configuration_handles: HashMap<String, RegisteredConfiguration>,
}

impl OpaqueConfigurationRegistry {
  /// Returns a handle for configuration updates matching the specified configuration type. This
  /// should be called exactly once per configuration type the caller cares about.
  pub fn register<T: protobuf::MessageFull>(&mut self) -> RegistryHandle<T> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    let previous = self.configuration_handles.insert(
      "type.googleapis.com/".to_string() + T::descriptor().full_name(),
      RegisteredConfiguration {
        active_version: String::new(),
        tx,
        unpacker: Box::new(|any| {
          let configuration = any.unpack_dyn(&T::descriptor())?;
          let configuration = configuration.ok_or(Error::Generic("invalid type".into()))?;

          bd_pgv::proto_validate::validate(&*configuration)
            .map_err(|e| Error::Generic(e.to_string().into()))?;

          Ok(configuration)
        }),
      },
    );

    // Duplicate registrations would indicate a bug. We could clone the handle in this case if we
    // want to support it but assert for now.
    debug_assert!(previous.is_none());

    RegistryHandle::new(rx)
  }

  #[allow(clippy::unnecessary_wraps)]
  fn nack<RequestType>(
    type_url: &str,
    error: &str,
    version_nonce: &str,
    last_applied_version_nonce: &str,
  ) -> Option<RequestType>
  where
    OpaqueConfigurationUpdateAck: IntoRequest<RequestType>,
  {
    log::debug!("update for {type_url} not accepted: {error}");

    Some(
      OpaqueConfigurationUpdateAck {
        type_url: type_url.to_string(),
        ack: Some(ConfigurationUpdateAck {
          last_applied_version_nonce: last_applied_version_nonce.to_string(),
          nack: Some(Nack {
            version_nonce: version_nonce.to_string(),
            error_details: error.to_string(),
            ..Default::default()
          })
          .into(),
          ..Default::default()
        })
        .into(),
        ..Default::default()
      }
      .into_request(),
    )
  }
}

#[async_trait::async_trait]
impl<RequestType, ResponseType: Sync> ConfigurationUpdate<RequestType, ResponseType>
  for OpaqueConfigurationRegistry
where
  OpaqueConfigurationUpdate: FromResponse<ResponseType>,
  OpaqueConfigurationUpdateAck: IntoRequest<RequestType>,
{
  async fn try_apply_config(&mut self, response: &ResponseType) -> Option<RequestType> {
    let update = OpaqueConfigurationUpdate::from_response(response)?;

    let type_url = &update
      .configuration
      .as_ref()
      .map(|config| config.type_url.clone())
      .unwrap_or_default();

    // Validated by pgv above.
    // TODO(snowp): Add validation + test for this above via pgv.
    let inner = update.configuration.as_ref().unwrap();

    log::debug!("handling opaque update for {type_url}",);

    let Some(handle) = self
      .configuration_handles
      .get_mut(&update.configuration.type_url)
    else {
      return Self::nack(
        type_url,
        &format!("type {type_url} not registered"),
        &update.version_nonce,
        "",
      );
    };

    if let Err(e) = bd_pgv::proto_validate::validate(update) {
      return Self::nack(
        type_url,
        &format!("pgv failure: {e}"),
        &update.version_nonce,
        "",
      );
    }

    let last_applied_version_nonce = handle.active_version.clone();

    // This includes unpacking + pgv validation.
    let unpacked_configuration = match handle.unpack(inner) {
      Ok(config) => config,
      Err(e) => {
        return Self::nack(
          type_url,
          &e.to_string(),
          &update.version_nonce,
          &last_applied_version_nonce,
        )
      },
    };

    let (raw_update, rx) = RawConfigUpdate::new(unpacked_configuration);

    handle.tx.send(raw_update).await.unwrap();

    if let Ok(Some(error)) = rx.await {
      return Self::nack(
        type_url,
        &error,
        &update.version_nonce,
        &last_applied_version_nonce,
      );
    }

    handle.active_version = update.version_nonce.clone();

    log::debug!(
      DEBUGGER,
      "acknowledging updated configuration for {type_url}",
    );

    Some(
      OpaqueConfigurationUpdateAck {
        type_url: update.configuration.type_url.clone(),
        ack: Some(ConfigurationUpdateAck {
          last_applied_version_nonce,
          nack: None.into(),
          ..Default::default()
        })
        .into(),
        ..Default::default()
      }
      .into_request(),
    )
  }

  async fn try_load_persisted_config(&mut self) {}

  fn partial_handshake(&self) -> HandshakeRequest {
    HandshakeRequest {
      opqaue_version_nonces: self
        .configuration_handles
        .iter()
        .map(|(key, value)| (key.to_string(), value.active_version.clone()))
        .collect(),
      ..Default::default()
    }
  }

  fn on_handshake_complete(&self) {}
}
