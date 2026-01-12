// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use logger_cli::service::RemoteClient;
use logger_cli::types::{LogLevel, LogType};
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
  CallToolResult,
  InitializeRequestParam,
  InitializeResult,
  ProtocolVersion,
  ServerCapabilities,
  ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::transport::stdio;
use rmcp::{
  ErrorData as McpError,
  RoleServer,
  ServerHandler,
  ServiceExt,
  schemars,
  tool,
  tool_handler,
  tool_router,
};
use serde_json::json;
use tarpc::client;
use tarpc::tokio_serde::formats::Json;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::DEBUG.into()))
    .with_writer(std::io::stderr)
    .with_ansi(false)
    .init();

  let server = Tool::new("localhost".to_string(), 5501)
    .serve(stdio())
    .await?;

  server.waiting().await?;

  Ok(())
}

#[derive(serde::Deserialize, Debug, schemars::JsonSchema)]
struct LogField {
  /// The name of the log field.
  name: String,

  /// The value of the log field.
  value: String,
}

#[derive(serde::Deserialize, Debug, schemars::JsonSchema)]
struct LogParameters {
  /// The log level for the log message.
  level: LogLevel,

  /// The log type for the log message.
  log_type: LogType,

  /// The log message to log. This typically contains the main content of the log but can be empty
  /// for certain logs that rely on fields to provide the entire context.
  #[serde(default)]
  message: String,

  /// Additional fields that provide additional context for the log message. The various log types
  /// have different expected fields that can be included here.
  #[serde(default)]
  fields: Vec<LogField>,
}

#[derive(serde::Deserialize, Debug, schemars::JsonSchema)]
struct SetFeatureFlagParameters {
  /// The name of the feature flag.
  name: String,

  /// The optional variant value for the feature flag.
  variant: Option<String>,
}

struct Tool {
  tool_router: ToolRouter<Self>,

  host: String,
  port: u16,
}

impl Tool {
  async fn with_logger<F>(&self, f: F) -> anyhow::Result<()>
  where
    F: AsyncFnOnce(RemoteClient) -> anyhow::Result<()>,
  {
    let addr = format!("{}:{}", self.host, self.port);
    let mut transport = tarpc::serde_transport::tcp::connect(addr, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);
    let logger = RemoteClient::new(client::Config::default(), transport.await?).spawn();
    f(logger).await?;
    Ok(())
  }
}

#[tool_router]
impl Tool {
  fn new(host: String, port: u16) -> Self {
    Self {
      tool_router: Self::tool_router(),
      host,
      port,
    }
  }

  #[tool(description = "Log a log")]
  async fn log(
    &self,
    Parameters(params): Parameters<LogParameters>,
  ) -> Result<CallToolResult, McpError> {
    self
      .with_logger(|logger: RemoteClient| async move {
        logger
          .log(
            tarpc::context::current(),
            params.level,
            params.log_type,
            params.message,
            params
              .fields
              .into_iter()
              .map(|f| (f.name, f.value))
              .collect(),
            false,
          )
          .await?;

        Ok(())
      })
      .await
      .map_err(|e| {
        McpError::internal_error(
          "failed to log",
          Some(json!({
            "error": format!("{e}")
          })),
        )
      })?;

    Ok(CallToolResult::success(vec![]))
  }

  #[tool(description = "Start a new session")]
  async fn new_session(&self) -> Result<CallToolResult, McpError> {
    self
      .with_logger(|logger: RemoteClient| async move {
        logger.start_new_session(tarpc::context::current()).await?;

        Ok(())
      })
      .await
      .map_err(|e| {
        McpError::internal_error(
          "failed to start new session",
          Some(json!({
            "error": format!("{e}")
          })),
        )
      })?;

    Ok(CallToolResult::success(vec![]))
  }

  #[tool(description = "Set a feature flag exposure")]
  async fn set_feature_flag(
    &self,
    Parameters(params): Parameters<SetFeatureFlagParameters>,
  ) -> Result<CallToolResult, McpError> {
    self
      .with_logger(|logger: RemoteClient| async move {
        logger
          .set_feature_flag(tarpc::context::current(), params.name, params.variant)
          .await?;

        Ok(())
      })
      .await
      .map_err(|e| {
        McpError::internal_error(
          "failed to set feature flag",
          Some(json!({
            "error": format!("{e}")
          })),
        )
      })?;

    Ok(CallToolResult::success(vec![]))
  }
}

#[tool_handler]
impl ServerHandler for Tool {
  fn get_info(&self) -> ServerInfo {
    ServerInfo {
      protocol_version: ProtocolVersion::V_2024_11_05,
      instructions: Some(
        "A tool that allows interacting with the dev logger CLI. For example, this can be used to \
         log a log."
          .into(),
      ),
      capabilities: ServerCapabilities::builder().enable_tools().build(),
      ..Default::default()
    }
  }

  async fn initialize(
    &self,
    _request: InitializeRequestParam,
    context: RequestContext<RoleServer>,
  ) -> Result<InitializeResult, McpError> {
    if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
      let initialize_headers = &http_request_part.headers;
      let initialize_uri = &http_request_part.uri;
      tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
    }
    Ok(self.get_info())
  }
}
