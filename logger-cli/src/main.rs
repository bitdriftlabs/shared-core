// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::cli::{Command, EnableFlag, FieldPairs, Options};
use bd_client_common::file::read_compressed_protobuf;
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_session::fixed::Callbacks;
use clap::Parser;
use logger_cli::logger::{MaybeStaticSessionGenerator, SESSION_FILE};
use logger_cli::service::RemoteClient;
use std::env;
use std::path::Path;
use tarpc::tokio_serde::formats::Json;
use tarpc::{client, context};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

mod cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // initialize console logging
  init_tracing();
  let args = crate::cli::Options::parse();

  let home = env::var("HOME")?;
  let sdk_directory = Path::new(&home).join(".local").join("bd-logger-cli");
  std::fs::create_dir_all(&sdk_directory)?;

  match args.command {
    Command::EnqueueArtifacts(cmd) => {
      let report_dir = &sdk_directory.join("reports/new");
      std::fs::create_dir_all(report_dir)?;
      for path in &cmd.path {
        let source_path = Path::new(&path);
        std::fs::copy(
          source_path,
          report_dir.join(source_path.file_name().unwrap()),
        )?;
      }
    },
    Command::Start(cmd) => {
      let port = args.port;
      eprintln!("starting server on :{port}");
      logger_cli::service::start(&sdk_directory, &cmd.into(), port).await?;
    },
    Command::Log(ref cmd) => {
      with_logger(&args, async |logger| {
        logger
          .log(
            context::current(),
            cmd.log_level.clone(),
            cmd.log_type.clone(),
            cmd.message.clone(),
            FieldPairs(cmd.field.clone()).into(),
            true,
          )
          .await?;
        Ok(())
      })
      .await?;
    },
    Command::NewSession => {
      let config_path = sdk_directory.join(SESSION_FILE);
      let _ = std::fs::remove_file(&config_path);
      let generator = MaybeStaticSessionGenerator { config_path };
      let session_id = generator.generate_session_id()?;
      with_logger(&args, async |logger| {
        logger.start_new_session(context::current()).await?;
        let session_url = get_session_url(logger, session_id).await?;
        eprintln!("new session: {session_url}");
        Ok(())
      })
      .await?;
    },
    Command::Timeline => {
      let config_path = sdk_directory.join(SESSION_FILE);
      let generator = MaybeStaticSessionGenerator { config_path };
      if let Ok(session_id) = generator.cached_session_id() {
        with_logger(&args, async |logger| {
          let session_url = get_session_url(logger, session_id).await?;
          std::process::Command::new("open")
            .arg(session_url)
            .output()?;
          Ok(())
        })
        .await?;
      } else {
        eprintln!("No session ID set");
      }
    },
    Command::Trap => {
      with_logger(&args, async |logger| {
        logger.breakpoint(context::current()).await?;
        Ok(())
      })
      .await?;
    },
    Command::UploadArtifacts => {
      with_logger(&args, async |logger| {
        logger.process_crash_reports(context::current()).await?;
        Ok(())
      })
      .await?;
    },
    Command::SetSleepMode(ref cmd) => {
      with_logger(&args, async |logger| {
        logger
          .set_sleep_mode(context::current(), cmd.enabled == EnableFlag::On)
          .await?;
        Ok(())
      })
      .await?;
    },
    Command::PrintRuntimeValue(ref cmd) => {
      with_logger(&args, async |logger| {
        let name = cmd.name.clone();
        eprintln!(
          "{name}: {}",
          logger
            .get_runtime_value(context::current(), name.clone(), cmd.type_)
            .await?,
        );
        Ok(())
      })
      .await?;
    },
    Command::SetFeatureFlag(ref cmd) => {
      with_logger(&args, async |logger| {
        logger
          .set_feature_flag(context::current(), cmd.name.clone(), cmd.variant.clone())
          .await?;
        Ok(())
      })
      .await?;
    },
    Command::GetFeatureFlags => {
      with_logger(&args, async |logger| {
        let flags = logger.get_feature_flags(context::current()).await?;
        if flags.is_empty() {
          eprintln!("No feature flags set");
        } else {
          // Print table header
          eprintln!("{:<30} VARIANT", "NAME");
          eprintln!("{}", "-".repeat(50));
          for (name, variant) in flags {
            let variant_display = if variant.is_empty() {
              "<none>".to_string()
            } else {
              variant
            };
            eprintln!("{name:<30} {variant_display}");
          }
        }
        Ok(())
      })
      .await?;
    },
    Command::Stop => {
      let addr = format!("{}:{}", args.host, args.port);
      let mut transport = tarpc::serde_transport::tcp::connect(addr, Json::default);
      transport.config_mut().max_frame_length(usize::MAX);
      let client = RemoteClient::new(client::Config::default(), transport.await?).spawn();

      client.stop(context::current()).await?;
    },
    Command::DumpConfig => {
      let config_file = sdk_directory.join("config").join("protobuf.pb");
      if config_file.exists() {
        let compressed_bytes = std::fs::read(&config_file)?;
        let config: ConfigurationUpdate = read_compressed_protobuf(&compressed_bytes)?;
        let json = protobuf_json_mapping::print_to_string(&config)?;
        println!("{json}");
      } else {
        eprintln!("No configuration cached (config file not found)");
      }
    },
  }

  Ok(())
}

async fn with_logger<F>(args: &Options, f: F) -> anyhow::Result<()>
where
  F: AsyncFnOnce(RemoteClient) -> anyhow::Result<()>,
{
  let addr = format!("{}:{}", args.host, args.port);
  let mut transport = tarpc::serde_transport::tcp::connect(addr, Json::default);
  transport.config_mut().max_frame_length(usize::MAX);
  let logger = RemoteClient::new(client::Config::default(), transport.await?).spawn();
  f(logger).await?;
  Ok(())
}

async fn get_session_url(logger: RemoteClient, session_id: String) -> anyhow::Result<String> {
  let api_url = logger.get_api_url(context::current()).await?;
  let base_url = api_url.replace("api.", "timeline.");
  Ok(format!("{base_url}/session/{session_id}"))
}

fn init_tracing() {
  let stderr = tracing_subscriber::fmt::layer()
    .with_writer(std::io::stderr)
    .with_ansi(true)
    .with_line_number(true)
    .with_thread_ids(true)
    .compact();

  let filter = EnvFilter::new(
    std::env::var("RUST_LOG")
      .as_deref()
      .unwrap_or("info,tarpc=error"),
  );

  Registry::default().with(filter).with(stderr).init();
}
