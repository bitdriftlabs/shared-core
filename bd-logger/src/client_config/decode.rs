use super::{BufferSelector, Configuration};
use anyhow::{Result, bail};
use bd_buffer::BufferSettings;
use bd_log_filter::FilterChain;
use bd_log_matcher::matcher::Tree;
use bd_proto::protos::bdtail::bdtail_config::DeviceCommandRequest;
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bd_proto_util::serialization::inline::views::api::{
  ConfigurationUpdate as UpdateView,
  ConfigurationUpdateUpdateType,
};
use bd_proto_util::serialization::inline::views::bdtail_config::BdTailConfigurations;
use bd_proto_util::serialization::inline::views::config::BufferConfigList;
use bd_workflows::config::WorkflowsConfiguration;
use protobuf::Chars;

impl Configuration {
  pub(super) fn from_bytes(bytes: &[u8]) -> Result<Self> {
    let update = UpdateView::from_proto_bytes(bytes)?;
    let Some(ConfigurationUpdateUpdateType::StateOfTheWorld(sow)) = update.update_type()? else {
      bail!("An invalid match configuration was received: missing oneof");
    };
    let buffers = sow.buffer_config_list()?.unwrap_or_default();
    let (filters, filter_parse_failures) =
      FilterChain::from_view(&sow.filters_configuration()?.unwrap_or_default())?;
    Ok(Self {
      buffer: buffer_settings(&buffers)?,
      buffer_selector: BufferSelector::from_view(&buffers)?,
      workflows: WorkflowsConfiguration::from_views(
        &sow.workflows_configuration()?.unwrap_or_default(),
        &sow.debug_workflows()?.unwrap_or_default(),
      )?,
      bdtail: TailUpdate::from_view(&sow.bdtail_configuration()?.unwrap_or_default())?,
      filters,
      filter_parse_failures,
    })
  }
}

fn buffer_settings(config: &BufferConfigList<'_>) -> Result<Vec<BufferSettings>> {
  config
    .buffer_config()?
    .iter()
    .map(|buffer| {
      let sizes = buffer
        .buffer_sizes()?
        .map(|sizes| {
          Ok::<_, anyhow::Error>((
            sizes.volatile_buffer_size_bytes()?,
            sizes.non_volatile_buffer_size_bytes()?,
          ))
        })
        .transpose()?
        .unwrap_or((10_000, 100_000));
      Ok(BufferSettings {
        name: buffer.name()?.to_owned(),
        id: buffer.id()?.to_owned(),
        type_: buffer.type_()?,
        volatile_buffer_size_bytes: sizes.0,
        non_volatile_buffer_size_bytes: sizes.1,
      })
    })
    .collect()
}

//
// TailUpdate
//

pub(super) struct TailUpdate {
  pub active_streams: Vec<(Chars, Option<Tree>)>,
  pub device_commands: Vec<DeviceCommandRequest>,
  pub has_live_streams: bool,
  pub parse_failures: u64,
}

impl TailUpdate {
  fn from_view(config: &BdTailConfigurations<'_>) -> Result<Self> {
    let mut update = Self {
      active_streams: Vec::new(),
      device_commands: Vec::new(),
      has_live_streams: false,
      parse_failures: 0,
    };
    for stream in config.active_streams()? {
      if let Some(command) = stream.device_command()? {
        if command.command_id.as_str() != stream.stream_id()? {
          bail!(
            "device command id {:?} does not match BDTail stream id {:?}",
            command.command_id,
            stream.stream_id()?
          );
        }
        update.device_commands.push(command);
        continue;
      }
      update.has_live_streams = true;
      let matcher = match stream.matcher()?.as_ref().map(Tree::from_view).transpose() {
        Ok(matcher) => matcher,
        Err(error) => {
          log::debug!("failed to parse stream match config: {error}");
          update.parse_failures += 1;
          continue;
        },
      };
      update
        .active_streams
        .push((stream.stream_id()?.to_owned().into(), matcher));
    }
    Ok(update)
  }
}
