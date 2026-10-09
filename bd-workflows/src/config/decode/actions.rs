use super::super::{
  Action,
  ActionEmitMetric,
  ActionEmitSankey,
  ActionFlushBuffers,
  FlushBufferId,
  JsonFieldExtraction,
  MetricMultiTag,
  Streaming,
  TagValue,
  ValueIncrement,
  parse_state_scope,
};
use anyhow::{anyhow, bail};
use bd_log_matcher::matcher::parse_json_path_view;
use bd_proto::protos::state::scope::StateScope;
use bd_proto_util::serialization::inline::ProtoDeserialize;
use bd_proto_util::serialization::inline::views::workflow::{
  self as wire,
  WorkflowActionActionEmitMetricMetricType as MetricKind,
  WorkflowActionActionEmitMetricValueExtractorType as IncrementKind,
  WorkflowActionActionFlushBuffersStreamingTerminationCriterionType as CriterionKind,
  WorkflowActionActionType as ActionKind,
  WorkflowActionTagTagType as TagKind,
  WorkflowFieldExtractedExtractionType as ExtractionKind,
};
use bd_state::Scope;
use bd_stats_common::MetricType;
use protobuf::EnumOrUnknown;
use std::collections::BTreeMap;

impl Action {
  pub(super) fn from_view(message: &wire::WorkflowAction<'_>) -> anyhow::Result<Self> {
    Ok(
      match message
        .action_type()?
        .ok_or_else(|| anyhow!("invalid action configuration: missing action type"))?
      {
        ActionKind::ActionFlushBuffers(action) => Self::FlushBuffers(ActionFlushBuffers {
          id: FlushBufferId::WorkflowActionId(action.id()?.to_owned()),
          buffer_ids: action.buffer_ids()?.into_iter().collect(),
          streaming: action
            .streaming()?
            .map(|streaming| {
              let mut max_logs_count = None;
              for criterion in streaming.termination_criteria()? {
                if let Some(CriterionKind::LogsCount(limit)) = criterion.type_()? {
                  let count = limit.max_logs_count()?;
                  max_logs_count =
                    Some(max_logs_count.map_or(count, |previous: u64| previous.min(count)));
                }
              }
              if max_logs_count == Some(0) {
                bail!("invalid streaming configuration: max_logs_count has to be greater than 0");
              }
              Ok(Streaming {
                destination_continuous_buffer_ids: streaming
                  .destination_streaming_buffer_ids()?
                  .into_iter()
                  .collect(),
                max_logs_count,
              })
            })
            .transpose()?,
        }),
        ActionKind::ActionEmitMetric(action) => {
          let metric_type = match action.metric_type()? {
            Some(MetricKind::Counter(_)) => MetricType::Counter,
            Some(MetricKind::Histogram(_)) => MetricType::Histogram,
            None => bail!("invalid action emit metric configuration: missing metric_type"),
          };
          let increment = match action.value_extractor_type()? {
            Some(IncrementKind::Fixed(value)) => ValueIncrement::Fixed(u64::from(value)),
            Some(IncrementKind::FieldExtracted(extracted)) => {
              match TagValue::field_from_view(&extracted)? {
                TagValue::JsonExtract(path) => ValueIncrement::JsonExtract(path),
                TagValue::FieldExtract(field) => ValueIncrement::Extract(field),
                _ => bail!("invalid field extraction"),
              }
            },
            None => bail!("invalid action emit metric configuration: unknown value_extractor_type"),
          };
          Self::EmitMetric(ActionEmitMetric {
            id: action.id()?.to_owned(),
            tags: tags(&action.tags()?)?,
            metric_type,
            increment,
            multi_tag: action
              .multi_tag()?
              .map(|tag| MetricMultiTag::from_inline(tag.as_message()))
              .transpose()?,
          })
        },
        ActionKind::ActionEmitSankeyDiagram(action) => Self::EmitSankey(ActionEmitSankey {
          id: action.id()?.to_owned(),
          limit: action.limit()?,
          tags: tags(&action.tags()?)?,
        }),
        ActionKind::ActionGenerateLog(action) => Self::GenerateLog(action),
        ActionKind::ActionStartTracing(_) => Self::StartTracing,
        ActionKind::ActionRunCommand(_) => bail!("workflow run command actions are not supported"),
      },
    )
  }
}

fn tags(tags: &[wire::WorkflowActionTag<'_>]) -> anyhow::Result<BTreeMap<String, TagValue>> {
  tags
    .iter()
    .map(|tag| {
      let value = match tag
        .tag_type()?
        .ok_or_else(|| anyhow!("invalid action emit metric configuration: unknown tag_type"))?
      {
        TagKind::FixedValue(value) => TagValue::Fixed(value.to_owned()),
        TagKind::FieldExtracted(field) => TagValue::field_from_view(&field)?,
        TagKind::LogBodyExtracted(_) => TagValue::LogBodyExtract,
        TagKind::StateExtracted(state) => {
          TagValue::StateExtract(scope(state.scope()?)?, state.key()?.to_owned())
        },
      };
      Ok((tag.name()?.to_owned(), value))
    })
    .collect()
}

impl TagValue {
  pub(super) fn field_from_view(
    message: &wire::WorkflowFieldExtracted<'_>,
  ) -> anyhow::Result<Self> {
    let field_name = message.field_name()?.to_owned();
    if let Some(ExtractionKind::JsonPath(path)) = message.extraction_type()? {
      Ok(Self::JsonExtract(JsonFieldExtraction {
        field_name,
        path: path
          .key_or_index()?
          .iter()
          .map(parse_json_path_view)
          .collect::<anyhow::Result<_>>()?,
      }))
    } else {
      Ok(Self::FieldExtract(field_name))
    }
  }
}

pub(super) fn scope(value: EnumOrUnknown<StateScope>) -> anyhow::Result<Scope> {
  parse_state_scope(value.enum_value_or_default())
}
