use super::super::{
  FieldExtractionConfig,
  FieldExtractionSource,
  Predicate,
  SankeyExtraction,
  StateChangeMatch,
  TagValue,
  TransitionExtractions,
};
use super::actions::scope;
use anyhow::{anyhow, bail};
use bd_log_matcher::matcher::Tree;
use bd_log_matcher::state_value_matcher::StateValueMatcher;
use bd_proto_util::serialization::inline::views::save_field::SaveFieldSaveFieldType as SaveKind;
use bd_proto_util::serialization::inline::views::workflow::{
  self as wire,
  WorkflowRuleRuleType as RuleKind,
  WorkflowTransitionExtensionExtensionType as ExtensionKind,
  WorkflowTransitionExtensionSankeyDiagramValueExtractionValueType as SankeyKind,
};
use regex_lite::Regex;
use std::collections::HashMap;
use time::Duration;

impl Predicate {
  pub(super) fn from_view(message: &wire::WorkflowRule<'_>) -> anyhow::Result<Self> {
    Ok(
      match message
        .rule_type()?
        .ok_or_else(|| anyhow!("invalid transition configuration: missing rule type"))?
      {
        RuleKind::OnNewSession(_) => Self::OnNewSession,
        RuleKind::RuleLogMatch(rule) => Self::LogMatch {
          matcher: Tree::from_view(
            &rule
              .log_matcher()?
              .ok_or_else(|| anyhow!("missing protobuf message field 1"))?,
          )?,
          required_matches: rule.count()?,
        },
        RuleKind::RuleStateChangeMatch(rule) => Self::StateChangeMatch {
          state_change_match: StateChangeMatch {
            scope: scope(rule.scope()?)?,
            key: rule.key()?.to_owned(),
            previous_value: rule
              .previous_value()?
              .as_ref()
              .map(StateValueMatcher::from_view)
              .transpose()?,
            new_value: StateValueMatcher::from_view(
              &rule
                .new_value()?
                .ok_or_else(|| anyhow!("missing protobuf message field 4"))?,
            )?,
          },
          extra_matcher: rule
            .log_matcher()?
            .as_ref()
            .map(Tree::from_view)
            .transpose()?,
        },
        RuleKind::OnReport(_) => Self::OnReport,
        RuleKind::MatchRunCommand(rule) => {
          let command_selector = rule
            .command_selector()?
            .ok_or_else(|| anyhow!("missing protobuf message field 1"))?;
          if command_selector.command_selector.is_none() {
            bail!("invalid workflow command matcher configuration: missing command selector");
          }
          let interval = rule
            .minimum_execution_interval()?
            .ok_or_else(|| anyhow!("missing protobuf message field 2"))?;
          let minimum_execution_interval = Duration::new(interval.seconds()?, interval.nanos()?);
          if minimum_execution_interval <= Duration::ZERO {
            bail!(
              "invalid workflow command matcher configuration: minimum interval must be positive"
            );
          }
          Self::MatchRunCommand {
            command_selector,
            minimum_execution_interval,
            outcome_log_matcher: rule
              .outcome_log_matcher()?
              .as_ref()
              .map(Tree::from_view)
              .transpose()?,
          }
        },
      },
    )
  }
}

impl TransitionExtractions {
  pub(super) fn from_view(
    message: &wire::WorkflowTransition<'_>,
    limits: &HashMap<String, u32>,
  ) -> anyhow::Result<Self> {
    let mut result = Self {
      sankey_extractions: Vec::new(),
      timestamp_extraction_id: None,
      field_extractions: Vec::new(),
    };
    for extension in message.extensions()? {
      match extension.extension_type()? {
        None => {},
        Some(ExtensionKind::SankeyDiagramValueExtraction(value)) => {
          let id = value.sankey_diagram_id()?;
          let limit = limits.get(id).ok_or_else(|| {
            anyhow!("invalid transition configuration: missing sankey limit for sankey id {id:?}")
          })?;
          let extracted = match value.value_type()?.ok_or_else(|| {
            anyhow!("invalid sankey value extraction configuration: missing value type")
          })? {
            SankeyKind::Fixed(value) => TagValue::Fixed(value.to_owned()),
            SankeyKind::FieldExtracted(field) => TagValue::field_from_view(&field)?,
          };
          result.sankey_extractions.push(SankeyExtraction {
            sankey_id: id.to_owned(),
            value: extracted,
            counts_toward_sankey_values_extraction_limit: value
              .counts_toward_sankey_extraction_limit()?,
            limit: (*limit).try_into()?,
          });
        },
        Some(ExtensionKind::SaveTimestamp(value)) => {
          if result.timestamp_extraction_id.is_some() {
            bail!("invalid transition configuration: multiple timestamp extractions");
          }
          result.timestamp_extraction_id = Some(value.id()?.to_owned());
        },
        Some(ExtensionKind::SaveField(value)) => {
          let source = match value
            .save_field_type()?
            .ok_or_else(|| anyhow!("invalid transition configuration: missing save field type"))?
          {
            SaveKind::FieldName(name) => FieldExtractionSource::FieldName(name.to_owned()),
            SaveKind::Message(_) => FieldExtractionSource::Message,
          };
          let regex_capture = value
            .regex_capture()?
            .map(|pattern| {
              let regex = Regex::new(pattern).map_err(|error| {
                anyhow!("invalid transition configuration: invalid regex capture ({error})")
              })?;
              if regex.captures_len() != 2 {
                bail!(
                  "invalid transition configuration: regex capture must contain exactly one \
                   capture group"
                );
              }
              Ok(regex)
            })
            .transpose()?;
          result.field_extractions.push(FieldExtractionConfig {
            source,
            regex_capture,
            id: value.id()?.to_owned(),
          });
        },
      }
    }
    Ok(result)
  }
}
