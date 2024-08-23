//
// LogReplay
//

// An abstraction for logger replay. Introduced to make testing easier.
#[async_trait::async_trait]
pub trait LogReplay {
  fn create(filters_configuration: FiltersConfiguration) -> Self;

  async fn replay_log(
    &mut self,
    log: AnnotatedLogLine,
    log_processing_completed_tx: Option<oneshot::Sender<()>>,
    initialized_logging_context: &mut InitializedLoggingContext,
  ) -> anyhow::Result<()>;
}

//
// LoggerReplay
//

pub struct LoggerReplay {
  filters_configuration: FiltersConfiguration,
}

#[async_trait::async_trait]
impl LogReplay for LoggerReplay {
  fn create(filters_configuration: FiltersConfiguration) -> Self {
    Self {
      filters_configuration,
    }
  }

  async fn replay_log(
    &mut self,
    log: AnnotatedLogLine,
    log_processing_completed_tx: Option<oneshot::Sender<()>>,
    initialized_logging_context: &mut InitializedLoggingContext,
  ) -> anyhow::Result<()> {
    write_log_with_logging_context(
      &LogRef {
        log_level: log.log_level,
        log_type: log.log_type,
        message: &log.message,
        session_id: &log.session_id,
        occurred_at: log.occurred_at,
        fields: &FieldsRef::new(&log.fields, &log.matching_fields),
      },
      log_processing_completed_tx,
      initialized_logging_context,
    )
    .await
  }
}
