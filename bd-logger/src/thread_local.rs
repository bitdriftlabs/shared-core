// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::logging_state::{BufferProducers, InitializedLoggingContext};
use bd_buffer::{AbslCode, BuffersWithAck, Error};
use bd_client_common::fb::make_log;
use bd_client_stats::FlushTrigger;
use bd_log_primitives::{log_level, LogRef};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use std::borrow::Cow;
use std::collections::BTreeSet;

/// TODO(Augustyniak): Move out of `thread_local` module.
/// Called on the event loop used to process logs.
pub(crate) async fn write_log_with_logging_context(
  log: &LogRef<'_>,
  log_processing_completed_tx: Option<tokio::sync::oneshot::Sender<()>>,
  logging_context: &mut InitializedLoggingContext,
) -> anyhow::Result<()> {
  logging_context
    .stats
    .log_level_counters
    .record(log.log_level);

  let flush_stats_trigger = logging_context.flush_stats_trigger.clone();
  let flush_buffers_tx = logging_context.flush_buffers_tx.clone();

  match logging_context
    .tail_configs
    .maybe_stream_log(&mut logging_context.buffer_producers, log)
  {
    Ok(streamed) => {
      if streamed {
        logging_context.stats.streamed_logs.inc();
      }
    },
    Err(e) => {
      log::debug!("failed to stream log: {e:?}");
    },
  }

  let mut matching_buffers =
    logging_context
      .buffer_selector
      .buffers(log.log_type, log.log_level, log.message, log.fields);

  if let Some(workflows_engine) = logging_context.workflows_engine.as_mut() {
    let result = workflows_engine.process_log(log, &matching_buffers);

    log::debug!(
      "processed {:?} log, destination buffer(s): {:?}",
      log.message.as_str().unwrap_or("[DATA]"),
      result.log_destination_buffer_ids,
    );

    if !result.triggered_flush_buffers_action_ids.is_empty() {
      log::debug!(
        "triggered flush buffer action IDs: {:?}",
        result.triggered_flush_buffers_action_ids
      );
    }

    write_to_buffers(
      &mut logging_context.buffer_producers,
      &result.log_destination_buffer_ids,
      log,
      result.triggered_flush_buffers_action_ids.iter().copied(),
    )?;

    if let Some(extra_matching_buffer) = process_flush_buffers_actions(
      &result.triggered_flush_buffers_action_ids,
      &mut logging_context.buffer_producers,
      &result.triggered_flushes_buffer_ids,
      &result.log_destination_buffer_ids,
      log,
    ) {
      // We emitted a synthetic log. Add the buffer it was written to to the list of matching
      // buffers.
      matching_buffers.insert(extra_matching_buffer.into());
    }

    // Check whether the caller is waiting for the log processing to complete. We call such logs
    // "blocking".
    let is_blocking = log_processing_completed_tx.is_some();

    // Force the persistence of workflows state to disk if log is blocking.
    workflows_engine.maybe_persist(is_blocking).await;
  } else {
    write_to_buffers(
      &mut logging_context.buffer_producers,
      &matching_buffers,
      log,
      std::iter::empty(),
    )?;
  }

  if let Some(log_processing_completed_tx) = log_processing_completed_tx {
    let result =
      finish_blocking_log_processing(flush_buffers_tx, flush_stats_trigger, matching_buffers).await;

    log_processing_completed_tx
      .send(())
      .map_err(|e| anyhow::anyhow!("failed to send log processing completion signal: {e:?}"))?;

    return result;
  }

  Ok(())
}

async fn finish_blocking_log_processing(
  flush_buffers_tx: tokio::sync::mpsc::Sender<BuffersWithAck>,
  flush_stats_trigger: Option<FlushTrigger>,
  matching_buffers: BTreeSet<Cow<'_, str>>,
) -> anyhow::Result<()> {
  // The processing of a blocking log is about to complete.
  // We make an arbitrary decision to start with the flushing of log buffers to disk first and move
  // on to flushing stats to disk next.

  log::debug!("blocking log: sending signal to flush buffers after log");

  let flush_stats_trigger = flush_stats_trigger.clone();

  if matching_buffers.is_empty() {
    // Return early to avoid unnecessary tokio messages.
    log::debug!(
      "blocking log: log processed but no buffers matched, returning without waiting for buffers \
       flush"
    );
  } else {
    let (tx, rx) = bd_completion::Sender::new();
    // call the sender to flush the buffers using the tx that was created along with the logger
    let buffers_to_flush = BuffersWithAck::new(
      matching_buffers.iter().map(ToString::to_string).collect(),
      Some(tx),
    );

    let result = flush_buffers_tx.send(buffers_to_flush).await;
    if let Err(e) = result {
      anyhow::bail!("blocking log: failed to send signal to flush buffer(s): {e:?}");
    }

    if let Err(e) = rx.recv().await {
      anyhow::bail!("blocking log: failed to receive buffer(s) flush completion signal: {e:?}");
    }
  }

  // If the log is blocking, we need to flush the stats to disk.
  if let Some(flush_stats_tx) = flush_stats_trigger {
    log::debug!("blocking log: sending signal to flush stats to disk");
    let (sender, receiver) = bd_completion::Sender::new();
    if let Err(e) = flush_stats_tx.flush(Some(sender)).await {
      anyhow::bail!("blocking log: failed to send signal to flush stats: {e:?}");
    }

    return receiver.recv().await.map_err(|e| {
      anyhow::anyhow!("failed to await receiving flush stats trigger completion: {e:?}")
    });
  }

  Ok(())
}

fn write_to_buffers<'a>(
  buffers: &mut BufferProducers,
  matching_buffers: &BTreeSet<Cow<'_, str>>,
  log: &LogRef<'_>,
  workflow_flush_buffer_action_ids: impl Iterator<Item = &'a str>,
) -> anyhow::Result<()> {
  if matching_buffers.is_empty() {
    return Ok(());
  }

  make_log(
    &mut buffers.builder,
    log.log_level,
    log.log_type,
    log.message,
    log.fields.captured_fields,
    log.session_id,
    &log.occurred_at,
    workflow_flush_buffer_action_ids,
    std::iter::empty(),
    |data| {
      for buffer in matching_buffers {
        // TODO(snowp): For both logger and buffer lookup we end up doing a map lookup, which
        // seems less than ideal in the logging path. Look into ways to optimize this,
        // possibly via vector indices instead of string keys.
        match BufferProducers::producer(&mut buffers.buffers, buffer)?.write(data) {
          // If the buffer is locked, drop the error. This helps ensure that we are able to
          // log to all buffers even if one of them is locked.
          // TODO(snowp): Track how often logs are dropped due to locks.
          // If the buffer is out of space, drop the error.
          // TODO(mattklein123): Track this via stats.
          Err(Error::AbslStatus(AbslCode::FailedPrecondition | AbslCode::ResourceExhausted, _)) => {
            Ok(())
          },
          e => e,
        }?;
      }
      Ok(())
    },
  )?;

  Ok(())
}

fn process_flush_buffers_actions(
  triggered_flush_buffers_action_ids: &BTreeSet<&str>,
  buffers: &mut BufferProducers,
  triggered_flushes_buffer_ids: &BTreeSet<Cow<'_, str>>,
  written_to_buffers: &BTreeSet<Cow<'_, str>>,
  log: &LogRef<'_>,
) -> Option<String> {
  if triggered_flush_buffers_action_ids.is_empty() {
    return None;
  }

  // Indicates whether the log was written to any of the continuous buffers. Continuous buffers are
  // periodically uploaded, so if the log was written to a continuous buffer, we assume that it will
  // eventually be uploaded to the remote.
  let written_to_continuous_buffer =
    !written_to_buffers.is_disjoint(&buffers.continuous_buffer_ids);

  // Indicates whether the log was written to any of the trigger buffers. Trigger buffers are
  // uploaded only if there is a workflow action that instructs the system to flush them.
  // Therefore, we assume that the log ends up being uploaded only if it is directed to one of the
  // trigger buffers that is about to be flushed.
  let written_to_flushed_trigger_buffer =
    !written_to_buffers.is_disjoint(&buffers.trigger_buffer_ids);

  let is_log_about_to_be_uploaded =
    written_to_continuous_buffer || written_to_flushed_trigger_buffer;

  // The log that triggered a flush of the buffer(s) has not been written to any of the buffers, so
  // it's not going to be flushed. To work around that, create a synthetic log that resembles the
  // original log and add it to one of the buffers scheduled to be uploaded.
  if !is_log_about_to_be_uploaded && !triggered_flushes_buffer_ids.is_empty() {
    // Select a 'random' buffer from the 'list' of buffers scheduled for flushing.
    let arbitrary_buffer_id_to_flush =
      (*triggered_flushes_buffer_ids.iter().next().unwrap()).to_string();

    log::debug!(
      "adding synthetic log \"{:?}\" to \"{}\" buffer; flush buffer action IDs {:?}",
      log.message,
      arbitrary_buffer_id_to_flush,
      triggered_flush_buffers_action_ids
    );

    let result = make_log(
      &mut buffers.builder,
      log_level::DEBUG,
      LogType::InternalSDK,
      log.message,
      log.fields.captured_fields,
      log.session_id,
      &log.occurred_at,
      triggered_flush_buffers_action_ids.clone().into_iter(),
      std::iter::empty(),
      |synthetic_log| {
        if let Ok(buffer_producer) =
          BufferProducers::producer(&mut buffers.buffers, arbitrary_buffer_id_to_flush.as_str())
        {
          if let Err(e) = buffer_producer.write(synthetic_log) {
            log::debug!("failed to write synthetic log to buffer: {e}");
          }
        }

        Ok(())
      },
    );
    if let Err(e) = result {
      log::debug!("failed to make a synhetic log: {e}");
      return None;
    }

    return Some(arbitrary_buffer_id_to_flush);
  }

  None
}
