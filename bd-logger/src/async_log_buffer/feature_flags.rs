//! Lazy initialization wrapper for feature flags in the async log buffer.
//!
//! This module provides a holder type that defers feature flag initialization until first use,
//! avoiding blocking operations during log buffer creation.

use crate::async_log_buffer::LogLine;
use bd_error_reporter::reporter::handle_unexpected_error_with_details;
use bd_feature_flags::FeatureFlagsBuilder;
use bd_log::warn_every;
use bd_log_primitives::{AnnotatedLogField, LogType, log_level};
use time::ext::NumericalDuration;

/// A holder for feature flags that supports lazy initialization.
///
/// This enum manages the lifecycle of feature flags, starting in a pending state with just a
/// builder and transitioning to an initialized state on first access. Initialization is deferred
/// to avoid blocking operations during construction.
pub enum FeatureFlagsHolder {
  /// Feature flags have not been initialized yet, only the builder is available.
  Pending(FeatureFlagsBuilder),
  /// Feature flags have been initialized. Contains `None` if initialization failed.
  Initialized(Option<bd_feature_flags::FeatureFlags>),
}

impl FeatureFlagsHolder {
  /// Creates a new holder in the pending state with the given builder.
  ///
  /// Feature flags will not be initialized until the first call to a method that requires them.
  pub fn new(builder: FeatureFlagsBuilder) -> Self {
    Self::Pending(builder)
  }

  /// Returns a reference to the feature flags if they have been initialized.
  ///
  /// Returns `None` if the flags are still pending or if initialization failed.
  pub fn get(&self) -> Option<&bd_feature_flags::FeatureFlags> {
    match self {
      Self::Pending(_) => None,
      Self::Initialized(flags_option) => flags_option.as_ref(),
    }
  }

  /// Sets a feature flag to the specified variant.
  ///
  /// This will trigger initialization if the flags are still pending. If initialization fails,
  /// this operation will be a no-op.
  pub async fn set(&mut self, flag: String, variant: Option<&str>) -> LogLine {
    if let Some(feature_flags) = self.maybe_initialize_feature_flags().await {
      feature_flags
        .set(flag.clone(), variant)
        .unwrap_or_else(|e| {
          log::warn!("failed to set feature flag: {e}");
        });
    }

    LogLine::new_with_fields(
      log_level::INFO,
      LogType::Device,
      "Set feature flag".into(),
      [
        ("_set_flag".into(), AnnotatedLogField::new_ootb(flag)),
        (
          "_set_variant".into(),
          AnnotatedLogField::new_ootb(variant.unwrap_or("none").to_string()),
        ),
      ]
      .into(),
    )
  }

  /// Sets multiple feature flags at once.
  ///
  /// This will trigger initialization if the flags are still pending. If initialization fails,
  /// this operation will be a no-op.
  pub async fn set_multiple(&mut self, flags: Vec<(String, Option<String>)>) -> LogLine {
    if let Some(feature_flags) = self.maybe_initialize_feature_flags().await {
      feature_flags
        .set_multiple(flags.clone())
        .unwrap_or_else(|e| {
          log::warn!("failed to set feature flags: {e}");
        });
    }

    LogLine::new_with_fields(
      log_level::INFO,
      LogType::Device,
      "Set multiple feature flags".into(),
      flags
        .into_iter()
        .map(|(flag, variant)| {
          (
            format!("_set_flag_{flag}").into(),
            AnnotatedLogField::new_ootb(variant.unwrap_or("none".to_string())),
          )
        })
        .collect(),
    )
  }

  /// Removes a feature flag by name.
  ///
  /// This will trigger initialization if the flags are still pending. If initialization fails,
  /// this operation will be a no-op.
  pub async fn remove(&mut self, flag: String) -> LogLine {
    if let Some(feature_flags) = self.maybe_initialize_feature_flags().await {
      feature_flags.remove(&flag).unwrap_or_else(|e| {
        log::warn!("failed to remove feature flag: {e}");
      });
    }

    LogLine::new_with_fields(
      log_level::INFO,
      LogType::Device,
      "Removed feature flag".into(),
      [("_removed_flag".into(), AnnotatedLogField::new_ootb(flag))].into(),
    )
  }

  /// Clears all feature flags.
  ///
  /// This will trigger initialization if the flags are still pending. If initialization fails,
  /// this operation will be a no-op.
  pub async fn clear(&mut self) -> LogLine {
    if let Some(feature_flags) = self.maybe_initialize_feature_flags().await {
      feature_flags.clear().unwrap_or_else(|e| {
        log::warn!("failed to clear feature flags: {e}");
      });
    }

    LogLine::new_simple(
      log_level::INFO,
      LogType::Device,
      "Cleared all feature flags".into(),
    )
  }

  /// Lazily initializes the feature flags if they have not been initialized yet.
  async fn maybe_initialize_feature_flags(
    &mut self,
  ) -> Option<&mut bd_feature_flags::FeatureFlags> {
    if let Self::Pending(builder) = &self {
      let builder = builder.clone();
      let feature_flags = tokio::task::spawn_blocking(move || {
        // This should never fail unless there's a serious filesystem issue.
        // Treat this as an unexpected error so we get visibility into it.
        // If this keeps happening for normal reasons we can remove this later.
        builder
          .current_feature_flags()
          .map_err(|e| {
            handle_unexpected_error_with_details(e, "feature flag initialization", || None);
          })
          .ok()
      })
      .await
      .ok()
      .flatten();

      *self = Self::Initialized(feature_flags);
    }

    // TODO(snowp): Instead of failing permanently we should fall back to in-memory only.
    if let Self::Initialized(feature_flags) = self {
      feature_flags.as_mut()
    } else {
      warn_every!(
        30.seconds(),
        "feature flags failed to initialize, will not be available"
      );
      None
    }
  }
}
