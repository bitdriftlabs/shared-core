// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! bitdrift Async Runtime Utilities
//! ============================
//!
//! This library is, more or less, some common bits of running an async service
//! at bitdrift. It provides a sensible default tokio executor for your environment,
//! and TODO: more stuff.

use std::num::NonZeroUsize;
use std::sync::LazyLock;
use tokio::runtime::{Builder, Runtime};

/// Reasonable upper bound on the number of threads we'll ever want to run in a
/// threadpool. This is used to handle runtimes where we guess the CPU count
/// wildly wrong.
pub const MAX_RT_THREADPOOL_SIZE: usize = 4096;

/// Guess the number of CPU cores we've got available to us.
///
/// First checks the `NUM_CPUS` environment variable to see if we've been given
/// an explicit hint by e.g. our init system or kubelet or w/e, then falls back
/// to the number of CPUs the host reports.
///
/// Returns values in the range `[1, USIZE_MAX]`.
pub fn num_cpus() -> usize {
  // Extract the number of CPU shares we're allocated from the NUM_CPUS env
  // var, if it's given. Kubernetes environments will use this to tell us at
  // what point they'll start niceing us, so it behooves us to try not to
  // allocate giant threadpools if we only get one cpu worth of time anyway.
  if let Ok(num_cpus_env) = std::env::var("NUM_CPUS") {
    match num_cpus_env.parse::<f64>() {
      Ok(fractional_cpus) => {
        if !fractional_cpus.is_normal() || fractional_cpus.is_sign_negative() {
          log::warn!("env variable NUM_CPUS set to invalid float `{fractional_cpus}`; ignoring it");
        } else {
          #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
          let num_threads = fractional_cpus.ceil() as usize;
          return num_threads.clamp(1, MAX_RT_THREADPOOL_SIZE);
        }
      },
      Err(_) => {
        log::warn!("non-numeric value in `NUM_CPUS` environment variable; ignoring it");
      },
    }
  }
  let num_threads = std::thread::available_parallelism().map_or_else(
    |err| {
      log::error!("couldn't estimate number of parallel threads on host, defaulting to one: {err}");
      1
    },
    NonZeroUsize::get,
  );
  num_threads.clamp(1, MAX_RT_THREADPOOL_SIZE)
}

/// The number of CPUs available to this program.
///
/// Takes into account Kubernetes CPU share allocation, if known.
///
/// This is calculated on first access by running [`num_cpus()`] and
/// memoizing the result here, so subsequent CPU hotplugs or environment
/// writes won't chagne this value.
pub static NUM_CPUS: LazyLock<usize> = LazyLock::new(num_cpus);

/// Create a new tokio runtime with Lyne default settings.
///
/// * Infer worker pool size from the environment
/// * Use single-threaded runtime if appropriate
/// * Name threads recognizably
pub fn new_runtime() -> Result<Runtime, std::io::Error> {
  #[cfg(target_arch = "aarch64")]
  log::info!(
    "AArch64 binary compiled with CPU features: neon={} crc={} lse={} dotprod={} i8mm={} bf16={} \
     sve={} sve2={}",
    cfg!(target_feature = "neon"),
    cfg!(target_feature = "crc"),
    cfg!(target_feature = "lse"),
    cfg!(target_feature = "dotprod"),
    cfg!(target_feature = "i8mm"),
    cfg!(target_feature = "bf16"),
    cfg!(target_feature = "sve"),
    cfg!(target_feature = "sve2"),
  );

  let num_cpus = *NUM_CPUS;
  let mut tokio_rt_builder = if num_cpus == 1 {
    log::info!("NUM_CPUS is 1, using a single-threaded tokio runtime");
    Builder::new_current_thread()
  } else {
    log::info!("NUM_CPUS is {num_cpus}, using multithreaded tokio runtime");
    let mut rtb = Builder::new_multi_thread();
    rtb.worker_threads(num_cpus);
    rtb
  };
  let tokio = tokio_rt_builder
    .enable_all()
    .thread_name_fn(|| format!("bitdrift_rt_{:x?}", std::thread::current().id()))
    .build()?;
  Ok(tokio)
}
