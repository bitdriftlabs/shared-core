// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.

#[cfg(target_os = "linux")]
mod linux_only {
  use bd_event_buffer::{
    AdmissionOutcome,
    CapturedLog,
    EventBuffer,
    EventBufferEntry,
    EventBufferLimits,
    LoggerFieldMapLimits,
  };
  use bd_log_primitives::{AnnotatedLogFields, LogFieldValue, LogLine, log_level};
  use bd_proto::protos::logging::payload::LogType;
  use gungraun::{
    Callgrind,
    EntryPoint,
    LibraryBenchmarkConfig,
    library_benchmark,
    library_benchmark_group,
  };
  use std::hint::black_box;

  struct Fixture {
    buffer: EventBuffer,
    low: Option<EventBufferEntry>,
    high: Option<EventBufferEntry>,
    protected: Option<EventBufferEntry>,
    runtime: tokio::runtime::Runtime,
  }

  impl Fixture {
    fn empty() -> Self {
      Self::with_limit(16 * 1024)
    }
    fn low_full() -> Self {
      let low = log(log_level::DEBUG, LogType::NORMAL, 0);
      let fixture = Self::with_limit(low.size());
      assert_eq!(AdmissionOutcome::Admitted, fixture.buffer.admit(low));
      fixture
    }
    fn low_then_high() -> Self {
      let low = log(log_level::DEBUG, LogType::NORMAL, 0);
      let high = log(log_level::INFO, LogType::NORMAL, 0);
      let fixture = Self::with_limit(low.size() + high.size() - 1);
      assert_eq!(AdmissionOutcome::Admitted, fixture.buffer.admit(low));
      fixture
    }
    fn low_then_protected() -> Self {
      let low = log(log_level::DEBUG, LogType::NORMAL, 0);
      let protected = log(log_level::INFO, LogType::LIFECYCLE, 0);
      let fixture = Self::with_limit(low.size() + protected.size() - 1);
      assert_eq!(AdmissionOutcome::Admitted, fixture.buffer.admit(low));
      fixture
    }
    fn many_low(count: usize) -> Self {
      let low = log(log_level::DEBUG, LogType::NORMAL, 0);
      let mut fixture = Self::with_limit(count * low.size());
      let incoming = log(
        log_level::INFO,
        LogType::NORMAL,
        count * low.size() - log(log_level::INFO, LogType::NORMAL, 0).size(),
      );
      fixture.high = Some(incoming);
      for _ in 0 .. count {
        assert_eq!(
          AdmissionOutcome::Admitted,
          fixture
            .buffer
            .admit(log(log_level::DEBUG, LogType::NORMAL, 0))
        );
      }
      fixture
    }
    fn many_low_then_protected(count: usize) -> Self {
      let mut fixture = Self::many_low(count);
      fixture.protected = Some(log(
        log_level::INFO,
        LogType::LIFECYCLE,
        count * log(log_level::DEBUG, LogType::NORMAL, 0).size()
          - log(log_level::INFO, LogType::LIFECYCLE, 0).size(),
      ));
      fixture
    }
    fn many_low_then_log_limit_shrink(count: usize) -> Self {
      let mut fixture = Self::many_low(count);
      let protected = log(log_level::INFO, LogType::LIFECYCLE, 0);
      fixture.buffer.set_pending_limits(EventBufferLimits {
        log_limit_bytes: 0,
        total_limit_bytes: count * log(log_level::DEBUG, LogType::NORMAL, 0).size()
          + protected.size(),
      });
      fixture.protected = Some(protected);
      fixture
    }
    fn shared_fields(count: usize) -> Self {
      let buffer = EventBuffer::new(
        EventBufferLimits {
          log_limit_bytes: 16 * 1024,
          total_limit_bytes: 16 * 1024,
        },
        LoggerFieldMapLimits {
          max_fields: count + 1,
          max_bytes: 32 * 1024,
        },
      );
      buffer.reserve_fixture_capacity();
      let fixture = Self {
        buffer,
        low: None,
        high: None,
        protected: None,
        runtime: tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .expect("benchmark runtime construction must succeed"),
      };
      for index in 0 .. count {
        assert!(
          fixture
            .buffer
            .set_field(
              format!("field_{index}").into(),
              LogFieldValue::String("value".into())
            )
            .is_ok()
        );
      }
      assert_eq!(
        AdmissionOutcome::Admitted,
        fixture
          .buffer
          .admit(log(log_level::INFO, LogType::NORMAL, 0))
      );
      fixture
    }
    fn with_limit(limit: usize) -> Self {
      let buffer = EventBuffer::new(
        EventBufferLimits {
          log_limit_bytes: limit,
          total_limit_bytes: limit,
        },
        LoggerFieldMapLimits::default(),
      );
      buffer.reserve_fixture_capacity();
      Self {
        buffer,
        low: Some(log(log_level::DEBUG, LogType::NORMAL, 0)),
        high: Some(log(log_level::INFO, LogType::NORMAL, 0)),
        protected: Some(log(log_level::INFO, LogType::LIFECYCLE, 0)),
        runtime: tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .expect("benchmark runtime construction must succeed"),
      }
    }
    fn admit_low(&mut self) -> bool {
      self
        .low
        .take()
        .is_some_and(|entry| self.buffer.admit(entry) == AdmissionOutcome::Admitted)
    }
    fn admit_high(&mut self) -> bool {
      self
        .high
        .take()
        .is_some_and(|entry| self.buffer.admit(entry) == AdmissionOutcome::Admitted)
    }
    fn admit_protected(&mut self) -> bool {
      self
        .protected
        .take()
        .is_some_and(|entry| self.buffer.admit(entry) == AdmissionOutcome::Admitted)
    }
    fn drain_one(&self) -> bool {
      !self.runtime.block_on(self.buffer.next_batch(1)).is_empty()
    }
    fn update_field(&self) -> bool {
      self
        .buffer
        .set_field("benchmark".into(), LogFieldValue::String("value".into()))
        .is_ok()
    }
  }

  fn log(
    level: bd_log_primitives::LogLevel,
    log_type: LogType,
    message_bytes: usize,
  ) -> EventBufferEntry {
    EventBufferEntry::Log(CapturedLog::new(
      LogLine {
        log_level: level,
        log_type,
        message: "x".repeat(message_bytes).into(),
        fields: AnnotatedLogFields::default(),
        matching_fields: AnnotatedLogFields::default(),
        attributes_overrides: None,
        capture_session: None,
      },
      false,
      None,
    ))
  }
  type Setup = fn() -> Fixture;
  fn measure<T>(operation: impl FnOnce() -> T) {
    gungraun::client_requests::callgrind::start_instrumentation();
    let _ = black_box(operation());
    gungraun::client_requests::callgrind::stop_instrumentation();
  }
  fn config() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(
      Callgrind::with_args(["--instr-atstart=no", "--dump-instr=yes"])
        .entry_point(EntryPoint::None),
    );
    config
  }
  #[library_benchmark(config = config())]
  #[bench::empty(Fixture::empty)]
  #[bench::full(Fixture::low_full)]
  fn admit_low(setup: Setup) {
    let mut fixture = setup();
    measure(|| fixture.admit_low());
  }
  #[library_benchmark(config = config())]
  #[bench::empty(Fixture::empty)]
  #[bench::one_low_victim(Fixture::low_then_high)]
  #[bench::eight_low_victims(many_low_8)]
  #[bench::sixty_four_low_victims(many_low_64)]
  #[bench::two_hundred_fifty_six_low_victims(many_low_256)]
  fn admit_high(setup: Setup) {
    let mut fixture = setup();
    measure(|| fixture.admit_high());
  }
  #[library_benchmark(config = config())]
  #[bench::one_low_victim(Fixture::low_then_protected)]
  #[bench::sixty_four_low_victims(many_low_then_protected_64)]
  fn admit_protected(setup: Setup) {
    let mut fixture = setup();
    measure(|| fixture.admit_protected());
  }
  #[library_benchmark(config = config())]
  #[bench::sixty_four_logs(many_low_then_log_limit_shrink_64)]
  #[bench::two_hundred_fifty_six_logs(many_low_then_log_limit_shrink_256)]
  fn apply_log_limit_shrink(setup: Setup) {
    let mut fixture = setup();
    measure(|| fixture.admit_protected());
  }
  #[library_benchmark(config = config())]
  #[bench::one_entry(Fixture::low_full)]
  fn drain_one(setup: Setup) {
    let fixture = setup();
    measure(|| fixture.drain_one());
  }
  #[library_benchmark(config = config())]
  #[bench::unique(Fixture::empty)]
  #[bench::shared_8(shared_8)]
  #[bench::shared_64(shared_64)]
  #[bench::shared_128(shared_128)]
  fn update_field(setup: Setup) {
    let fixture = setup();
    measure(|| fixture.update_field());
  }
  fn many_low_8() -> Fixture {
    Fixture::many_low(8)
  }
  fn many_low_64() -> Fixture {
    Fixture::many_low(64)
  }
  fn many_low_then_protected_64() -> Fixture {
    Fixture::many_low_then_protected(64)
  }
  fn many_low_then_log_limit_shrink_64() -> Fixture {
    Fixture::many_low_then_log_limit_shrink(64)
  }
  fn many_low_256() -> Fixture {
    Fixture::many_low(256)
  }
  fn many_low_then_log_limit_shrink_256() -> Fixture {
    Fixture::many_low_then_log_limit_shrink(256)
  }
  fn shared_8() -> Fixture {
    Fixture::shared_fields(8)
  }
  fn shared_64() -> Fixture {
    Fixture::shared_fields(64)
  }
  fn shared_128() -> Fixture {
    Fixture::shared_fields(128)
  }
  library_benchmark_group!(
    name = benches;
    benchmarks = admit_low, admit_high, admit_protected, apply_log_limit_shrink, drain_one,
      update_field
  );
}
#[cfg(target_os = "linux")]
fn main() {
  use gungraun::main;
  use linux_only::benches;
  main!(library_benchmark_groups = benches);
  main();
}
#[cfg(not(target_os = "linux"))]
fn main() {
  println!("EventBuffer callgrind benchmarks are only available on Linux");
}
