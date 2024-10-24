use bd_runtime::runtime::{session_replay, BoolWatch, ConfigLoader, DurationWatch};
use bd_shutdown::{ComponentShutdown, ComponentShutdownTrigger};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{Instant, Interval};

#[cfg(test)]
#[ctor::ctor]
fn test_global_init() {
  bd_test_helpers::test_global_init();
}

pub trait Target {
  fn capture_wireframes(&self);

  fn take_screenshot(&self);
}

//
// Recorder
//

pub struct Recorder {
  target: Box<dyn Target + Send + Sync>,

  is_enabled: bool,
  reporting_interval_rate: Duration,
  reporting_interval: Option<Interval>,

  take_screenshot_rx: Receiver<()>,

  is_enabled_flag: BoolWatch<session_replay::EnabledFlag>,
  reporting_interval_flag: DurationWatch<session_replay::ReportingIntervalFlag>,
}

impl Recorder {
  pub fn new(
    target: Box<dyn Target + Send + Sync>,
    runtime_loader: &Arc<ConfigLoader>,
  ) -> (Self, Sender<()>) {
    let (take_screenshot_tx, take_screenshot_rx) = channel(2);

    let mut is_enabled = session_replay::EnabledFlag::register(runtime_loader).unwrap();
    let reporting_interval_rate = session_replay::ReportingIntervalFlag::register(runtime_loader)
      .unwrap()
      .read_mark_update();

    (
      Self {
        target,
        is_enabled: is_enabled.read_mark_update(),
        reporting_interval_rate: reporting_interval_rate.unsigned_abs(),
        reporting_interval: None,
        take_screenshot_rx,
        is_enabled_flag: session_replay::EnabledFlag::register(runtime_loader).unwrap(),
        reporting_interval_flag: session_replay::ReportingIntervalFlag::register(runtime_loader)
          .unwrap(),
      },
      take_screenshot_tx,
    )
  }

  fn create_interval(interval: Duration, fire_immediately: bool) -> tokio::time::Interval {
    let mut interval = if fire_immediately {
      tokio::time::interval(interval)
    } else {
      tokio::time::interval_at(Instant::now() + interval, interval)
    };

    log::debug!("session replay interval is {:?}", interval.period());

    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
  }

  pub async fn run(&mut self) {
    let shutdown_trigger = ComponentShutdownTrigger::default();
    self
      .run_with_shutdown(shutdown_trigger.make_shutdown())
      .await;
  }

  pub async fn run_with_shutdown(&mut self, mut shutdown: ComponentShutdown) {
    if self.reporting_interval.is_none() {
      self.reporting_interval = Some(Self::create_interval(self.reporting_interval_rate, true));
    }

    let local_shutdown = shutdown.cancelled();
    tokio::pin!(local_shutdown);

    loop {
      tokio::select! {
        () = async {
          if let Some(reporting_interval) = &mut self.reporting_interval {
            reporting_interval.tick().await;
          }
        }, if self.reporting_interval.is_some() && self.is_enabled => {
          log::debug!("session replay recorder capturing wireframe");
          self.target.capture_wireframes();
        },
        _ = self.reporting_interval_flag.changed() => {
          self.reporting_interval = Some(
            Self::create_interval(
              self.reporting_interval_flag.read().unsigned_abs(),
              false
            )
          );
        },
        _ = self.take_screenshot_rx.recv() => {
          log::debug!("session replay recorder taking screenshot");
          self.target.take_screenshot();
        },
        _ = self.is_enabled_flag.changed() => {
          self.is_enabled = self.is_enabled_flag.read();
        },
        () = &mut local_shutdown => {
          return;
        },
      }
    }
  }
}
