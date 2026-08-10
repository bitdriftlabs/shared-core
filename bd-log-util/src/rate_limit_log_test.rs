use super::warn_every;
use time::ext::NumericalDuration;

fn test_warn() {
  warn_every!(1.seconds(), "{}", "function");
}

fn test_warn_captured_arg() {
  let err = "boom";
  warn_every!(1.seconds(), "hello: {err}");
}

fn test_warn_runtime_string() {
  let message = "runtime string";
  warn_every!(1.seconds(), "{message}");
}

#[tokio::test(start_paused = true)]
async fn rate_limit_log() {
  warn_every!(1.seconds(), "{}", "hello");
  warn_every!(1.seconds(), "{}", "world");
  test_warn();
  test_warn();
  test_warn_captured_arg();
  test_warn_runtime_string();
}
