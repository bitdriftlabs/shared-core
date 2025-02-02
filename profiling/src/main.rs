#[cfg(not(target_family = "wasm"))]
mod paths;
#[cfg(not(target_family = "wasm"))]
mod profiling;

#[cfg(not(target_family = "wasm"))]
#[tokio::main]
pub async fn main() {
  bd_log::SwapLogger::initialize();

  crate::profiling::run_network_requests_profiling();
}

#[cfg(target_family = "wasm")]
pub fn main() {
  unimplemented!("This binary is not meant to be run in a browser environment.");
}
