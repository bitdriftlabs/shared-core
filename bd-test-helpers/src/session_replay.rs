//
// NoOpTarget
//

pub struct NoOpTarget;

impl bd_session_replay::Target for NoOpTarget {
  fn capture_wireframes(&self) {}
  fn take_screenshot(&self) {}
}
