pub mod de;
#[cfg(test)]
pub mod ser;
pub mod type_codes;

mod primitives;
mod writer;

pub fn add(left: u64, right: u64) -> u64 {
  left + right
}

pub enum Error {
  InvalidSerialization,
  InvalidDeserialization,
  Io { offset: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    let result = add(2, 2);
    assert_eq!(result, 4);
  }
}
