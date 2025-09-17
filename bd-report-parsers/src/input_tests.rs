// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::input::MemmapView;
use crate::make_tempfile;
use memmap2::Mmap;
use nom::{Compare, FindSubstring, Input, Offset};

#[test]
fn wrapper_new_test() -> anyhow::Result<()> {
  let contents = b"0123456789";
  let file = make_tempfile(contents)?;
  let mmap = unsafe { Mmap::map(&file)? };
  let wrapper = MemmapView::new(&mmap);

  assert_eq!(contents.len(), wrapper.len());
  assert!(!wrapper.is_empty());
  assert_eq!(Some(b'0'), wrapper.peek());
  assert_eq!(Some(5), wrapper.find('5'));
  assert_eq!(String::from_utf8(Vec::from(contents))?, wrapper.to_string());
  assert_eq!(Some(0), wrapper.find_substring("01"));
  assert_eq!(Some(2), wrapper.find_substring("234"));
  assert_eq!(Some(6), wrapper.find_substring("6789"));
  assert_eq!(Some(9), wrapper.find_substring("9"));
  assert_eq!(None, wrapper.find_substring("14"));
  assert_eq!(None, wrapper.position(|c| c == &b'a'));
  assert_eq!(Some(3), wrapper.position(|c| c == &b'3'));
  assert_eq!(Some(9), wrapper.position(|c| c == &b'9'));
  assert_eq!(Some(0), wrapper.position(|c| c > &b'\0'));
  assert_eq!(
    contents.to_vec(),
    wrapper.iter_elements().copied().collect::<Vec<u8>>(),
  );
  Ok(())
}

#[test]
fn wrapper_take_test() -> anyhow::Result<()> {
  let contents = b"0123456789";
  let file = make_tempfile(contents)?;
  let mmap = unsafe { Mmap::map(&file)? };
  let wrapper = MemmapView::new(&mmap);

  let sub_left = wrapper.take(5);
  assert_eq!(5, sub_left.len());
  assert_eq!(Some(b'0'), sub_left.peek());
  assert_eq!(Some(4), sub_left.find('4'));
  assert_eq!(0, wrapper.offset(&sub_left));
  assert_eq!(Some(1), sub_left.find_substring("12"));
  assert_eq!(Some(3), sub_left.position(|c| c == &b'3'));
  assert_eq!(Some(4), sub_left.position(|c| c == &b'4'));
  assert_eq!(None, sub_left.position(|c| c == &b'5'));
  assert_eq!(
    String::from_utf8(contents[.. 5].to_vec())?,
    sub_left.to_string()
  );
  assert_eq!(
    contents[.. 5].to_vec(),
    sub_left.iter_elements().copied().collect::<Vec<u8>>(),
  );

  let sub_right = sub_left.take_from(2);
  assert_eq!(3, sub_right.len());
  assert_eq!(Some(b'2'), sub_right.peek());
  assert_eq!(Some(2), sub_right.find('4'));
  assert_eq!(2, wrapper.offset(&sub_right));
  assert_eq!(Some(1), sub_right.find_substring("3"));
  assert_eq!(Some(1), sub_right.position(|c| c == &b'3'));
  assert_eq!(Some(2), sub_right.position(|c| c == &b'4'));
  assert_eq!(None, sub_right.position(|c| c == &b'9'));
  assert_eq!(
    String::from_utf8(contents[2 .. 5].to_vec())?,
    sub_right.to_string()
  );
  assert_eq!(
    contents[2 .. 5].to_vec(),
    sub_right.iter_elements().copied().collect::<Vec<u8>>(),
  );
  Ok(())
}

#[test]
fn wrapper_take_split_test() -> anyhow::Result<()> {
  let contents = b"0123456789";
  let file = make_tempfile(contents)?;
  let mmap = unsafe { Mmap::map(&file)? };
  let wrapper = MemmapView::new(&mmap);

  let (sub_left, sub_right) = wrapper.take_split(5);
  assert_eq!(5, sub_left.len());
  assert_eq!(Some(b'0'), sub_left.peek());
  assert_eq!(Some(4), sub_left.find('4'));
  assert_eq!(0, wrapper.offset(&sub_left));
  assert_eq!(
    String::from_utf8(contents[.. 5].to_vec())?,
    sub_left.to_string()
  );
  assert_eq!(
    contents[.. 5].to_vec(),
    sub_left.iter_elements().copied().collect::<Vec<u8>>(),
  );
  assert_eq!(
    contents[.. 5]
      .iter()
      .enumerate()
      .collect::<Vec<(usize, &u8)>>(),
    sub_left.iter_indices().collect::<Vec<(usize, &u8)>>(),
  );
  assert_eq!(5, sub_right.len());
  assert_eq!(Some(b'5'), sub_right.peek());
  assert_eq!(Some(1), sub_right.find('6'));
  assert_eq!(5, wrapper.offset(&sub_right));
  assert_eq!(
    String::from_utf8(contents[5 ..].to_vec())?,
    sub_right.to_string()
  );
  assert_eq!(
    contents[5 ..].to_vec(),
    sub_right.iter_elements().copied().collect::<Vec<u8>>(),
  );
  assert_eq!(
    contents[5 ..]
      .iter()
      .enumerate()
      .collect::<Vec<(usize, &u8)>>(),
    sub_right.iter_indices().collect::<Vec<(usize, &u8)>>(),
  );
  Ok(())
}

#[test]
fn wrapper_compare_test() -> anyhow::Result<()> {
  let contents = b"Beets and cheese";
  let file = make_tempfile(contents)?;
  let mmap = unsafe { Mmap::map(&file)? };
  let wrapper = MemmapView::new(&mmap);

  assert_eq!(nom::CompareResult::Ok, wrapper.compare("Beets and cheese"));
  assert_eq!(nom::CompareResult::Ok, wrapper.compare("Beets"));
  assert_eq!(
    nom::CompareResult::Error,
    wrapper.compare("Beets and sleaze")
  );
  assert_eq!(
    nom::CompareResult::Ok,
    wrapper.compare_no_case("Beets and cheese")
  );
  assert_eq!(
    nom::CompareResult::Ok,
    wrapper.compare_no_case("beets and cheese")
  );
  assert_eq!(nom::CompareResult::Ok, wrapper.compare_no_case("beets"));
  assert_eq!(nom::CompareResult::Error, wrapper.compare_no_case("beans"));
  Ok(())
}
