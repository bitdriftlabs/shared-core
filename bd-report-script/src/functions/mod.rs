mod add_field;
mod set_grouping_key;
mod set_significant_frame;

use crate::functions::add_field::AddField;
use crate::functions::set_grouping_key::SetGroupingKey;
use crate::functions::set_significant_frame::SetSignificantFrame;
use vrl::prelude::Function;


pub fn all() -> Vec<Box<dyn Function>> {
  vec![
    Box::new(AddField),
    Box::new(SetGroupingKey),
    Box::new(SetSignificantFrame),
  ]
}
