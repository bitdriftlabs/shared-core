// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use arbitrary::{Arbitrary, Unstructured};
use bd_bonjson::Value;
use std::collections::HashMap;

// Custom implementation of Arbitrary for Value
impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let variant: u8 = u.arbitrary()?;
        Ok(match variant % 8 {
            0 => Value::Null,
            1 => Value::Bool(u.arbitrary()?),
            2 => Value::Float(u.arbitrary()?),
            3 => Value::Signed(u.arbitrary()?),
            4 => Value::Unsigned(u.arbitrary()?),
            5 => Value::String(u.arbitrary()?),
            6 => {
                let len = u.int_in_range(0..=5)?; // Keep arrays small
                let mut arr = Vec::new();
                for _ in 0..len {
                    arr.push(Value::arbitrary(u)?);
                }
                Value::Array(arr)
            }
            7 => {
                let len = u.int_in_range(0..=3)?; // Keep objects small
                let mut obj = HashMap::new();
                for _ in 0..len {
                    let key: String = u.arbitrary()?;
                    let value = Value::arbitrary(u)?;
                    obj.insert(key, value);
                }
                Value::Object(obj)
            }
            _ => unreachable!(),
        })
    }
}
