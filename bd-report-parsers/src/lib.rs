use nom::bytes::complete::{take_till, take_while};
use nom::bytes::tag;
use nom::{IResult, Parser};

pub struct Source {
  pub path: String,
  pub lineno: Option<i64>,
}

pub fn source_location(input: &str) -> IResult<&str, Source> {
  let mut file_parser = take_till(|c| c == ':');
  let (remainder, path) = file_parser.parse(input)?;
  if remainder.is_empty() {
    Ok((
      remainder,
      Source {
        path: path.to_owned(),
        lineno: None,
      },
    ))
  } else {
    let lineno_parser = take_while(|c: char| c.is_ascii_digit());
    let (bits, (_, num)) = (tag(":"), lineno_parser).parse(remainder)?;
    Ok((
      bits,
      Source {
        path: path.to_owned(),
        lineno: num.parse::<i64>().ok(),
      },
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn source_location_test() {
    let Ok((remainder, source)) = source_location("File:64") else {
      panic!("failed to parse");
    };
    assert_eq!("", remainder);
    assert_eq!("File".to_owned(), source.path);
    assert_eq!(Some(64), source.lineno);
  }

  #[test]
  fn source_location_no_lineno_test() {
    let Ok((remainder, source)) = source_location("generated-source") else {
      panic!("failed to parse");
    };
    assert_eq!("", remainder);
    assert_eq!("generated-source".to_owned(), source.path);
    assert_eq!(None, source.lineno);
  }
}
