use itertools::Itertools;
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::digit1,
    combinator::{map, opt},
    multi::count,
    sequence::{pair, preceded, terminated},
    IResult,
};

use crate::types::{Command, Value};

pub(crate) fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    let (remaining, val) = parse_array(input)?;

    let (command, args) = match val {
        Value::Array(array) => match array.as_slice() {
            [Value::BulkString(command), args @ ..] => {
                (command.to_lowercase().to_owned(), args.to_owned())
            }
            _ => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::IsNot,
                )))
            }
        },
        _ => unreachable!(),
    };

    match (command.as_str(), args.as_slice()) {
        ("ping", []) => Ok((remaining, Command::Ping)),
        ("echo", [contents]) => Ok((remaining, Command::Echo(contents.to_owned()))),
        ("get", [key]) => Ok((remaining, Command::Get(key.to_owned()))),
        ("set", [key, value]) => Ok((
            remaining,
            Command::Set(key.to_owned(), value.to_owned(), None),
        )),
        ("set", [key, value, Value::BulkString(expr_flag), Value::BulkString(expr_val)])
            if expr_flag.eq_ignore_ascii_case("ex") =>
        {
            let parsed_expr_val = expr_val.parse::<u128>().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::IsNot))
            })?;

            Ok((
                remaining,
                Command::Set(
                    key.to_owned(),
                    value.to_owned(),
                    Some(parsed_expr_val * 1000),
                ),
            ))
        }
        ("set", [key, value, Value::BulkString(expr_flag), Value::BulkString(expr_val)])
            if expr_flag.eq_ignore_ascii_case("px") =>
        {
            let parsed_expr_val = expr_val.parse::<u128>().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::IsNot))
            })?;

            Ok((
                remaining,
                Command::Set(key.to_owned(), value.to_owned(), Some(parsed_expr_val)),
            ))
        }
        ("info", [of_type]) => Ok((remaining, Command::Info(of_type.to_owned()))),
        ("replconf", pairs) => {
            if pairs.len() % 2 != 0 {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::IsNot,
                )));
            }

            if pairs.iter().any(|val| !matches!(val, Value::BulkString(_))) {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::IsNot,
                )));
            }

            let pairs = pairs
                .into_iter()
                .tuples()
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
                .collect_vec();

            Ok((remaining, Command::ReplConf(pairs)))
        }
        ("psync", [replid, offset]) => Ok((
            remaining,
            Command::Psync(replid.to_owned(), offset.to_owned()),
        )),
        _ => Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::IsNot,
        ))),
    }
}

pub(crate) fn parse_value(input: &[u8]) -> IResult<&[u8], Value> {
    alt((
        parse_simple_string,
        parse_integer,
        parse_bulk_string,
        parse_array,
        parse_rdb_file,
    ))(input)
}

pub(crate) fn parse_simple_string(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("+"), parse_simple_string_impl)(input)
}

pub(crate) fn parse_integer(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag(":"), parse_integer_impl)(input)
}

pub(crate) fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("$"), parse_bulk_string_impl)(input)
}

pub(crate) fn parse_array(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("*"), parse_array_impl)(input)
}

pub(crate) fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("$"), parse_rdb_file_impl)(input)
}

fn parse_simple_string_impl(input: &[u8]) -> IResult<&[u8], Value> {
    map(terminated(take_until("\r\n"), parse_crlf), |s| {
        Value::SimpleString(String::from_utf8_lossy(s).to_string())
    })(input)
}

fn parse_integer_impl(input: &[u8]) -> IResult<&[u8], Value> {
    map(
        terminated(pair(opt(alt((tag("+"), tag("-")))), digit1), parse_crlf),
        |(sign, val)| {
            let val = String::from_utf8_lossy(val).parse::<i64>().unwrap();
            match sign {
                Some(b"-") => Value::Integer(-val),
                _ => Value::Integer(val),
            }
        },
    )(input)
}

fn parse_bulk_string_impl(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, val) = parse_integer_impl(input)?;

    let len = match val {
        Value::Integer(-1) => return Ok((input, Value::Null)),
        Value::Integer(len) if len >= 0 => len,
        _ => {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )))
        }
    };

    let (input, data) = map(terminated(take(len as usize), parse_crlf), |s: &[u8]| {
        String::from_utf8_lossy(s).to_string()
    })(input)?;

    Ok((input, Value::BulkString(data)))
}

fn parse_array_impl(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, val) = parse_integer_impl(input)?;

    let len = match val {
        Value::Integer(-1) => return Ok((input, Value::Null)),
        Value::Integer(len) if len >= 0 => len,
        _ => {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )))
        }
    };

    let (input, values) = count(parse_value, len as usize)(input)?;

    Ok((input, Value::Array(values)))
}

fn parse_rdb_file_impl(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, val) = parse_integer_impl(input)?;

    let len = match val {
        Value::Integer(len) if len >= 0 => len,
        _ => {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )))
        }
    };

    let (input, values) = take(len as usize)(input)?;

    Ok((input, Value::RdbFile(values.to_vec())))
}

fn parse_crlf(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("\r\n")(input)
}
