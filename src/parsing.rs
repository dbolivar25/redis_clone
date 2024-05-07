use anyhow::{anyhow, Result};
use itertools::{Either, Itertools};
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

pub(crate) fn parse_command(input: &[u8]) -> Result<Command> {
    let (_, array) = parse_array(input).map_err(|_| anyhow!("Failed to parse array"))?;
    let mut args = match array {
        Value::Array(args) => args.into_iter(),
        _ => return Err(anyhow!("Invalid command format: expected an Array")),
    };

    let command_value = args.next().ok_or_else(|| anyhow!("Empty command"))?;
    let command = match command_value {
        Value::BulkString(command) => command,
        _ => return Err(anyhow!("Invalid command type: expected a BulkString")),
    };

    match command.to_lowercase().as_str() {
        "ping" => {
            if args.next().is_some() {
                Err(anyhow!("PING command does not take any arguments"))
            } else {
                Ok(Command::Ping)
            }
        }
        "echo" => {
            let value = args
                .next()
                .ok_or_else(|| anyhow!("Missing value for ECHO command"))?;
            if args.next().is_some() {
                Err(anyhow!("ECHO command takes exactly one argument"))
            } else {
                Ok(Command::Echo(value))
            }
        }
        "get" => {
            let key = args
                .next()
                .ok_or_else(|| anyhow!("Missing key for GET command"))?;
            if args.next().is_some() {
                Err(anyhow!("GET command takes exactly one argument"))
            } else {
                Ok(Command::Get(key))
            }
        }
        "set" => {
            let key = args
                .next()
                .ok_or_else(|| anyhow!("Missing key for SET command"))?;

            let value = args
                .next()
                .ok_or_else(|| anyhow!("Missing value for SET command"))?;

            match args.next() {
                Some(Value::BulkString(s)) => match s.to_lowercase().as_str() {
                    "ex" => {
                        let expiration = match args.next() {
                            Some(Value::BulkString(expiration)) => {
                                Some(expiration.parse::<u128>()? * 1000)
                            }
                            Some(_) => {
                                return Err(anyhow!(
                                    "Invalid expiration type: expected a BulkString"
                                ))
                            }
                            None => None,
                        };

                        if args.next().is_some() {
                            Err(anyhow!("SET command takes two or three arguments"))
                        } else {
                            Ok(Command::Set(key, value, expiration))
                        }
                    }
                    "px" => {
                        let expiration = match args.next() {
                            Some(Value::BulkString(expiration)) => {
                                Some(expiration.parse::<u128>()?)
                            }
                            Some(_) => {
                                return Err(anyhow!(
                                    "Invalid expiration type: expected a BulkString"
                                ))
                            }
                            None => None,
                        };

                        if args.next().is_some() {
                            Err(anyhow!("SET command takes two or three arguments"))
                        } else {
                            Ok(Command::Set(key, value, expiration))
                        }
                    }
                    _ => return Err(anyhow!("Invalid SET option")),
                },
                Some(_) => return Err(anyhow!("Invalid SET option")),
                None => Ok(Command::Set(key, value, None)),
            }
        }
        "info" => {
            if let Some(of_type) = args.next() {
                if args.next().is_some() {
                    Err(anyhow!("INFO command takes zero or one argument"))
                } else {
                    Ok(Command::Info(of_type))
                }
            } else {
                Ok(Command::Info(Value::BulkString("default".to_string())))
            }
        }
        "replconf" => {
            let (oks, errs): (Vec<_>, Vec<_>) = args.tuples().partition_map(|pair| {
                if let (Value::BulkString(_), Value::BulkString(_)) = pair {
                    Either::Left(pair)
                } else {
                    Either::Right(())
                }
            });

            if !errs.is_empty() {
                return Err(anyhow!("Invalid REPLCONF option"));
            }

            Ok(Command::ReplConf(oks))
        }
        "psync" => {
            let replid = args
                .next()
                .ok_or_else(|| anyhow!("Missing REPLID for PSYNC command"))?;

            let offset = args
                .next()
                .ok_or_else(|| anyhow!("Missing OFFSET for PSYNC command"))?;

            if args.next().is_some() {
                Err(anyhow!("PSYNC command takes exactly two arguments"))
            } else {
                Ok(Command::Psync(replid, offset))
            }
        }
        _ => Err(anyhow!("Unknown command: {}", command)),
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
