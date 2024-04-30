use anyhow::{anyhow, Result};
use nom::{
    branch::alt,
    bytes::complete::{tag, take},
    character::complete::{alphanumeric0, digit1},
    combinator::{map, opt},
    multi::count,
    sequence::{pair, preceded, terminated},
    IResult,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug)]
enum Command {
    Ping,
    Echo(Value),
}

#[derive(Debug)]
enum Value {
    SimpleString(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    Null,
}

fn encode_value(val: &Value) -> String {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s),
        Value::Integer(i) => format!(":{}\r\n", i),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
        Value::Array(vals) => {
            let head = format!("*{}\r\n", vals.len());
            let body = vals.iter().map(encode_value).collect::<String>();

            head + &body
        }
        Value::Null => "_\r\n".to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    loop {
        let n = match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("Connection closed");
                return;
            }
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to read from socket: {}", e);
                return;
            }
        };

        let received_data = &buffer[..n];
        println!("Received: {:?}", String::from_utf8_lossy(received_data));

        let command = match parse_command(received_data) {
            Ok(command) => command,
            Err(e) => {
                eprintln!("Failed to parse command: {}", e);
                continue;
            }
        };

        let response = match command {
            Command::Ping => Value::SimpleString("PONG".to_string()),
            Command::Echo(val) => val,
        };

        let encoded_response = encode_value(&response);

        if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
            eprintln!("Failed to write to socket: {}", e);
            return;
        }

        println!("Sent: {}", encoded_response);
    }
}

fn parse_command(input: &[u8]) -> Result<Command> {
    let (_, array) = parse_array(input).map_err(|_| anyhow!("Failed to parse command"))?;
    let mut args = match array {
        Value::Array(args) => args.into_iter(),
        _ => unreachable!(),
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
        _ => Err(anyhow!("Unknown command: {}", command)),
    }
}

fn parse_value(input: &[u8]) -> IResult<&[u8], Value> {
    alt((
        parse_simple_string,
        parse_integer,
        parse_bulk_string,
        parse_array,
    ))(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("+"), parse_simple_string_impl)(input)
}

fn parse_integer(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag(":"), parse_integer_impl)(input)
}

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("$"), parse_bulk_string_impl)(input)
}

fn parse_array(input: &[u8]) -> IResult<&[u8], Value> {
    preceded(tag("*"), parse_array_impl)(input)
}

fn parse_simple_string_impl(input: &[u8]) -> IResult<&[u8], Value> {
    map(terminated(alphanumeric0, parse_crlf), |s| {
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

fn parse_crlf(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag("\r\n")(input)
}
