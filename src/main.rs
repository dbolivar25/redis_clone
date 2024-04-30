use anyhow::Result;
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
            let mut head = format!("*{}\r\n", vals.len());
            let body = vals.iter().map(encode_value);
            head.extend(body);

            head
        }
        Value::Null => "_\r\n".to_string(),
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted connection: {}", addr);

                tokio::spawn(handler(stream));
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn handler(mut stream: TcpStream) {
    let mut buf = [0; 1024];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                println!("Connection closed");
                return;
            }
            Ok(n) => {
                println!("Received: {:?}", String::from_utf8_lossy(&buf[..n]));

                match parse_command(&buf[..n]) {
                    Ok(cmd) => {
                        println!("Parsed command: {:?}", cmd);

                        let resp = match cmd {
                            Command::Ping => Value::SimpleString("PONG".to_string()),
                            Command::Echo(val) => val,
                        };

                        println!("Response: {:?}", resp);

                        let encoded = encode_value(&resp);

                        println!("Sending response: {:?}", encoded);

                        if let Err(e) = stream.write_all(encoded.as_bytes()).await {
                            panic!("Error writing to stream: {}", e);
                        }

                        println!("Sent response: {:?}", encoded);
                    }
                    Err(e) => {
                        println!("Error parsing command: {}", e);
                    }
                }
            }
            Err(e) => {
                panic!("Error reading from stream: {}", e);
            }
        }
    }
}

fn parse_command(input: &[u8]) -> Result<Command> {
    let (_, vals) = parse_value(input).map_err(|e| anyhow::anyhow!("Error parsing: {:?}", e))?;

    let mut iter = match vals {
        Value::Array(vals) => vals.into_iter(),
        _ => return Err(anyhow::anyhow!("Invalid command type")),
    };

    let cmd = iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("Empty command"))?;

    match cmd {
        Value::BulkString(s) => match s.to_lowercase().as_str() {
            "ping" => {
                if iter.next().is_some() {
                    return Err(anyhow::anyhow!("Too many arguments"));
                }

                Ok(Command::Ping)
            }
            "echo" => {
                let val = iter
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing value"))?;

                if iter.next().is_some() {
                    return Err(anyhow::anyhow!("Too many arguments"));
                }

                Ok(Command::Echo(val))
            }
            _ => Err(anyhow::anyhow!("Unknown command: {}", s)),
        },
        _ => Err(anyhow::anyhow!("Invalid command type")),
    }
}

fn parse_value(input: &[u8]) -> IResult<&[u8], Value> {
    alt((
        preceded(tag("+"), parse_simple_string),
        preceded(tag(":"), parse_integer),
        preceded(tag("$"), parse_bulk_string),
        preceded(tag("*"), parse_array),
    ))(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], Value> {
    map(terminated(alphanumeric0, parse_crlf), |s| {
        Value::SimpleString(String::from_utf8_lossy(s).to_string())
    })(input)
}

fn parse_integer(input: &[u8]) -> IResult<&[u8], Value> {
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

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, val) = parse_integer(input)?;

    let len = match val {
        Value::Integer(-1) => return Ok((input, Value::Null)),
        Value::Integer(len) => len,
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

fn parse_array(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, val) = parse_integer(input)?;

    let len = match val {
        Value::Integer(-1) => return Ok((input, Value::Null)),
        Value::Integer(len) => len,
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
