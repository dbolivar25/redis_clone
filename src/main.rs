use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

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
    select,
    sync::mpsc::{Receiver, Sender},
    time::interval,
};

#[derive(Debug)]
enum Command {
    Ping,
    Echo(Value),
    Get(Value),
    Set(Value, Value, Option<u128>),
}

#[derive(Debug)]
struct Message {
    command: Command,
    response_sender: Option<Sender<Value>>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
enum Value {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    NullBulkString,
    NullArray,
    Null,
}

#[derive(Debug)]
struct DataActor {
    data: HashMap<Value, (Option<u128>, Value)>,
    expiration: BTreeMap<u128, Value>,
    msg_receiver: Receiver<Message>,
}

impl DataActor {
    fn new() -> (Self, Sender<Message>) {
        let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(256);

        (
            DataActor {
                data: HashMap::new(),
                expiration: BTreeMap::new(),
                msg_receiver,
            },
            msg_sender,
        )
    }

    async fn run(mut self) {
        let mut gc_interval = interval(Duration::from_secs(2));

        loop {
            select! {
                _ = gc_interval.tick() => {
                    self.handle_expired_keys();
                }
                Some(message) = self.msg_receiver.recv() => {
                    let response = match message.command {
                        Command::Ping => self.handle_ping(),
                        Command::Echo(value) => self.handle_echo(value),
                        Command::Get(key) => self.handle_get(key),
                        Command::Set(key, value, expiration) => self.handle_set(key, expiration, value),
                    };

                    if let Some(response_sender) = message.response_sender {
                        if let Err(e) = response_sender.send(response).await {
                            eprintln!("Failed to send response: {}", e);
                        }
                    }
                }
            }
        }
    }

    fn handle_ping(&self) -> Value {
        Value::SimpleString("PONG".to_string())
    }

    fn handle_echo(&self, value: Value) -> Value {
        value
    }

    fn handle_get(&mut self, key: Value) -> Value {
        let (expiration, value) = match self.data.get(&key) {
            Some(pair) => pair,
            None => return Value::NullBulkString,
        };

        if let &Some(expiration) = expiration {
            let now = chrono::Utc::now().timestamp_millis() as u128;
            if expiration <= now {
                self.data.remove(&key);
                self.expiration.remove(&expiration);

                return Value::NullBulkString;
            }
        }

        value.clone()
    }

    fn handle_set(&mut self, key: Value, expiration: Option<u128>, value: Value) -> Value {
        let expiration_time = expiration.map(|val| {
            let now = chrono::Utc::now().timestamp_millis() as u128;
            now + val
        });

        self.data.insert(key.clone(), (expiration_time, value));

        if let Some(expiration_time) = expiration_time {
            self.expiration.insert(expiration_time, key);
        }

        Value::SimpleString("OK".to_string())
    }

    fn handle_expired_keys(&mut self) {
        let now = chrono::Utc::now().timestamp_millis() as u128;
        let expired = self
            .expiration
            .range(..=now)
            .map(|(expr, key)| (*expr, key.to_owned()))
            .collect::<Vec<_>>();

        expired.iter().for_each(|(expr, key)| {
            self.expiration.remove(expr);
            self.data.remove(&key);
        });
    }
}

struct MsgSender {
    msg_sender: Sender<Message>,
}

impl MsgSender {
    async fn handle_command(&self, command: Command) -> Result<Value> {
        let (response_sender, mut response_receiver) = tokio::sync::mpsc::channel(1);

        let message = Message {
            command,
            response_sender: Some(response_sender),
        };

        self.msg_sender.send(message).await?;
        response_receiver
            .recv()
            .await
            .ok_or_else(|| anyhow!("Failed to receive response"))
    }
}

struct Context {
    stream: TcpStream,
    msg_sender: MsgSender,
}

impl Context {
    fn new(stream: TcpStream, msg_sender: Sender<Message>) -> Self {
        let msg_sender = MsgSender { msg_sender };
        Context { stream, msg_sender }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on {}", listener.local_addr()?);

    let (data_actor, msg_sender) = DataActor::new();

    tokio::spawn(data_actor.run());

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("\nAccepted connection from {}", addr);

        let context = Context::new(stream, msg_sender.clone());
        tokio::spawn(handle_connection(context));
    }
}

async fn handle_connection(context: Context) {
    let mut buffer = [0; 1024];
    let Context {
        mut stream,
        msg_sender,
    } = context;

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

                let response = Value::SimpleError(e.to_string());
                let encoded_response = encode_value(&response);

                if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    return;
                }

                println!("Sent: {:?}", encoded_response);

                continue;
            }
        };

        let response = match msg_sender.handle_command(command).await {
            Ok(response) => response,
            Err(e) => {
                eprintln!("Failed to send command: {}", e);

                let response = Value::SimpleError(e.to_string());
                let encoded_response = encode_value(&response);

                if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    return;
                }

                println!("Sent: {:?}", encoded_response);

                continue;
            }
        };

        let encoded_response = encode_value(&response);

        if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
            eprintln!("Failed to write to socket: {}", e);
            return;
        }

        println!("Sent: {:?}", encoded_response);
    }
}

fn parse_command(input: &[u8]) -> Result<Command> {
    let (_, array) = parse_array(input).map_err(|_| anyhow!("Failed to parse array"))?;
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
                            Some(_) => return Err(anyhow!("Invalid expiration time")),
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
                            Some(_) => return Err(anyhow!("Invalid expiration time")),
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
                None => {
                    if args.next().is_some() {
                        Err(anyhow!("SET command takes two or three arguments"))
                    } else {
                        Ok(Command::Set(key, value, None))
                    }
                }
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

fn encode_value(val: &Value) -> String {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s),
        Value::SimpleError(s) => format!("-{}\r\n", s),
        Value::Integer(i) => format!(":{}\r\n", i),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
        Value::Array(vals) => {
            let head = format!("*{}\r\n", vals.len());
            let body = vals.iter().map(encode_value).collect::<String>();

            head + &body
        }
        Value::NullBulkString => "$-1\r\n".to_string(),
        Value::NullArray => "*-1\r\n".to_string(),
        Value::Null => "_\r\n".to_string(),
    }
}
