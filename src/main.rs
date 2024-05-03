use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::{Display, Formatter},
    io::Read,
    time::Duration,
};

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

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};

use chrono::Utc;

// clap interface
use clap::Parser;

/// Redis server
#[derive(Parser, Debug)]
#[clap(version, author = "Daniel Bolivar")]
struct Args {
    /// Port to listen on
    #[clap(short, long, default_value = "6379")]
    port: u16,

    /// Host and port of the master server
    #[clap(short, long, num_args = 2)]
    replicaof: Vec<String>,
}

#[derive(Debug)]
enum ServerType {
    Master(u32, String, u128),
    Slave(TcpStream),
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(Value),
    Get(Value),
    Set(Value, Value, Option<u128>),
    Info(Value),
    ReplConf(Vec<(Value, Value)>),
    Psync(Value, Value),
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
    #[allow(dead_code)]
    NullArray,
    RdbFile(Vec<u8>),
    Null,
}

#[derive(Debug)]
struct CommandHandler {
    listen_port: u16,
    server_type: ServerType,
    data: HashMap<Value, (Option<u128>, Value)>,
    expiration: BTreeMap<u128, Value>,
    msg_receiver: Receiver<Message>,
}

struct MsgSender {
    msg_sender: Sender<Message>,
}

struct Context {
    stream: TcpStream,
    msg_sender: MsgSender,
}

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bind_addr = format!("127.0.0.1:{}", args.port);

    let listener = TcpListener::bind(bind_addr.clone()).await?;
    println!("Listening on {}", listener.local_addr()?);

    let server_type = match args.replicaof.as_slice() {
        [] => ServerType::Master(0, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), 0),
        [host, port] => {
            let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
            ServerType::Slave(stream)
        }
        _ => unreachable!(),
    };

    let (command_handler, msg_sender) = CommandHandler::new(args.port, server_type);

    tokio::spawn(command_handler.run());

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("\nAccepted connection from {}", addr);

        let context = Context::new(stream, msg_sender.clone());
        tokio::spawn(handle_connection(context));
    }
}

impl Display for ServerType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ServerType::Master(_count, _replid, _repl_offset) => write!(f, "master"),
            ServerType::Slave(_master_strm) => write!(f, "slave"),
        }
    }
}

impl CommandHandler {
    fn new(listen_port: u16, server_type: ServerType) -> (Self, Sender<Message>) {
        let (msg_sender, msg_receiver) = mpsc::channel(256);

        (
            CommandHandler {
                listen_port,
                server_type,
                data: HashMap::new(),
                expiration: BTreeMap::new(),
                msg_receiver,
            },
            msg_sender,
        )
    }

    async fn run(mut self) {
        let mut gc_interval = interval(Duration::from_secs(1));

        if let ServerType::Slave(ref mut stream) = self.server_type {
            let mut buf = [0; 1024];

            let encoded_response =
                encode_value(&Value::Array(vec![Value::BulkString("PING".to_string())]));

            if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            let n = match stream.read(&mut buf).await {
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

            let received_data = &buf[..n];
            println!("Received: {:?}", String::from_utf8_lossy(received_data));

            let encoded_response = encode_value(&Value::Array(vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("listening-port".to_string()),
                Value::BulkString(format!("{}", self.listen_port)),
            ]));

            if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            let n = match stream.read(&mut buf).await {
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

            let received_data = &buf[..n];
            println!("Received: {:?}", String::from_utf8_lossy(received_data));

            let encoded_response = encode_value(&Value::Array(vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("capa".to_string()),
                Value::BulkString("eof".to_string()),
                Value::BulkString("capa".to_string()),
                Value::BulkString("psync2".to_string()),
            ]));

            if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            let n = match stream.read(&mut buf).await {
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

            let received_data = &buf[..n];
            println!("Received: {:?}", String::from_utf8_lossy(received_data));

            let encoded_response = encode_value(&Value::Array(vec![
                Value::BulkString("PSYNC".to_string()),
                Value::BulkString("?".to_string()),
                Value::BulkString("-1".to_string()),
            ]));

            if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            let n = match stream.read(&mut buf).await {
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

            let received_data = &buf[..n];
            println!("Received: {:?}", String::from_utf8_lossy(received_data));

            let (_remaining, response) = match parse_simple_string(received_data) {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("Failed to parse response: {}", e);
                    return;
                }
            };

            match response {
                Value::SimpleString(s) => {
                    let parts = s.split_whitespace().collect::<Vec<_>>();
                    match parts.as_slice() {
                        [msg] if msg.to_lowercase().as_str() == "continue" => {}
                        [msg, _replid, _offset] if msg.to_lowercase().as_str() == "fullresync" => {
                            let n = match stream.read(&mut buf).await {
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

                            let received = &buf[..n];
                            println!("Received: {:?}", String::from_utf8_lossy(received));

                            let (_remaining, response) = match parse_rdb_file(received) {
                                Ok(res) => res,
                                Err(e) => {
                                    eprintln!("Failed to parse response: {}", e);
                                    return;
                                }
                            };

                            println!("RDB file: {:?}", response);
                        }
                        _ => {
                            eprintln!("Invalid response: {}", s);
                            return;
                        }
                    }
                }
                _ => {
                    eprintln!("Invalid response type");
                    return;
                }
            }
        }

        loop {
            select! {
                _ = gc_interval.tick() => {
                    self.handle_expired_keys();
                }
                Some(message) = self.msg_receiver.recv() => {
                    let responses = match message.command {
                        Command::Ping => self.handle_ping(),
                        Command::Echo(value) => self.handle_echo(value),
                        Command::Get(key) => self.handle_get(key),
                        Command::Set(key, value, expiration) => self.handle_set(key, value, expiration),
                        Command::Info(of_type) => self.handle_info(of_type),
                        Command::ReplConf(_pairs) => vec![Value::SimpleString("OK".to_string())],
                        Command::Psync(replid, offset) => self.handle_psync(replid, offset),
                    };

                    if let Some(response_sender) = message.response_sender {
                        for response in responses {
                            if let Err(e) = response_sender.send(response).await {
                                eprintln!("Failed to send response: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    fn handle_ping(&self) -> Vec<Value> {
        vec![Value::SimpleString("PONG".to_string())]
    }

    fn handle_echo(&self, value: Value) -> Vec<Value> {
        vec![value]
    }

    fn handle_get(&mut self, key: Value) -> Vec<Value> {
        let (expiration, value) = match self.data.get(&key) {
            Some(pair) => pair,
            None => return vec![Value::NullBulkString],
        };

        if let &Some(expiration) = expiration {
            let now = Utc::now().timestamp_millis() as u128;
            if expiration <= now {
                self.data.remove(&key);
                self.expiration.remove(&expiration);

                return vec![Value::NullBulkString];
            }
        }

        vec![value.clone()]
    }

    fn handle_set(&mut self, key: Value, value: Value, expiration: Option<u128>) -> Vec<Value> {
        let expiration_time = expiration.map(|val| {
            let now = Utc::now().timestamp_millis() as u128;
            now + val
        });

        self.data.insert(key.clone(), (expiration_time, value));

        if let Some(expiration_time) = expiration_time {
            self.expiration.insert(expiration_time, key);
        }

        vec![Value::SimpleString("OK".to_string())]
    }

    fn handle_info(&self, of_type: Value) -> Vec<Value> {
        match of_type {
            Value::BulkString(s) => {
                let info = match s.to_lowercase().as_str() {
                    "replication" => {
                        let role = match &self.server_type {
                            ServerType::Master(_, _, _) => "master",
                            ServerType::Slave(_) => "slave",
                        };

                        let (replid, repl_offset) = match &self.server_type {
                            ServerType::Master(_, replid, repl_offset) => {
                                (replid.as_str(), repl_offset)
                            }
                            ServerType::Slave(_) => ("0", &0),
                        };

                        vec![
                            format!("role:{}", role),
                            format!("master_replid:{}", replid),
                            format!("master_repl_offset:{}", repl_offset),
                        ]
                    }

                    _ => vec!["DEFAULT INFO".to_string()],
                };

                vec![Value::BulkString(info.join("\n"))]
            }
            _ => vec![Value::SimpleError("Invalid INFO type".to_string())],
        }
    }

    fn handle_psync(&self, replid: Value, offset: Value) -> Vec<Value> {
        let replid = match replid {
            Value::BulkString(s) => s,
            _ => return vec![Value::SimpleError("Invalid REPLID type".to_string())],
        };

        let offset = match offset {
            Value::BulkString(s) => s,
            _ => return vec![Value::SimpleError("Invalid OFFSET type".to_string())],
        };

        match &self.server_type {
            ServerType::Master(_, replid_str, repl_offset) => {
                if &replid == replid_str && offset == repl_offset.to_string() {
                    vec![Value::SimpleString("CONTINUE".to_string())]
                } else {
                    let file = hex::decode(EMPTY_RDB).unwrap();
                    vec![
                        Value::SimpleString(format!("FULLRESYNC {} {}", replid_str, repl_offset)),
                        Value::RdbFile(file),
                    ]
                }
            }
            ServerType::Slave(_) => vec![Value::SimpleString("CONTINUE".to_string())],
        }
    }

    fn handle_expired_keys(&mut self) {
        let now = Utc::now().timestamp_millis() as u128;
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

impl MsgSender {
    async fn send_command(&self, command: Command) -> Result<Vec<Value>> {
        let (response_sender, mut response_receiver) = mpsc::channel(2);

        let message = Message {
            command,
            response_sender: Some(response_sender),
        };

        self.msg_sender.send(message).await?;

        let mut responses = Vec::new();
        while let Some(response) = response_receiver.recv().await {
            responses.push(response);
        }

        Ok(responses)
    }
}

impl Context {
    fn new(stream: TcpStream, msg_sender: Sender<Message>) -> Self {
        let msg_sender = MsgSender { msg_sender };
        Context { stream, msg_sender }
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

        let responses = match msg_sender.send_command(command).await {
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

        for response in responses {
            let encoded_response = encode_value(&response);

            if let Err(e) = stream.write_all(encoded_response.as_bytes()).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            println!("Sent: {:?}", encoded_response);
        }
    }
}

fn encode_value(val: &Value) -> String {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s),
        Value::SimpleError(s) => format!("-{}\r\n", s),
        Value::Integer(i) => format!(":{}\r\n", i),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
        Value::Array(vals) => {
            let n = vals.len();
            let body = vals.iter().map(encode_value).collect::<String>();

            format!("*{}\r\n{}", n, body)
        }
        Value::NullBulkString => "$-1\r\n".to_string(),
        Value::NullArray => "*-1\r\n".to_string(),
        Value::RdbFile(rdb) => {
            let rdb_file = rdb.iter().map(|b| format!("{:02x}", b)).collect::<String>();

            format!("${}\r\n{}", rdb_file.len(), rdb_file)
        }
        Value::Null => "_\r\n".to_string(),
    }
}

fn parse_command(input: &[u8]) -> Result<Command> {
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

fn parse_value(input: &[u8]) -> IResult<&[u8], Value> {
    alt((
        parse_simple_string,
        parse_integer,
        parse_bulk_string,
        parse_array,
        parse_rdb_file,
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

fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], Value> {
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
