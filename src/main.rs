use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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
    sync::mpsc::{Receiver, Sender},
};

// IDEA FOR EXPIRATION HANDLING: STORE EXPIRATION TIME WITH VALUE AND ALSO KEEP A SEPARATE STRUCTURE
// WITH THE EXPIRATION TIME AS THE KEY AND THE DATA MAP KEY AS THE VALUE. WHEN WE GET AN EXPIRED
// KEY GRAB ALL EXPIRATION TIMES THAT ARE LESS THAN OR EQUAL TO THE CURRENT TIME AND DELETE
// THEM FROM THE EXPIRATION MAP AND THE DATA MAP ALL AT ONCE.

// SWITCH TO AN EVENT LOOP MODEL WHERE WE HAVE A SINGLE TOKIO TASK THAT HANDLES ALL CONNECTIONS
// AND COMMANDS. THIS WAY WE CAN HAVE A SINGLE STORAGE STRUCTURE THAT IS SHARED ACROSS ALL
// CONNECTIONS. WE CAN USE A CHANNEL TO SEND COMMANDS TO THE TASK AND RECEIVE RESPONSES.
// WE CAN ALSO USE A CHANNEL TO SEND COMMANDS TO THE TASK TO SHUTDOWN.

#[derive(Debug)]
enum Command {
    Ping,
    Echo(Value),
    Get(Value),
    Set(Value, Value),
}

#[derive(Debug)]
struct Message {
    command: Command,
    response: Sender<Value>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
enum Value {
    SimpleString(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    NullBulkString,
    NullArray,
    Null,
}

#[derive(Debug)]
struct DataActor {
    data: HashMap<Value, Value>,
    expiration: BTreeMap<i64, Value>,
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
        while let Some(message) = self.msg_receiver.recv().await {
            let response = match message.command {
                Command::Ping => self.handle_ping(),
                Command::Echo(value) => self.handle_echo(value),
                Command::Get(key) => self.handle_get(key),
                Command::Set(key, value) => self.handle_set(key, value),
            };

            match message.response.send(response).await {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to send response: {}", e),
            }
        }
    }

    fn handle_ping(&self) -> Value {
        Value::SimpleString("PONG".to_string())
    }

    fn handle_echo(&self, value: Value) -> Value {
        value
    }

    fn handle_get(&self, key: Value) -> Value {
        match self.data.get(&key) {
            Some(value) => value.clone(),
            None => Value::NullBulkString,
        }
    }

    fn handle_set(&mut self, key: Value, value: Value) -> Value {
        self.data.insert(key, value);
        Value::SimpleString("OK".to_string())
    }
}

struct MsgSender {
    msg_sender: Sender<Message>,
}

impl MsgSender {
    async fn send_command(&self, command: Command) -> Result<Value> {
        let (response_sender, mut response_receiver) = tokio::sync::mpsc::channel(1);

        let message = Message {
            command,
            response: response_sender,
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
        println!("Accepted connection from {}", addr);

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
                continue;
            }
        };

        let response = match msg_sender.send_command(command).await {
            Ok(response) => response,
            Err(e) => {
                eprintln!("Failed to send command: {}", e);
                return;
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
            if args.next().is_some() {
                Err(anyhow!("SET command takes exactly two arguments"))
            } else {
                Ok(Command::Set(key, value))
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
