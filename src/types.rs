use anyhow::Result;
use core::fmt;
use itertools::Itertools;
use std::fmt::{Display, Formatter};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Sender},
};

#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(Value),
    Get(Value),
    Set(Value, Value, Option<u128>),
    Info(Value),
    ReplConf(Vec<(Value, Value)>),
    Psync(Value, Value),
}

pub(crate) struct MsgSender {
    msg_sender: Sender<Message>,
}
pub(crate) struct Context {
    pub(crate) stream: TcpStream,
    pub(crate) msg_sender: MsgSender,
}

#[derive(Debug)]
pub(crate) enum ServerType {
    Master(u32, String, u128),
    Slave(TcpStream),
}

#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) command: Command,
    pub(crate) response_sender: Option<Sender<Value>>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(crate) enum Value {
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

impl Display for ServerType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ServerType::Master(_count, _replid, _repl_offset) => write!(f, "master"),
            ServerType::Slave(_master_strm) => write!(f, "slave"),
        }
    }
}

impl MsgSender {
    pub(crate) async fn send_command(&self, command: Command) -> Result<Vec<Value>> {
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
    pub(crate) fn new(stream: TcpStream, msg_sender: Sender<Message>) -> Self {
        let msg_sender = MsgSender { msg_sender };
        Context { stream, msg_sender }
    }
}

pub(crate) fn into_bytes(val: &Value) -> Vec<u8> {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
        Value::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
        Value::Integer(i) => format!(":{}\r\n", i).into_bytes(),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
        Value::Array(vals) => {
            let n = vals.len();
            let body = vals.iter().map(into_bytes).flatten().collect_vec();

            format!("*{}\r\n", n)
                .into_bytes()
                .iter()
                .chain(body.iter())
                .map(|&b| b)
                .collect_vec()
        }
        Value::NullBulkString => b"$-1\r\n".to_vec(),
        Value::NullArray => b"*-1\r\n".to_vec(),
        Value::RdbFile(rdb_file) => format!("${}\r\n", rdb_file.len())
            .into_bytes()
            .iter()
            .chain(rdb_file.iter())
            .map(|&b| b)
            .collect_vec(),
        Value::Null => b"_\r\n".to_vec(),
    }
}
