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

#[derive(Debug)]
pub(crate) enum ServerType {
    Master(Vec<TcpStream>, String, u128),
    Replica(TcpStream),
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
            ServerType::Master(..) => write!(f, "master"),
            ServerType::Replica(..) => write!(f, "slave"),
        }
    }
}

impl MsgSender {
    pub(crate) async fn send_command(&self, command: Command) -> Result<Vec<Value>> {
        let (response_sender, mut response_receiver) = mpsc::channel(16);

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

pub(crate) fn into_bytes(val: &Value) -> Vec<u8> {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
        Value::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
        Value::Integer(i) => format!(":{}\r\n", i).into_bytes(),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
        Value::Array(vals) => {
            let body = vals.iter().map(into_bytes).flatten().collect_vec();

            format!("*{}\r\n", vals.len())
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
