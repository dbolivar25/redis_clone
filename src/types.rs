use anyhow::Result;
use core::fmt;
use itertools::Itertools;
use std::fmt::{Display, Formatter};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};

#[derive(Debug)]
pub(crate) enum Expiration {
    Ex(u128),
    Px(u128),
}

#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(Value),
    Get(Value),
    Set(Value, Value, Option<Expiration>),
    Info(Value),
    ReplConf(Vec<(Value, Value)>),
    Psync(Value, Value),
    ReplAdd(TcpStream),
}

pub(crate) struct MsgSender {
    msg_sender: mpsc::Sender<Message>,
}

#[derive(Debug)]
pub(crate) enum ServerType {
    Master(Vec<TcpStream>, String, u128),
    Replica(TcpStream),
}

#[derive(Debug)]
pub(crate) struct Message {
    pub(crate) command: Command,
    pub(crate) response_sender: Option<oneshot::Sender<Vec<Value>>>,
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
    pub(crate) fn new(msg_sender: mpsc::Sender<Message>) -> Self {
        Self { msg_sender }
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<Vec<Value>> {
        let (response_sender, response_receiver) = oneshot::channel();

        let message = Message {
            command,
            response_sender: Some(response_sender),
        };

        self.msg_sender.send(message).await?;

        let responses = response_receiver.await?;

        Ok(responses)
    }
}

pub(crate) fn val_into_bytes(val: &Value) -> Vec<u8> {
    match val {
        Value::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
        Value::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
        Value::Integer(i) => format!(":{}\r\n", i).into_bytes(),
        Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
        Value::Array(vals) => {
            let body = vals.iter().map(val_into_bytes).flatten().collect_vec();

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

pub(crate) fn cmd_into_bytes(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Ping => val_into_bytes(&Value::Array(vec![Value::BulkString("PING".to_string())])),
        Command::Echo(val) => val_into_bytes(&Value::Array(vec![
            Value::BulkString("ECHO".to_string()),
            val.clone(),
        ])),
        Command::Get(val) => val_into_bytes(&Value::Array(vec![
            Value::BulkString("GET".to_string()),
            val.clone(),
        ])),
        Command::Set(key, val, expiry) => match expiry {
            Some(Expiration::Ex(s)) => val_into_bytes(&Value::Array(vec![
                Value::BulkString("SET".to_string()),
                key.clone(),
                val.clone(),
                Value::BulkString("EX".to_string()),
                Value::BulkString(s.to_string()),
            ])),
            Some(Expiration::Px(ms)) => val_into_bytes(&Value::Array(vec![
                Value::BulkString("SET".to_string()),
                key.clone(),
                val.clone(),
                Value::BulkString("PX".to_string()),
                Value::BulkString(ms.to_string()),
            ])),
            None => val_into_bytes(&Value::Array(vec![
                Value::BulkString("SET".to_string()),
                key.clone(),
                val.clone(),
            ])),
        },
        Command::Info(section) => val_into_bytes(&Value::Array(vec![
            Value::BulkString("INFO".to_string()),
            section.clone(),
        ])),
        Command::ReplConf(conf) => {
            let mut cmd = vec![Value::BulkString("REPLCONF".to_string())];
            for (key, val) in conf {
                cmd.push(key.clone());
                cmd.push(val.clone());
            }

            val_into_bytes(&Value::Array(cmd))
        }
        Command::Psync(replid, offset) => val_into_bytes(&Value::Array(vec![
            Value::BulkString("PSYNC".to_string()),
            replid.clone(),
            offset.clone(),
        ])),
        Command::ReplAdd(..) => unimplemented!(),
    }
}
