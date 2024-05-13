use anyhow::{anyhow, Result};
use chrono::Utc;
use itertools::Itertools;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};

use crate::parsing::{parse_rdb_file, parse_simple_string};
use crate::types::into_bytes;
use crate::types::{Command, Message, ServerType, Value};

const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[derive(Debug)]
pub(crate) struct CommandHandler {
    listen_port: u16,
    server_type: ServerType,
    data: HashMap<Value, (Option<u128>, Value)>,
    expiration: BTreeMap<u128, Value>,
    msg_receiver: Receiver<Message>,
}

impl CommandHandler {
    pub(crate) fn new(listen_port: u16, server_type: ServerType) -> (Self, Sender<Message>) {
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

    pub(crate) async fn run(mut self) {
        let mut gc_interval = interval(Duration::from_secs(1));

        let result = self.handle_handshake().await;
        if let Err(e) = result {
            eprintln!("Failed to handle handshake: {}", e);
            return;
        }

        loop {
            select! {
                _ = gc_interval.tick() => {
                    self.handle_expired_keys();
                }
                Some(message) = self.msg_receiver.recv() => {
                    let Message { command, response_sender } = message;

                    let responses = match command {
                        Command::Ping => self.handle_ping(),
                        Command::Echo(value) => self.handle_echo(value),
                        Command::Get(key) => self.handle_get(key),
                        Command::Set(key, value, expiration) => self.handle_set(key, value, expiration),
                        Command::Info(of_type) => self.handle_info(of_type),
                        Command::ReplConf(_pairs) => vec![Value::SimpleString("OK".to_string())],
                        Command::Psync(replid, offset) => self.handle_psync(replid, offset),
                    };

                    if let Some(response_sender) = response_sender {
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
                            ServerType::Replica(_) => "slave",
                        };

                        let (replid, repl_offset) = match &self.server_type {
                            ServerType::Master(_, replid, repl_offset) => {
                                (replid.as_str(), repl_offset)
                            }
                            ServerType::Replica(_) => ("0", &0),
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
                    let decoded = hex::decode(EMPTY_RDB_HEX).unwrap();

                    vec![
                        Value::SimpleString(format!("FULLRESYNC {} {}", replid_str, repl_offset)),
                        Value::RdbFile(decoded),
                    ]
                }
            }
            ServerType::Replica(_) => vec![Value::SimpleString("CONTINUE".to_string())],
        }
    }

    async fn handle_handshake(&mut self) -> Result<()> {
        if let ServerType::Replica(ref mut stream) = self.server_type {
            let mut buf = [0; 1024];

            let encoded_response =
                into_bytes(&Value::Array(vec![Value::BulkString("PING".to_string())]));

            if let Err(e) = stream.write_all(&encoded_response).await {
                return Err(anyhow!("Failed to write to socket: {}", e));
            }

            println!("Sent: {}", encoded_response.escape_ascii());

            let n = match stream.read(&mut buf).await {
                Ok(0) => {
                    return Err(anyhow!("Connection closed"));
                }
                Ok(n) => n,
                Err(e) => {
                    return Err(anyhow!("Failed to read from socket: {}", e));
                }
            };

            let received_data = &buf[..n];
            println!("Received: {}", received_data.escape_ascii());

            let encoded_response = into_bytes(&Value::Array(vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("listening-port".to_string()),
                Value::BulkString(format!("{}", self.listen_port)),
            ]));

            if let Err(e) = stream.write_all(&encoded_response).await {
                return Err(anyhow!("Failed to write to socket: {}", e));
            }

            println!("Sent: {}", encoded_response.escape_ascii());

            let n = match stream.read(&mut buf).await {
                Ok(0) => {
                    return Err(anyhow!("Connection closed"));
                }
                Ok(n) => n,
                Err(e) => {
                    return Err(anyhow!("Failed to read from socket: {}", e));
                }
            };

            let received_data = &buf[..n];
            println!("Received: {}", received_data.escape_ascii());

            let encoded_response = into_bytes(&Value::Array(vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("capa".to_string()),
                Value::BulkString("eof".to_string()),
                Value::BulkString("capa".to_string()),
                Value::BulkString("psync2".to_string()),
            ]));

            if let Err(e) = stream.write_all(&encoded_response).await {
                return Err(anyhow!("Failed to write to socket: {}", e));
            }

            println!("Sent: {}", encoded_response.escape_ascii());

            let n = match stream.read(&mut buf).await {
                Ok(0) => {
                    return Err(anyhow!("Connection closed"));
                }
                Ok(n) => n,
                Err(e) => {
                    return Err(anyhow!("Failed to read from socket: {}", e));
                }
            };

            let received_data = &buf[..n];
            println!("Received: {}", received_data.escape_ascii());

            let encoded_response = into_bytes(&Value::Array(vec![
                Value::BulkString("PSYNC".to_string()),
                Value::BulkString("?".to_string()),
                Value::BulkString("-1".to_string()),
            ]));

            if let Err(e) = stream.write_all(&encoded_response).await {
                return Err(anyhow!("Failed to write to socket: {}", e));
            }

            println!("Sent: {}", encoded_response.escape_ascii());

            let n = match stream.read(&mut buf).await {
                Ok(0) => {
                    return Err(anyhow!("Connection closed"));
                }
                Ok(n) => n,
                Err(e) => {
                    return Err(anyhow!("Failed to read from socket: {}", e));
                }
            };

            let received_data = &buf[..n];
            println!("Received: {}", received_data.escape_ascii());

            let (remaining, response) = match parse_simple_string(received_data) {
                Ok(res) => res,
                Err(e) => {
                    return Err(anyhow!("Failed to parse response: {}", e));
                }
            };

            if let Value::SimpleString(s) = response {
                let parts = s.split_whitespace().collect_vec();
                match parts.as_slice() {
                    [msg] if msg.to_lowercase().as_str() == "continue" => Ok(()),
                    [msg, _replid, _offset] if msg.to_lowercase().as_str() == "fullresync" => {
                        let received = if remaining.is_empty() {
                            let n = match stream.read(&mut buf).await {
                                Ok(0) => {
                                    return Err(anyhow!("Connection closed"));
                                }
                                Ok(n) => n,
                                Err(e) => {
                                    return Err(anyhow!("Failed to read from socket: {}", e));
                                }
                            };

                            &buf[..n]
                        } else {
                            remaining
                        };

                        let (_remaining, response) = match parse_rdb_file(received) {
                            Ok(res) => res,
                            Err(e) => {
                                return Err(anyhow!("Failed to parse response: {}", e));
                            }
                        };

                        if let Value::RdbFile(response) = response {
                            println!("Received: {}", response.escape_ascii());
                            Ok(())
                        } else {
                            Err(anyhow!("Invalid response: {}", s))
                        }
                    }
                    _ => {
                        return Err(anyhow!("Invalid response: {}", s));
                    }
                }
            } else {
                return Err(anyhow!("Invalid response: {:?}", response));
            }
        } else {
            Ok(())
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
