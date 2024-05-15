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

use crate::types::{cmd_into_bytes, val_into_bytes};
use crate::types::{Command, Message, ServerType, Value};
use crate::{
    parsing::{parse_rdb_file, parse_simple_string},
    types::Expiration,
};

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

        if let Err(e) = self.handle_handshake().await {
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

                    match command {
                        Command::Set(..) => {
                            match self.server_type {
                                ServerType::Master(ref mut repl_streams, ..) => {
                                    let mut to_remove = vec![];

                                    for (i, stream) in repl_streams.iter_mut().enumerate() {
                                        let encoded_command = cmd_into_bytes(&command);

                                        if let Err(_) = stream.write_all(&encoded_command).await {
                                            to_remove.push(i);
                                        }
                                    }

                                    for i in to_remove.into_iter().rev() {
                                        repl_streams.remove(i);
                                    }
                                }
                                ServerType::Replica(..) => {},
                            }
                        }
                        _ => {}
                    }

                    let responses = match command {
                        Command::Ping => self.handle_ping(),
                        Command::Echo(value) => self.handle_echo(value),
                        Command::Get(key) => self.handle_get(key),
                        Command::Set(key, value, expiration) => self.handle_set(key, value, expiration),
                        Command::Info(of_type) => self.handle_info(of_type),
                        Command::ReplConf(pairs) => self.handle_replconf(pairs),
                        Command::Psync(replid, offset) => self.handle_psync(replid, offset),
                        Command::ReplAdd(stream) => {
                            match self.server_type {
                                ServerType::Master(ref mut repl_streams, ..) => {
                                    repl_streams.push(stream);
                                }
                                ServerType::Replica(..) => {}
                            }

                            vec![]
                        }
                    };

                    if let Some(response_sender) = response_sender {
                        if let Err(e) = response_sender.send(responses) {
                            eprintln!("Failed to send response: {:?}", e);
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

    fn handle_set(
        &mut self,
        key: Value,
        value: Value,
        expiration: Option<Expiration>,
    ) -> Vec<Value> {
        let expiration_time = expiration.map(|val| {
            let now = Utc::now().timestamp_millis() as u128;
            match val {
                Expiration::Ex(s) => now + s * 1000,
                Expiration::Px(ms) => now + ms,
            }
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
                            ServerType::Master(..) => "master",
                            ServerType::Replica(..) => "slave",
                        };

                        let (replid, repl_offset) = match &self.server_type {
                            ServerType::Master(_, replid, repl_offset) => {
                                (replid.as_str(), repl_offset)
                            }
                            ServerType::Replica(..) => ("0", &0),
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

    fn handle_replconf(&self, pairs: Vec<(Value, Value)>) -> Vec<Value> {
        for (key, value) in pairs {
            match (key, value) {
                (Value::BulkString(k), Value::BulkString(v))
                    if k.eq_ignore_ascii_case("listening-port")
                        && v.parse() == Ok(self.listen_port) => {}
                (Value::BulkString(_k), Value::BulkString(_v)) => {}
                _ => {
                    return vec![Value::SimpleError("Invalid REPLCONF value".to_string())];
                }
            }
        }

        vec![Value::SimpleString("OK".to_string())]
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
                val_into_bytes(&Value::Array(vec![Value::BulkString("PING".to_string())]));

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

            match parse_simple_string(received_data) {
                Ok((_, Value::SimpleString(s))) if s.eq_ignore_ascii_case("pong") => {}
                Ok(_) => {
                    return Err(anyhow!("Invalid response: {:?}", received_data));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to parse response: {}", e));
                }
            }

            let encoded_response = val_into_bytes(&Value::Array(vec![
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

            match parse_simple_string(received_data) {
                Ok((_, Value::SimpleString(s))) if s.eq_ignore_ascii_case("ok") => {}
                Ok(_) => {
                    return Err(anyhow!("Invalid response: {:?}", received_data));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to parse response: {}", e));
                }
            }

            let encoded_response = val_into_bytes(&Value::Array(vec![
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

            match parse_simple_string(received_data) {
                Ok((_, Value::SimpleString(s))) if s.eq_ignore_ascii_case("ok") => {}
                Ok(_) => {
                    return Err(anyhow!("Invalid response: {:?}", received_data));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to parse response: {}", e));
                }
            }

            let encoded_response = val_into_bytes(&Value::Array(vec![
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

            match parse_simple_string(received_data) {
                Ok((_, Value::SimpleString(s))) if s.eq_ignore_ascii_case("continue") => Ok(()),
                Ok((remaining, Value::SimpleString(s))) => {
                    let parts = s.split_whitespace().collect_vec();

                    match parts.as_slice() {
                        [msg, _replid, _offset] if msg.to_lowercase().as_str() == "fullresync" => {
                            let rdb_file = if remaining.is_empty() {
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

                            match parse_rdb_file(rdb_file) {
                                Ok((_, Value::RdbFile(rdb_file))) => {
                                    println!("Received: {}", rdb_file.escape_ascii());
                                    Ok(())
                                }
                                Ok(_) => Err(anyhow!("Invalid response: {}", s)),
                                Err(e) => Err(anyhow!("Failed to parse response: {}", e)),
                            }
                        }
                        _ => {
                            return Err(anyhow!("Invalid response: {}", s));
                        }
                    }
                }
                Ok(_) => {
                    return Err(anyhow!("Invalid response: {:?}", received_data));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to parse response: {}", e));
                }
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
