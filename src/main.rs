mod command;
mod config;
mod parsing;
mod types;

use anyhow::Result;
use clap::Parser;
use nom::combinator::all_consuming;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};

use command::CommandHandler;
use config::Args;
use parsing::parse_command;
use types::{into_bytes, Message, ServerType, Value};

#[tokio::main]
async fn main() -> Result<()> {
    let Args { port, replicaof } = Args::parse();
    let bind_addr = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(bind_addr.clone()).await?;
    println!("Listening on {}", listener.local_addr()?);

    let server_type = match replicaof.as_slice() {
        [] => ServerType::Master(
            vec![],
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            0,
        ),
        [host, port] => {
            let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
            ServerType::Replica(stream)
        }
        _ => unreachable!(),
    };

    let (command_handler, msg_sender) = CommandHandler::new(port, server_type);

    tokio::spawn(command_handler.run());

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("\nAccepted connection from {}", addr);

        tokio::spawn(handle_connection(stream, msg_sender.clone()));
    }
}

async fn handle_connection(stream: TcpStream, msg_sender: Sender<Message>) {
    let mut buffer = Vec::new();
    let mut stream = BufReader::new(stream);

    loop {
        let mut chunk = [0; 1024];
        let n = match stream.read(&mut chunk).await {
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

        buffer.extend_from_slice(&chunk[..n]);

        println!("Buffer: {}", buffer.escape_ascii());

        loop {
            let (remaining, command) = match all_consuming(parse_command)(&buffer) {
                Ok((remaining, command)) => (remaining, command),
                Err(nom::Err::Incomplete(_)) => break,
                Err(e) => {
                    eprintln!("Failed to parse command: {}", e);

                    let response = Value::SimpleError(e.to_string());
                    let encoded_response = into_bytes(&response);

                    if let Err(e) = stream.write_all(&encoded_response).await {
                        eprintln!("Failed to write to socket: {}", e);
                        return;
                    }

                    println!("Sent: {}", encoded_response.escape_ascii());

                    continue;
                }
            };

            buffer = remaining.to_vec();

            let responses = match msg_sender.send_command(command).await {
                Ok(response) => response,
                Err(e) => {
                    eprintln!("Failed to send command: {}", e);

                    let response = Value::SimpleError(e.to_string());
                    let encoded_response = into_bytes(&response);

                    if let Err(e) = stream.write_all(&encoded_response).await {
                        eprintln!("Failed to write to socket: {}", e);
                        return;
                    }

                    println!("Sent: {}", encoded_response.escape_ascii());

                    continue;
                }
            };

            for response in responses {
                let encoded_response = into_bytes(&response);

                if let Err(e) = stream.write_all(&encoded_response).await {
                    eprintln!("Failed to write to socket: {}", e);
                    return;
                }

                println!("Sent: {}", encoded_response.escape_ascii());
            }
        }
    }
}
