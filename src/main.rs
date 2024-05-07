mod command;
mod config;
mod parsing;
mod types;

use anyhow::Result;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use command::CommandHandler;
use config::Args;
use parsing::parse_command;
use types::{into_bytes, Context, ServerType, Value};

#[tokio::main]
async fn main() -> Result<()> {
    let Args { port, replicaof } = Args::parse();
    let bind_addr = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(bind_addr.clone()).await?;
    println!("Listening on {}", listener.local_addr()?);

    let server_type = match replicaof.as_slice() {
        [] => ServerType::Master(0, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(), 0),
        [host, port] => {
            let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
            ServerType::Slave(stream)
        }
        _ => unreachable!(),
    };

    let (command_handler, msg_sender) = CommandHandler::new(port, server_type);

    tokio::spawn(command_handler.run());

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
                let encoded_response = into_bytes(&response);

                if let Err(e) = stream.write_all(&encoded_response).await {
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
                let encoded_response = into_bytes(&response);

                if let Err(e) = stream.write_all(&encoded_response).await {
                    eprintln!("Failed to write to socket: {}", e);
                    return;
                }

                println!("Sent: {:?}", encoded_response);

                continue;
            }
        };

        for response in responses {
            let encoded_response = into_bytes(&response);

            if let Err(e) = stream.write_all(&encoded_response).await {
                eprintln!("Failed to write to socket: {}", e);
                return;
            }

            println!("Sent: {:?}", encoded_response);
        }
    }
}
