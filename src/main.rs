use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted connection: {}", addr);

                tokio::spawn(handle_client(stream));
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 1024];

    loop {
        let bytes_read = stream.read(&mut buf).await.unwrap();
        if bytes_read == 0 {
            println!("Connection closed");
            break;
        }

        println!("Bytes read: {}", bytes_read);

        stream.write(b"+PONG\r\n").await.unwrap();
    }
}
