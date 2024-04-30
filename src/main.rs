use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    listener.incoming().for_each(|stream| match stream {
        Ok(mut stream) => {
            println!("New connection: {:?}", stream);

            let mut buf = [0; 1024];

            stream.read(&mut buf).unwrap();
            println!("Received: {:?}", buf);

            stream.write(b"+PONG\r\n").unwrap();
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    });
}
