use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    listener.incoming().for_each(|stream| match stream {
        Ok(mut stream) => {
            println!("New connection: {:?}", stream);

            let mut buf = [0; 1024];

            loop {
                let bytes_read = stream.read(&mut buf).unwrap();
                if bytes_read == 0 {
                    println!("Connection closed");
                    break;
                }

                println!("Bytes read: {}", bytes_read);

                stream.write(b"+PONG\r\n").unwrap();
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    });
}
