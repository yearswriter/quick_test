use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio_util::sync::CancellationToken;

async fn process_socket(mut socket: TcpStream, token: CancellationToken) {
    dbg!(&socket);
    let mut buf = [0u8; 8];

    match socket.read_exact(&mut buf).await {
        Ok(_) => {
            let received_task: [u8; 2] = buf[0..2].try_into().unwrap();
            let task_id = u16::from_be_bytes(received_task);
            println!("Received task: {}", task_id);
        }
        Err(e) => {
            eprintln!("Failed to read: {}", e);
            token.cancel();
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8150").await?;
    let token = CancellationToken::new();

    loop {
        tokio::select! {
         Ok((socket, _)) = listener.accept() => {
            tokio::spawn(process_socket(socket, token.clone()));
        }
        _ = token.cancelled() => {
            println!("\nTask canceled token, shutting down!");
            break;
        }
        Ok(()) = signal::ctrl_c() => {
            println!("\nReceived Ctrl+C, shutting down!");
            break;
        }
        }
    }
    Ok(())
}
