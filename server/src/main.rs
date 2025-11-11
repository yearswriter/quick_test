use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::signal;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

#[instrument(skip_all)]
async fn process_socket(mut socket: TcpStream, token: CancellationToken) {
    let mut buf = [0u8; 8];
    // timeout for our clients so no zombie holds the connection
    let read_timeout = Duration::from_secs(5);

    match timeout(read_timeout, socket.read_exact(&mut buf)).await {
        Ok(Ok(_)) => {
            let received_task: [u8; 4] = match buf[0..4].try_into() {
                Ok(task) => task,
                Err(error) => {
                    error!("Failed to slice buff for task_id: {:?}", error);
                    token.cancel();
                    return;
                }
            };
            let task_id = u32::from_be_bytes(received_task);
            info!(
                "Received task: {}, from fd: {:#}",
                task_id,
                socket.as_raw_fd()
            );
        }
        Ok(Err(read_error)) => {
            error!("Failed to read: {}", read_error);
        }
        Err(_) => {
            warn!("Client timed out from fd: {}", socket.as_raw_fd());
        }
    };
}

async fn create_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    // need this over TcListener::bind(addr) to enlarge OS backlog for tcp bind
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(4096)?;
    Ok(listener)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr: SocketAddr = "0.0.0.0:8150".parse().unwrap();
    let listener = create_listener(addr).await?;

    let token = CancellationToken::new();

    let file_appender = tracing_appender::rolling::hourly("logs", "server");
    let (writer, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(writer)
        .init();

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
            _ = token.cancelled() => {
                info!("\nTask canceled token, shutting down!");
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
