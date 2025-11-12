use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::signal;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

#[instrument(skip_all)]
async fn process_socket(mut socket: TcpStream) {
    let mut buf = [0u8; 8];
    // timeout for our clients so no zombie holds the connection
    // in a race with client's task retry timeout
    let read_timeout = Duration::from_millis(100);

    // Loop to wait for a client to end connection
    // TODO: Frames with tokio_util::{Decoder, Encoder}
    //  (but that requies thinking about
    //      proper packet with length headers and fields and such)
    loop {
        match timeout(read_timeout, socket.read_exact(&mut buf)).await {
            Ok(Ok(_)) => {
                let received_task: [u8; 4] = match buf[0..4].try_into() {
                    Ok(task) => task,
                    Err(error) => {
                        error!("Failed to slice buff for task_id: {:?}", error);
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
                error!("Failed to read socket: {}", read_error);
            }
            Err(_) => {
                warn!("Client timed out from fd: {}", socket.as_raw_fd());
                return;
            }
        };
    }
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

    // 500 for listener backlog of 4096 and fd limit of 1024
    let sem_conn = Arc::new(Semaphore::new(500));
    // empty conn_permit to switch select branches
    let mut conn_permit: Option<OwnedSemaphorePermit> = None;

    loop {
        tokio::select! {
        // This branch is *only* enabled if we are *not*
        // already holding a permit (`conn_permit.is_none()`).
        // This also means that after 500 connections tasks are
        // more or less sequential, but wcyd
            conn_result = sem_conn.clone().acquire_owned(), if conn_permit.is_none() => {
                match conn_result {
                    Ok(c_permit) => {
                        info!("conn permit");
                        conn_permit = Some(c_permit)
                    }
                    Err(error) => {
                            warn!("Failed to acquire conn permit: {:?}, Shutting down", error);
                            token.cancel();
                    }
                }
            }
            Ok((socket, _)) = listener.accept(), if conn_permit.is_some() => {
                info!("new socket");
                let permit = conn_permit.take().unwrap();
                tokio::spawn(async move {
                    process_socket(socket).await;
                    // explicit for linter
                    // and for readability
                   drop(permit);
                });
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
