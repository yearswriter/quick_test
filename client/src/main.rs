use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpSocket;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info};

#[tokio::main]
async fn main() -> io::Result<()> {
    let file_appender = tracing_appender::rolling::hourly("logs", "client");
    let (writer, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(writer)
        .init();

    let addr = "127.0.0.1:8150".parse().unwrap();
    let mut tasks = JoinSet::new();
    let sem = Arc::new(Semaphore::new(1000)); //absolute maximum of 1024

    for task in 1..100_000 {
        let sem_cloned = sem.clone();
        tasks.spawn(async move {
            let _permit = match sem_cloned.acquire().await {
                Ok(permit) => permit,
                Err(error) => {
                    error!(
                        "Failed to aquire permit on sempahore, task: {}, error: {:?}",
                        task, error
                    );
                    panic!("sync issue");
                }
            };
            let socket = match TcpSocket::new_v4() {
                Ok(socket) => socket,
                Err(error) => {
                    error!(
                        "Failed to create the socket, task: {}, error: {:?}",
                        task, error
                    );
                    panic!("Failed to create the socket");
                }
            };
            let task_bytes = (task as u32).to_be_bytes(); // Big Endian [u8;2]
            let buf: [u8; 8] = [
                task_bytes[0],
                task_bytes[1],
                task_bytes[2],
                task_bytes[3],
                0xB,
                0xA,
                9,
                8,
            ];
            let mut stream = match socket.connect(addr).await {
                Ok(socket) => socket,
                Err(error) => {
                    error!(
                        "Failed to connect to the socket, task: {}, error: {:?}",
                        task, error
                    );
                    panic!("Failed to connect to the socket");
                }
            };
            match stream.write_all(&buf.clone()).await {
                Ok(socket) => {
                    info!("writing to the stream from task: {}", task);
                    socket
                }
                Err(error) => {
                    error!(
                        "Failed to write to the stream, task: {}, error: {:?}",
                        task, error
                    );
                    panic!("Failed to write to the stream");
                }
            };
        });
    }

    while let Some(res) = tasks.join_next().await {
        // join_next() returns results as they complete.
        // We loop until it returns None (meaning the set is empty).
        // `res` is a Result<T, JoinError>, so we check for panics.
        if let Err(e) = res {
            error!("A task panicked: {:?}", e);
            break;
        }
    }

    Ok(())
}
