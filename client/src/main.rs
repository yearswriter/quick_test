use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::OffsetTime;

const NUM_TASKS: u32 = 1_000_000;

#[derive(Debug)]
#[allow(dead_code)]
enum ConnectionError {
    Semaphore(String),
    SocketCreate(io::Error),
    Connect(io::Error),
    Write(io::Error),
}

type ConnectionPool = Arc<Mutex<Vec<TcpStream>>>;

#[instrument(skip_all)]
async fn connection(
    task: u32,
    sem: Arc<Semaphore>,
    addr: SocketAddr,
    pool: ConnectionPool,
) -> Result<(), ConnectionError> {
    // this will be released as _permit's lifetime ends(scope of this fn)
    let _permit = match sem.acquire().await {
        Ok(permit) => permit,
        Err(error) => {
            // This specific error (Closed) is non-recoverable.
            error!(
                "Failed to acquire permit, task: {}, error: {:?}",
                task, error
            );
            return Err(ConnectionError::Semaphore(error.to_string()));
        }
    };

    #[allow(unused_mut)]
    let mut stream = {
        let mut locked_pool = pool.lock().await;
        locked_pool.pop()
    };

    let mut stream = match stream {
        Some(stream) => {
            info!("Reusing connection from pool for tak: {}", task);
            stream
        }
        None => {
            info!("Pool empty. Creating new connection for task: {}", task);
            let socket = match TcpSocket::new_v4() {
                Ok(socket) => socket,
                Err(error) => {
                    error!(
                        "Failed to create the socket, task: {}, error: {:?}",
                        task, error
                    );
                    return Err(ConnectionError::SocketCreate(error));
                }
            };
            match socket.connect(addr).await {
                Ok(socket) => socket,
                Err(error) => {
                    warn!(
                        "Failed to connect to the socket, task: {}, error: {:?}",
                        task, error
                    );
                    return Err(ConnectionError::Connect(error));
                }
            }
        }
    };

    let task_bytes: [u8; 4] = task.to_be_bytes(); // Big Endian
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&task_bytes);
    buf[4..8].copy_from_slice(&[0xB, 0xA, 9, 8]);

    match stream.write_all(&buf).await {
        Ok(_) => {
            info!("wrote to the stream from task: {}", task);
            let mut locked_pool = pool.lock().await;
            locked_pool.push(stream);
            Ok(())
        }
        Err(error) => {
            error!(
                "Failed to write to the stream, task: {}, error: {:?}",
                task, error
            );
            Err(ConnectionError::Write(error))
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <ip:port>", args[0]);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Missing <ip:port> argument",
        ));
    }
    let addr: SocketAddr = match args[1].parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Invalid IP:port argument: {}", e);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid <ip:port> argument",
            ));
        }
    };
    let file_appender = tracing_appender::rolling::hourly("logs", "client");
    let (writer, _guard) = tracing_appender::non_blocking(file_appender);

    let log_filter = match EnvFilter::try_from_env("QT_LOG_LVL") {
        Ok(filter) => filter,
        Err(er) => {
            warn!("Could not read QT_LOG_LVL env val: {:?}", er);
            EnvFilter::new("info,time=error")
        }
    };

    let local_clock = match OffsetTime::local_rfc_3339() {
        Ok(time) => time,
        Err(er) => {
            warn!("Failed to create clock for logs: {:?}", er);
            OffsetTime::new(time::UtcOffset::UTC, Rfc3339)
        }
    };
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_writer(writer)
        .with_timer(local_clock)
        .init();

    let mut tasks = JoinSet::new();
    // 500 works for fd and ephemeral port limit
    // and it matches server's 500 inc conn
    let sem = Arc::new(Semaphore::new(1000));
    let pool: ConnectionPool = Arc::new(Mutex::new(Vec::new()));

    for task in 1..=NUM_TASKS {
        let sem_cloned = sem.clone();
        let pool_cloned = pool.clone();
        tasks.spawn(async move {
            (task, connection(task, sem_cloned, addr, pool_cloned).await)
        });
    }

    let mut successful_tasks = 0;
    let mut retried_tasks = 0;
    let mut failed_tasks = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((_task, Ok(()))) => {
                successful_tasks += 1;
            }
            Ok((task, Err(error))) => {
                let (is_retryable, specific_error) = match &error {
                    ConnectionError::Connect(err) => (true, err),
                    ConnectionError::Write(err) => (true, err),
                    _ => (false, &io::Error::other("non-io error")),
                };

                if is_retryable {
                    let _reason = match error {
                        ConnectionError::Connect(_) => "Connection failed",
                        ConnectionError::Write(_) => "write failed",
                        _ => "unreachable",
                    };
                    warn!(
                        "Retrying task {}, reason: connection failed ({:?})",
                        task, specific_error
                    );
                    retried_tasks += 1;
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let sem_cloned = sem.clone();
                    let pool_cloned = pool.clone();
                    tasks.spawn(async move {
                        (task, connection(task, sem_cloned, addr, pool_cloned).await)
                    });
                } else {
                    error!("Task {} failed: {:?}", task, error);
                    failed_tasks += 1;
                }
            }
            Err(panic_error) => {
                error!("A task panicked: {:?}", panic_error);
                failed_tasks += 1;
            }
        }
    }

    info!(
        "JoinSet is empty. Final counts: {} successful, {} failed, {} retries executed.",
        successful_tasks, failed_tasks, retried_tasks
    );
    Ok(())
}
