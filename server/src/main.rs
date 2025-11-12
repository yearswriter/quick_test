use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use time::format_description::well_known::Rfc3339;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::signal;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::OffsetTime;

const NUM_TASKS: usize = 1_000_000;
const NUM_BUCKETS: usize = (NUM_TASKS + 63) / 64;

#[instrument(skip_all)]
async fn process_socket(mut socket: TcpStream, state: Arc<Vec<AtomicU64>>) {
    let mut buf = [0u8; 8];
    // timeout for our clients so no zombie holds the connection
    // in a race with client's task retry timeout
    let read_timeout = Duration::from_secs(10);

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

                if task_id >= 1 && (task_id as usize) <= NUM_TASKS {
                    let task_idx = (task_id - 1) as usize;
                    let index = task_idx / 64;
                    let bit = task_idx % 64;
                    let mask = 1 << bit;

                    state[index].fetch_or(mask, std::sync::atomic::Ordering::Relaxed);
                } else {
                    warn!("Received out-of-range task_id: {}", task_id);
                }
            }
            Ok(Err(read_error)) => {
                error!("Failed to read socket: {}", read_error);
                return;
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

    let state_vec: Vec<AtomicU64> =
        (0..NUM_BUCKETS).map(|_| AtomicU64::new(0)).collect();
    let state = Arc::new(state_vec);

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
                let state_cloned = state.clone();
                tokio::spawn(async move {
                    process_socket(socket, state_cloned).await;
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

                let state_clone = state.clone();
                let check_handle = tokio::task::spawn_blocking(move || {
                    let mut missing_tasks = Vec::new();
                    for task_idx in 1..=NUM_TASKS {
                        let index = task_idx / 64;
                        let bit = task_idx % 64;
                        let mask = 1 << bit;
                        if (state_clone[index].load(std::sync::atomic::Ordering::Relaxed) & mask) == 0 {
                            missing_tasks.push(task_idx + 1);
                        }
                    }
                    missing_tasks
                });

                match check_handle.await {
                    Ok(missing) => {
                        if missing.is_empty() {
                            info!("All {} tasks received!", NUM_TASKS);
                        } else {
                            warn!("{} tasks missing!", missing.len());
                        }
                    }
                    Err(error) => {
                        error!("Task check panicked: {:?}", error);
                    }
                }
                break;
            }
        }
    }
    Ok(())
}
