use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpSocket;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = "10.204.1.2:8150".parse().unwrap();
    let mut tasks = JoinSet::new();

    for task in 1..1000 {
        tasks.spawn(async move {
            let socket = TcpSocket::new_v4()
                .expect(&format!("failed to create the socket: {task}"));
            let task_bytes = (task as u16).to_be_bytes(); // Big Endian [u8;2]
            let buf: [u8; 8] = [task_bytes[0], task_bytes[1], 0xD, 0xC, 0xB, 0xA, 9, 8];
            let mut stream = socket
                .connect(addr)
                .await
                .expect(&format!("failed to connect the socket: {task}"));
            stream
                .write_all(&buf.clone())
                .await
                .expect(&format!("failed to write the stream: {task}"));
        });
    }

    while let Some(res) = tasks.join_next().await {
        // join_next() returns results as they complete.
        // We loop until it returns None (meaning the set is empty).
        // `res` is a Result<T, JoinError>, so we check for panics.
        if let Err(e) = res {
            eprintln!("A task panicked: {:?}", e);
            break;
        }
    }

    Ok(())
}
