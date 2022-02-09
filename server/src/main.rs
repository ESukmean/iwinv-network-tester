#[derive(Clone, Eq, PartialEq, Debug)]
enum Message{
	broadcast(usize, bytes::Bytes)
}

#[derive(Clone)]
struct Shared {
	broadcast: tokio::sync::broadcast::Sender<Message>,
	message_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
	message_sent: std::sync::Arc<std::sync::atomic::AtomicUsize>
}

fn main() {
	use tokio::runtime;

	// This will spawn a work-stealing runtime with 4 worker threads.
	let rt = runtime::Builder::new_multi_thread()
		.worker_threads(1)
		.enable_all()
		.build()
		.unwrap();

	rt.block_on(main_proc());
}

async fn main_proc() {
	let (broadcast, _) = tokio::sync::broadcast::channel(65536);

	let shared = Shared {
		broadcast,
		message_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
		message_sent: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0))
	};

	tokio::spawn(listen(shared.clone()));

	let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
	let mut report_no = 0usize;
	loop {
		interval.tick().await;

		println!("{} : {} message recved, {} message sent", report_no, shared.message_count.load(std::sync::atomic::Ordering::Relaxed), shared.message_sent.load(std::sync::atomic::Ordering::Relaxed), );
		report_no += 1;
	}
}
async fn listen(shared: Shared) {
	let listener = tokio::net::TcpListener::bind("0.0.0.0:9800").await.unwrap();
	let mut conn_no = 0usize;

	let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

	loop {
		tokio::select! {
			acpt = listener.accept() => {
				if let Ok(sock) = acpt {
					tokio::spawn(process(conn_no, shared.clone(), sock.1, sock.0));
					conn_no += 1;
				}
			}
			_ = interval.tick() => {
				println!("accept len = {}", conn_no);
			}
		}
	}
}

async fn process(conn_no: usize, shared: Shared, addr: std::net::SocketAddr, mut sock: tokio::net::TcpStream) {
	use tokio::io::*;
	use bytes::*;

	let mut rx = shared.broadcast.subscribe();
	let tx = shared.broadcast;
	let mut read_buf = BytesMut::with_capacity(16384);
	let mut write_buf = BytesMut::with_capacity(32768);
	let mut box_size = 0usize;

	sock.set_nodelay(true);

	write_buf.put_slice(&format!("{}", addr).into_bytes());
	sock.write_u64(write_buf.len() as u64);
	sock.write_buf(&mut write_buf);

	loop {
		tokio::select! {
			Ok(Message::broadcast(from, data)) = rx.recv() => {
				if from == conn_no { continue; }

				write_buf.put_u64(data.len() as u64);
				write_buf.extend_from_slice(&data);

				sock.write_buf(&mut write_buf).await;
				shared.message_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
			res = sock.read_buf(&mut read_buf) => {
				if res.is_err() || res.unwrap() == 0 { return; }
				'inner: loop {
					if read_buf.len() == 0 { break 'inner; }
					if box_size == 0 { 
						if read_buf.len() < 8 { break 'inner; }
						box_size = read_buf.get_u64() as usize;
					}
					
					if read_buf.len() < box_size { break 'inner }
					let msg = read_buf.split_to(box_size).freeze();

					box_size = 0;
					
	
					tx.send(Message::broadcast(conn_no, msg));
					shared.message_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				}
			}
			else => {}
		}

		write_buf.reserve(16348);
		read_buf.reserve(32786);
	}
}