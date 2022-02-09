#[macro_use]
extern crate lazy_static;

#[derive(Eq, PartialEq, Clone, Debug)]
enum State {
	init,
	wait_for_connect,
	run,
	end
}

lazy_static! {
	static ref CONNECT_ADDR: String = "49.247.34.176:9800".to_string();
	static ref WRITE_CNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
	static ref READ_CNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
	static ref ADDITONAL_CNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
}

#[tokio::main]
async fn main() {
	let (notifier_tx, notifier_rx) = tokio::sync::watch::channel(State::init);

	let mut step = 0usize;
	for _ in 0..22000 {
		tokio::spawn(process(step, notifier_rx.clone()));
		
		if step % 100 == 0 {
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
		step += 1;
	}

	notifier_tx.send(State::wait_for_connect);
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	notifier_tx.send(State::run);
	let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
	let mut repeat = 0usize;

	while repeat < 180 {
		interval.tick().await;

		println!("{}: read {}, write {}", repeat, READ_CNT.load(std::sync::atomic::Ordering::Relaxed), WRITE_CNT.load(std::sync::atomic::Ordering::Relaxed));
		repeat += 1;
	}

	notifier_tx.send(State::end);
}


async fn process(no: usize, mut rx: tokio::sync::watch::Receiver<State>) {
	let mut interval = tokio::time::interval(std::time::Duration::from_millis(80));
	let mut last_state  = State::init;

	let mut readbuf = bytes::BytesMut::with_capacity(87380);
	let mut sock = tokio::net::TcpStream::connect(&*CONNECT_ADDR).await.unwrap();
	sock.set_nodelay(true); // 패킷이 합쳐지는것을 막기위해 no_delay flag 설정
	use tokio::io::*;

	loop {
		tokio::select! {
			_ = rx.changed() => {
				match *rx.borrow() {
					State::run => last_state = State::run,
					State::end => break,
					_ => ()
				}
			}
			_ = interval.tick(), if last_state == State::run => {
				use tokio::io::*;
				let random_len = rand::random::<usize>() % 127; // MTU에 맞추려고.. 1400에 가장 가까운 소수 선택
				let random_bytes: Vec<u8> = (0..random_len).map(|_| { rand::random::<u8>() }).collect();

				sock.write_u64(random_len as u64).await;
				WRITE_CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				
				if let Err(e) = sock.write(&random_bytes).await {
					println!("write err {:?}", e);
					return
				}
			}
			res = sock.read_buf(&mut readbuf) => {
				READ_CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

				if res.is_err() || res.unwrap() == 0 { return }
				readbuf.clear();

				println!("{}", no);
			}
		}
	}
}
