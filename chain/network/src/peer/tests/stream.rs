use crate::actix::ActixSystem;
use crate::network_protocol::testonly as data;
use crate::peer::peer_actor::{NETWORK_MESSAGE_MAX_SIZE_BYTES, RECOMMENDED_HARDWARE_MEMORY};
use crate::peer::stream;
use crate::tcp;
use crate::testonly::make_rng;
use crate::traffic::rate_limiter::{RateLimiter, RateLimiterConfig};
use actix::Actor as _;
use actix::ActorContext as _;
use near_async::time;
use near_primitives::network::PeerId;
use rand::Rng;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;

/// A writer on tcp stream used only for testing.
/// Bypasses limitations imposed by FramedStream for testing purposes.
/// note: Malicious senders do not need to adhere to these limitations from the sender's perspective
/// However, honest receivers will adhere to limitations imposed from receiver's perspective so we only test our default receiver's implementation.
/// - Size of sent messages set using crate::peer::stream::MAX_WRITE_BUFFER_CAPACITY_BYTES
/// - Sending Rate of messages limited by run_send_loop period
/// - Ability to send parts of a messages to exceed timeouts
// For concurrent tests, you'll need to start up multiple TestWriterTcpStream as each can only handle 1 stream.
struct TestWriterTcpStream {
    tcp_send_buf_wrifer: tokio::io::BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>,
    clock: Arc<time::Clock>,
}

impl TestWriterTcpStream {
    // write 1 message
    async fn write_one_immediately(&mut self, frame: stream::Frame) -> io::Result<()> {
        let message: Vec<u8> = frame.0;
        self.tcp_send_buf_wrifer.write_u32_le(message.len() as u32).await?;
        self.tcp_send_buf_wrifer.write_all(&message[..]).await?;
        self.tcp_send_buf_wrifer.flush().await?;
        Ok(())
    }

    // Write a frame without completely writing it
    // Can only write one incomplete frame instead of a vector of frames to a channel.
    // because the receive side of the tcp stream will not resume without a complete message.
    async fn write_one_without_completion(&mut self, frame: stream::Frame) -> io::Result<()> {
        let mut message: Vec<u8> = frame.0.clone();
        let message_len = message.len();
        self.tcp_send_buf_wrifer.write_u32_le(message_len as u32).await?;
        message.truncate(message_len - 1); // shorten each message by 1 and send the shorten message
        self.tcp_send_buf_wrifer.write_all(&message[..]).await?;
        self.tcp_send_buf_wrifer.flush().await?;
        Ok(())
    }

    async fn write_one_with_delay(
        &mut self,
        frame: stream::Frame,
        pause_duration: time::Duration,
    ) -> io::Result<()> {
        let message: Vec<u8> = frame.0.clone();
        self.tcp_send_buf_wrifer.write_u32_le(message.len() as u32).await?;
        self.clock.sleep(pause_duration).await; // note: Sleeping with a fake clock will require another thread to advance the clock
        self.tcp_send_buf_wrifer.write_all(&message[..]).await?;
        self.tcp_send_buf_wrifer.flush().await?;
        Ok(())
    }

    // Write all the messages as fast as possible
    async fn write_all_immediately(&mut self, frames: Vec<stream::Frame>) -> io::Result<()> {
        for frame in frames {
            let message: Vec<u8> = frame.0;
            self.tcp_send_buf_wrifer.write_u32_le(message.len() as u32).await?;
            self.tcp_send_buf_wrifer.write_all(&message[..]).await?;
            self.tcp_send_buf_wrifer.flush().await?;
        }
        Ok(())
    }
}

struct Actor {
    framed_stream: stream::FramedStream<Actor>,
    queue_send: mpsc::UnboundedSender<stream::Frame>,
}

impl actix::Actor for Actor {
    type Context = actix::Context<Actor>;
}

#[derive(actix::Message)]
#[rtype("()")]
struct SendFrame(stream::Frame);

// This is called in the send path
// SendFrame goes through FrameStreamActor and the send endpoint of tcp stream
impl actix::Handler<SendFrame> for Actor {
    type Result = ();
    fn handle(&mut self, SendFrame(frame): SendFrame, _ctx: &mut Self::Context) {
        self.framed_stream.send(frame);
    }
}

// This is called in the receive path
// Frames received by FrameSreamActor gets forwarded into queue_send by sending a stream::Frame message to this actor
impl actix::Handler<stream::Frame> for Actor {
    type Result = ();
    fn handle(&mut self, frame: stream::Frame, _ctx: &mut Self::Context) {
        self.queue_send.send(frame).ok().unwrap();
    }
}

impl actix::Handler<stream::Error> for Actor {
    type Result = ();
    fn handle(&mut self, _err: stream::Error, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<stream::Frame>,
    system: ActixSystem<Actor>,
}

impl Actor {
    async fn spawn(
        tcp_stream: tcp::Stream,
        recv_rate_limiter: Arc<RateLimiter>,
        recv_timeout: time::Duration,
    ) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        Handler {
            queue_recv,
            system: ActixSystem::spawn(move || {
                Actor::create(|ctx| {
                    let framed_stream = stream::FramedStream::spawn(
                        ctx,
                        tcp_stream,
                        Arc::default(),
                        recv_rate_limiter,
                        recv_timeout,
                    );
                    Self { framed_stream, queue_send }
                })
            })
            .await,
        }
    }
}

pub(crate) fn generate_frame_message(message_size: usize, seed: u64) -> stream::Frame {
    let mut rng = make_rng(seed);
    let range = std::ops::Range { start: message_size, end: message_size + 1 };
    let size = rng.gen_range(range);
    let mut msg = vec![0; size.try_into().unwrap()];
    rng.fill(&mut msg[..]);
    let frame = stream::Frame(msg);
    return frame;
}

pub(crate) fn generate_frame_messages(
    number_of_messages: usize,
    range: std::ops::Range<usize>,
    seed: u64,
) -> Vec<stream::Frame> {
    let mut rng = make_rng(seed);
    let msgs: Vec<_> = (0..number_of_messages)
        .map(|_| {
            let size = rng.gen_range(range.clone());
            let mut msg = vec![0; size.try_into().unwrap()];
            rng.fill(&mut msg[..]);
            stream::Frame(msg)
        })
        .collect();
    return msgs;
}

pub(crate) fn build_bypass_rate_limiter(clock: Arc<time::Clock>) -> Arc<RateLimiter> {
    // Maximum configurations to prevent rate limiter from influencing tests that are not testing rate limiter functionality
    let capacity: usize = NETWORK_MESSAGE_MAX_SIZE_BYTES;
    let refill_amount: usize = NETWORK_MESSAGE_MAX_SIZE_BYTES;
    let refill_period: time::Duration = time::Duration::milliseconds(1);
    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter = Arc::new(RateLimiter::new(rate_limiter_config, clock));
    return recv_rate_limiter;
}

async fn build_loopback_tcp_with_test_writer(
    clock: Arc<time::Clock>,
    recv_rate_limiter: Arc<RateLimiter>,
    recv_timeout: time::Duration,
) -> (Handler, TestWriterTcpStream) {
    let seed: u64 = 98324532;
    let mut rng = make_rng(seed);
    let peer_id: PeerId = data::make_peer_id(&mut rng);
    let tier: tcp::Tier = tcp::Tier::T2;
    let (send_endpoint, receive_endpoint) = tcp::Stream::loopback(peer_id, tier).await;
    let receiver_process =
        Actor::spawn(receive_endpoint, recv_rate_limiter.clone(), recv_timeout).await;
    let (_, tcp_send) = tokio::io::split(send_endpoint.stream);
    const WRITE_BUFFER_CAPACITY: usize = 8 * 1024; // Default size: https://github.com/rust-lang/rust/blob/08fd6f719ee764cf62659ddf481e22dbfe4b8894/library/std/src/sys_common/io.rs#L3
    let tcp_send_buf_writer = tokio::io::BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, tcp_send);
    let sender_test_writer =
        TestWriterTcpStream { tcp_send_buf_wrifer: tcp_send_buf_writer, clock: clock };
    return (receiver_process, sender_test_writer);
}

#[tokio::test]
async fn recv_only() {
    let recv_timeout = time::Duration::seconds(10);
    let fake_clock = time::FakeClock::default();
    let recv_rate_limiter = build_bypass_rate_limiter(Arc::new(fake_clock.clock()));
    let (mut receiver_process, mut sender_test_writer) = build_loopback_tcp_with_test_writer(
        Arc::new(fake_clock.clock()),
        recv_rate_limiter,
        recv_timeout,
    )
    .await;

    let seed: u64 = 98324532;
    let number_of_messages = 10;
    let message_size_constant_range = std::ops::Range { start: 10, end: 11 };
    let msgs = generate_frame_messages(number_of_messages, message_size_constant_range, seed);

    // Test messages are properly received via FramedStreamActor over tcp.
    sender_test_writer.write_all_immediately(msgs.clone()).await.unwrap();
    for expected in &msgs {
        let received = receiver_process.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }
}

#[tokio::test]
async fn send_recv() {
    let seed: u64 = 98324532;
    let mut rng = make_rng(seed);
    let (send_endpoint, receive_endpoint) =
        tcp::Stream::loopback(data::make_peer_id(&mut rng), tcp::Tier::T2).await;
    let recv_timeout = time::Duration::seconds(10);
    let fake_clock = time::FakeClock::default();
    let recv_rate_limiter = build_bypass_rate_limiter(Arc::new(fake_clock.clock()));
    let mut receiver_process =
        Actor::spawn(receive_endpoint, recv_rate_limiter.clone(), recv_timeout).await;
    let message_size_range = std::ops::Range { start: 1, end: 10 };
    let msgs = generate_frame_messages(5, message_size_range, seed);

    // Tests sent messages are properly sent and received via FramedStream Actor over tcp.
    let sender_process = Actor::spawn(send_endpoint, recv_rate_limiter.clone(), recv_timeout).await;
    for msg in &msgs {
        sender_process.system.addr.send(SendFrame(msg.clone())).await.unwrap();
    }
    for expected in &msgs {
        let actual = receiver_process.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &actual);
    }
}

#[tokio::test]
async fn latency_timeout_test() {
    // Need to use real clock as timeout by tokio uses real clock
    // Hence, use a low delay of 100 ms for this test
    let recv_timeout = time::Duration::milliseconds(100);
    let test_timeout: time::Duration = recv_timeout * 2.0;
    let seed: u64 = 98324532;
    let real_clock = Arc::new(time::Clock::real());
    let recv_rate_limiter = build_bypass_rate_limiter(real_clock.clone());
    let (mut receiver_process, mut sender_test_writer) =
        build_loopback_tcp_with_test_writer(real_clock.clone(), recv_rate_limiter, recv_timeout)
            .await;

    // Generate 1 message with size 2 bytes
    let message_size = 2;
    let one_msg = generate_frame_message(message_size, seed);

    // Writing with completion succeeds
    sender_test_writer.write_one_immediately(one_msg.clone()).await.unwrap();
    let received = receiver_process.queue_recv.recv().await.unwrap();
    assert_eq!(one_msg, received);

    // Writing without completion should timeout on the receiver end
    sender_test_writer.write_one_without_completion(one_msg.clone()).await.unwrap();
    let result =
        tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process.queue_recv.recv()).await;
    assert!(result.unwrap().is_none()); // Timeout error occurs

    // Writing with completion but exceeding timeout should timeout on the receiver end and not parse the message
    sender_test_writer.write_one_with_delay(one_msg.clone(), test_timeout).await.unwrap();
    let result =
        tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process.queue_recv.recv()).await;
    assert!(result.unwrap().is_none()); // Timeout error occurs
}

#[tokio::test]
async fn tps_rate_limiter() {
    let seed: u64 = 98324532;
    let recv_timeout = time::Duration::seconds(1); // shouldn't affect test with fake clock, used by tokio timeout which must use real clock
    let test_timeout = time::Duration::microseconds(10);
    let fake_clock = time::FakeClock::default();

    // Configure token bucket rate
    let capacity: usize = 100;
    let refill_period: time::Duration = time::Duration::milliseconds(1);
    let refill_amount: usize = 10;

    let number_of_messages = capacity / refill_amount;
    let message_size = refill_amount; // Use a constant size equivalent to refill_amount to simplify test
    let message_size_constant_range =
        std::ops::Range { start: message_size, end: message_size + 1 };

    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter =
        Arc::new(RateLimiter::new(rate_limiter_config, Arc::new(fake_clock.clock())));
    let (mut receiver_process, mut sender_test_writer) = build_loopback_tcp_with_test_writer(
        Arc::new(fake_clock.clock()),
        recv_rate_limiter,
        recv_timeout,
    )
    .await;

    let msgs = generate_frame_messages(number_of_messages, message_size_constant_range, seed);

    // Writing instantly everything till it uses all burst capacity
    // Should pass since it doesn't exceed capacity
    sender_test_writer.write_all_immediately(msgs.clone()).await.unwrap();
    for expected in &msgs {
        let received = receiver_process.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }

    // Immediately (without advancing the clock), writing an additional message should trigger a receive timeout.
    let one_msg = generate_frame_message(message_size, seed);
    sender_test_writer.write_one_immediately(one_msg.clone()).await.unwrap();
    let result =
        tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process.queue_recv.recv()).await;
    assert!(result.is_err()); // timeout occurs as the message has not acquired tokens

    // Rate limiter will now admit the message that was previously sent without having to send it again
    fake_clock.advance(refill_period);
    let received = receiver_process.queue_recv.recv().await.unwrap();
    assert_eq!(one_msg, received);
}

#[tokio::test]
async fn rate_limiter_concurrent_real_clock() {
    let seed: u64 = 98324532;
    let recv_timeout = time::Duration::seconds(1);
    let test_timeout = time::Duration::microseconds(10);
    // Concurrent tests must use real clock as the fake clock doesn't get properly woken up with sleep calls
    let real_clock = Arc::new(time::Clock::real());

    // Configure token bucket rate
    let capacity: usize = 100;
    let refill_period: time::Duration = time::Duration::seconds(100); // long enough to not refill during test itself
    let refill_amount: usize = 10;

    let number_of_messages = capacity / refill_amount;
    let message_size = refill_amount; // Use a constant size equivalent to refill_amount to simplify test
    let message_size_constant_range =
        std::ops::Range { start: message_size, end: message_size + 1 };

    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter = Arc::new(RateLimiter::new(rate_limiter_config, real_clock.clone()));

    // Build 2 concurrent nodes, both using same rate limiter
    let (mut receiver_process_a, mut sender_test_writer_a) = build_loopback_tcp_with_test_writer(
        real_clock.clone(),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;
    let (mut receiver_process_b, mut sender_test_writer_b) = build_loopback_tcp_with_test_writer(
        real_clock.clone(),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;

    let msgs_a =
        generate_frame_messages(number_of_messages / 2, message_size_constant_range.clone(), seed);
    let msgs_b =
        generate_frame_messages(number_of_messages / 2, message_size_constant_range.clone(), seed);

    // Writing instantly everything till it uses all burst capacity, but with 2 threads instead of 1
    sender_test_writer_a.write_all_immediately(msgs_a.clone()).await.unwrap();
    sender_test_writer_b.write_all_immediately(msgs_b.clone()).await.unwrap();
    for expected in &msgs_a {
        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }
    for expected in &msgs_b {
        let received = receiver_process_b.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }

    // Immediately (without advancing the clock), writing an additional message should fail the rate limiter
    let one_msg = generate_frame_message(message_size, seed);
    // Must write a to completion before writing b to guarantee a will have first access to rate limiter lock
    sender_test_writer_a.write_one_immediately(one_msg.clone()).await.unwrap();
    sender_test_writer_b.write_one_immediately(one_msg.clone()).await.unwrap();

    let result =
        tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process_a.queue_recv.recv())
            .await;
    assert!(result.is_err()); // timeout occurs as the message has not acquired tokens
    let result =
        tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process_b.queue_recv.recv())
            .await;
    assert!(result.is_err()); // timeout occurs as the message has not acquired tokens
}

#[tokio::test]
async fn rate_limiter_concurrent_fairness() {
    let seed: u64 = 98324532;
    let recv_timeout = time::Duration::seconds(1); // shouldn't affect test with fake clock, used by tokio timeout which must use real clock
    let test_timeout = time::Duration::microseconds(10); // will actually wait this duration to test for non-received messages
    let fake_clock = time::FakeClock::default();

    // Configure token bucket rate
    let capacity: usize = 100;
    let refill_period: time::Duration = time::Duration::milliseconds(1);
    let refill_amount: usize = 10;

    let number_of_messages = capacity / refill_amount;
    let message_size = refill_amount; // Use a constant size equivalent to refill_amount to simplify test
    let message_size_constant_range =
        std::ops::Range { start: message_size, end: message_size + 1 };

    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter =
        Arc::new(RateLimiter::new(rate_limiter_config, Arc::new(fake_clock.clock())));

    // Build 2 concurrent nodes, both using same rate limiter
    let (mut receiver_process_a, mut sender_test_writer_a) = build_loopback_tcp_with_test_writer(
        Arc::new(fake_clock.clock()),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;
    let (mut receiver_process_b, mut sender_test_writer_b) = build_loopback_tcp_with_test_writer(
        Arc::new(fake_clock.clock()),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;

    let msgs_a =
        generate_frame_messages(number_of_messages / 2, message_size_constant_range.clone(), seed);
    let msgs_b =
        generate_frame_messages(number_of_messages / 2, message_size_constant_range.clone(), seed);

    sender_test_writer_a.write_all_immediately(msgs_a.clone()).await.unwrap();
    sender_test_writer_b.write_all_immediately(msgs_b.clone()).await.unwrap();
    for expected in &msgs_a {
        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }
    for expected in &msgs_b {
        let received = receiver_process_b.queue_recv.recv().await.unwrap();
        assert_eq!(expected, &received);
    }

    // Test fairness is based on first come first serve
    // Repeat test a finite number of times to be sure, it runs on a fake clock so it's extremely quick with no actual delays
    for _i in 0..20 {
        // Advance to fit 2 messages
        fake_clock.advance(refill_period);
        fake_clock.advance(refill_period);

        // Write 3 messages in order a -> b -> a and ensure the last messsages always fail
        // Check the last messages always fail and the first 2 messages always succeed
        let one_msg = generate_frame_message(message_size, seed);
        let one_msg_fails = generate_frame_message(message_size, seed);
        sender_test_writer_a.write_one_immediately(one_msg.clone()).await.unwrap();
        sender_test_writer_b.write_one_immediately(one_msg.clone()).await.unwrap();

        // To guarantee his is the last message sent without any race conditions, it needs to be sent after b has been received
        let received = receiver_process_b.queue_recv.recv().await.unwrap();
        assert_eq!(one_msg, received);

        sender_test_writer_a.write_one_immediately(one_msg_fails.clone()).await.unwrap();

        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(one_msg, received);
        // last message should fail to send always
        let result =
            tokio::time::timeout(test_timeout.unsigned_abs(), receiver_process_a.queue_recv.recv())
                .await;
        assert!(result.is_err()); // timeout occurs as the message has not acquired tokens

        // Rate limiter will now admit the message that was previously sent without having to send it again
        fake_clock.advance(refill_period);
        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(one_msg_fails, received);
    }
}

#[tokio::test]
async fn rate_limiter_average_tps_concurrent() {
    let seed: u64 = 98324532;
    let recv_timeout = time::Duration::seconds(1); // should not affect this test
                                                   // need use real clock to test tps generation
    let real_clock = Arc::new(time::Clock::real());

    // Configure token bucket rate
    let capacity: usize = 20;
    let refill_period: time::Duration = time::Duration::microseconds(50);
    let refill_amount: usize = 10;

    let message_size = refill_amount / 2; // Divided by 2 since there are 2 senders sending at the refill_period

    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter = Arc::new(RateLimiter::new(rate_limiter_config, real_clock.clone()));

    // Build 2 concurrent nodes, both using same rate limiter
    let (mut receiver_process_a, mut sender_test_writer_a) = build_loopback_tcp_with_test_writer(
        real_clock.clone(),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;
    let (mut receiver_process_b, mut sender_test_writer_b) = build_loopback_tcp_with_test_writer(
        real_clock.clone(),
        recv_rate_limiter.clone(),
        recv_timeout,
    )
    .await;

    let msg_a = generate_frame_message(message_size, seed);
    let msg_b = generate_frame_message(message_size, seed);

    // Each taking turns to send
    for _i in 0..10 {
        sender_test_writer_a.write_one_immediately(msg_a.clone()).await.unwrap();
        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(msg_a, received);
        real_clock.sleep(refill_period / 2).await;
        sender_test_writer_b.write_one_immediately(msg_b.clone()).await.unwrap();
        let received = receiver_process_b.queue_recv.recv().await.unwrap();
        assert_eq!(msg_b, received);
        real_clock.sleep(refill_period / 2).await;
    }
    real_clock.sleep(refill_period / 2).await;
    // Each sending with a burst
    for _i in 0..10 {
        sender_test_writer_a.write_one_immediately(msg_a.clone()).await.unwrap();
        sender_test_writer_b.write_one_immediately(msg_b.clone()).await.unwrap();
        let received = receiver_process_a.queue_recv.recv().await.unwrap();
        assert_eq!(msg_a, received);
        let received = receiver_process_b.queue_recv.recv().await.unwrap();
        assert_eq!(msg_b, received);
        real_clock.sleep(refill_period).await;
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)] // Takes about 100 seconds to run
async fn neap11_attack_tests() {
    // Need to use real clock as timeout by tokio uses real clock
    let seed: u64 = 98324532;
    let recv_timeout = time::Duration::seconds(2);
    let real_clock = Arc::new(time::Clock::real());

    let largest_allowable_message_size = NETWORK_MESSAGE_MAX_SIZE_BYTES;
    let largest_allowable_message = generate_frame_message(largest_allowable_message_size, seed);

    // Configure token bucket rate
    let capacity: usize = NETWORK_MESSAGE_MAX_SIZE_BYTES;
    let refill_period: time::Duration = time::Duration::seconds(2);
    let refill_amount: usize = NETWORK_MESSAGE_MAX_SIZE_BYTES;
    let rate_limiter_config: RateLimiterConfig = RateLimiterConfig {
        capacity: capacity,
        refill_period: refill_period,
        refill_amount: refill_amount,
    };
    let recv_rate_limiter = Arc::new(RateLimiter::new(rate_limiter_config, real_clock.clone()));

    // Spawn many concurrent connections using the same rate limiter
    // ceiling(24 GB / 512MB) = 47, 24GB is the recommended, takes about 100 seconds to run
    let number_of_concurrent_calls: usize = RECOMMENDED_HARDWARE_MEMORY
        / NETWORK_MESSAGE_MAX_SIZE_BYTES
        + usize::from(RECOMMENDED_HARDWARE_MEMORY % NETWORK_MESSAGE_MAX_SIZE_BYTES == 0);
    for _i in 0..number_of_concurrent_calls {
        let (_receiver_process, mut sender_test_writer) = build_loopback_tcp_with_test_writer(
            real_clock.clone(),
            recv_rate_limiter.clone(),
            recv_timeout,
        )
        .await;
        // Writing without completion should timeout on the receiver end
        sender_test_writer
            .write_one_without_completion(largest_allowable_message.clone())
            .await
            .unwrap();
    }
    // the fact that your computer's memory with <= 24GB RAM doesn't blow up proves this test passes
}
