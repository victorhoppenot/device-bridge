use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{Path, WebSocketUpgrade, ws::{Message, WebSocket}},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use rdkafka::{
    ClientConfig, Message as _,
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
use serde_json::{Value, json};

use crate::Connection::{ConsumerConnection, ProducerConnection};

const PRODUCE_TIMEOUT: Duration = Duration::from_secs(5);

enum Connection {
    ProducerConnection(FutureProducer),
    ConsumerConnection(StreamConsumer),
}

#[tokio::main]
async fn main() {
    let bind_addr = env::var("BRIDGE_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    let app = Router::new().route("/bridge/:subject", get(ws_handler));
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|err| panic!("failed to bind {bind_addr}: {err}"));

    println!("device-bridge listening on {bind_addr}");
    axum::serve(listener, app).await.unwrap();
}

fn broker_url() -> String {
    env::var("KAFKA_BROKER_URL").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn unique_group_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("bridge-{nanos:x}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

fn error_frame(message: impl Into<String>) -> Value {
    json!({ "status": "error", "message": message.into() })
}

async fn ws_handler(Path(subject): Path<String>, ws: WebSocketUpgrade) -> Response {
    match subject.as_str() {
        "producer" => {
            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", broker_url())
                .set("message.timeout.ms", "30000");

            match config.create::<FutureProducer>() {
                Ok(producer) => {
                    ws.on_upgrade(move |socket| handle_socket(socket, ProducerConnection(producer)))
                }
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unable to create producer: {err}"),
                )
                    .into_response(),
            }
        }
        "consumer" => {
            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", broker_url())
                .set("auto.offset.reset", "earliest")
                .set("group.id", unique_group_id())
                // Offsets are committed by hand, once a record is actually on the wire.
                .set("enable.auto.commit", "false");

            match config.create::<StreamConsumer>() {
                Ok(consumer) => {
                    ws.on_upgrade(move |socket| handle_socket(socket, ConsumerConnection(consumer)))
                }
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unable to create consumer: {err}"),
                )
                    .into_response(),
            }
        }
        _ => (
            StatusCode::NOT_FOUND,
            "Invalid subject; expected 'producer' or 'consumer'.",
        )
            .into_response(),
    }
}

async fn handle_socket(socket: WebSocket, connection: Connection) {
    match connection {
        ProducerConnection(producer) => handle_producer(socket, producer).await,
        ConsumerConnection(consumer) => handle_consumer(socket, consumer).await,
    }
}

async fn handle_producer(mut socket: WebSocket, producer: FutureProducer) {
    let mut topic: Option<String> = None;
    let mut key: Option<Vec<u8>> = None;

    while let Some(frame) = socket.recv().await {
        let message = match frame {
            Ok(message) => message,
            Err(err) => {
                eprintln!("producer socket error: {err}");
                return;
            }
        };

        let reply = match message {
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return,

            Message::Text(text) => {
                if topic.is_none() && key.is_none() {
                    topic = Some(text);
                    continue;
                }
                topic = None;
                key = None;
                error_frame("Unexpected topic frame; a record was already in progress.")
            }
            Message::Binary(bin) => match (topic.take(), key.take()) {
                (None, _) => {
                    error_frame("Expected Text(topic) before Binary(key), Binary(payload).")
                }
                (Some(pending_topic), None) => {
                    topic = Some(pending_topic);
                    key = Some(bin);
                    continue;
                }
                (Some(record_topic), Some(record_key)) => {
                    let record = FutureRecord::to(&record_topic)
                        .key(&record_key)
                        .payload(&bin);

                    match producer.send(record, PRODUCE_TIMEOUT).await {
                        Ok((partition, offset)) => json!({
                            "status": "partition_response",
                            "topic": record_topic,
                            "partition": partition,
                            "offset": offset,
                        }),
                        Err((kafka_error, _original_record)) => {
                            error_frame(kafka_error.to_string())
                        }
                    }
                }
            },
        };

        if socket.send(Message::Text(reply.to_string())).await.is_err() {
            return;
        }
    }
}

async fn handle_consumer(socket: WebSocket, consumer: StreamConsumer) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    let subscription = loop {
        match ws_rx.next().await {
            Some(Ok(Message::Text(text))) => break text,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => return,
        }
    };

    let topics: Vec<&str> = subscription
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .collect();

    if topics.is_empty() {
        let reply = error_frame("Expected a comma-separated topic list as the first frame.");
        let _ = ws_tx.send(Message::Text(reply.to_string())).await;
        return;
    }

    if let Err(err) = consumer.subscribe(&topics) {
        let reply = error_frame(format!("Unable to subscribe: {err}"));
        let _ = ws_tx.send(Message::Text(reply.to_string())).await;
        return;
    }

    let ack = json!({ "status": "subscribed", "topics": topics });
    if ws_tx.send(Message::Text(ack.to_string())).await.is_err() {
        return;
    }

    loop {
        let message = tokio::select! {
            result = consumer.recv() => match result {
                Ok(message) => message,
                Err(err) => {
                    let reply = error_frame(err.to_string());
                    let _ = ws_tx.send(Message::Text(reply.to_string())).await;
                    return;
                }
            },
            frame = ws_rx.next() => match frame {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                _ => return,
            },
        };

        let header = json!({
            "status": "consumer",
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
        });
        let key = message.key().map(<[u8]>::to_vec).unwrap_or_default();
        let payload = message.payload().map(<[u8]>::to_vec).unwrap_or_default();
        if ws_tx.send(Message::Text(header.to_string())).await.is_err()
            || ws_tx.send(Message::Binary(key)).await.is_err()
            || ws_tx.send(Message::Binary(payload)).await.is_err()
        {
            return;
        }

        if let Err(err) = consumer.commit_message(&message, CommitMode::Async) {
            eprintln!("failed to commit offset: {err}");
        }
    }
}
