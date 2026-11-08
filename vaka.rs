use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD, Engine};
use clap::Parser;
use eyre::{Result, WrapErr};
use jito_protos::bundle::{bundle_result::Result as BR, Bundle};
use jito_protos::convert::proto_packet_from_versioned_tx;
use jito_protos::searcher::{SendBundleRequest, SubscribeBundleResultsRequest};
use jito_searcher_client::get_searcher_client_auth;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::transaction::VersionedTransaction;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(long, env = "KAFKA_REQUESTS_TOPIC")]
    requests_topic: String,
    #[arg(long, env = "KAFKA_ACKS_TOPIC")]
    acks_topic: String,
    #[arg(long, env = "KAFKA_RESULTS_TOPIC")]
    results_topic: String,
    #[arg(long, env = "KAFKA_STATS_TOPIC")]
    stats_topic: String,
    #[arg(long, env = "BLOCK_ENGINE_URL")]
    block_engine_url: String,
    #[arg(long, env = "KEYPAIR_PATH")]
    keypair_path: String,
}

#[derive(Deserialize)]
struct BundleRequest {
    txs: Vec<String>,
}

#[derive(Serialize)]
struct Event {
    bundle_id: String,
    kind: &'static str,
    slot: Option<u64>,
    validator: Option<String>,
    reason: Option<String>,
    received_at_ms: u64,
}

#[derive(Default)]
struct Counts {
    sent: u64,
    landed: u64,
    dropped: u64,
    outstanding: HashSet<String>,
}

impl Counts {
    fn observe(&mut self, ev: &Event) {
        let landed = matches!(ev.kind, "processed" | "finalized");
        let dropped = matches!(ev.kind, "dropped" | "rejected");
        if (landed || dropped) && self.outstanding.remove(&ev.bundle_id) {
            self.landed += landed as u64;
            self.dropped += dropped as u64;
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let settled = self.landed + self.dropped;
        json!({
            "event": "land_rate",
            "sent": self.sent,
            "landed": self.landed,
            "dropped": self.dropped,
            "pending": self.outstanding.len(),
            "land_rate": (settled > 0).then(|| self.landed as f64 / settled as f64),
            "at_ms": now_ms(),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn to_event(bundle_id: String, result: BR) -> Event {
    let (kind, slot, validator, reason) = match result {
        BR::Accepted(a) => ("accepted", Some(a.slot), Some(a.validator_identity), None),
        BR::Rejected(r) => ("rejected", None, None, Some(format!("{:?}", r.reason))),
        BR::Processed(p) => ("processed", Some(p.slot), Some(p.validator_identity), None),
        BR::Finalized(_) => ("finalized", None, None, None),
        BR::Dropped(d) => ("dropped", None, None, Some(d.reason.to_string())),
    };
    Event {
        bundle_id,
        kind,
        slot,
        validator,
        reason,
        received_at_ms: now_ms(),
    }
}

async fn publish(producer: &FutureProducer, topic: &str, key: &str, body: &impl Serialize) {
    let payload = match serde_json::to_string(body) {
        Ok(p) => p,
        Err(e) => return error!(err = ?e, topic, "serialize failed"),
    };
    let record = FutureRecord::to(topic).key(key).payload(&payload);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        error!(key, topic, err = ?e, "kafka produce failed");
    }
}

fn build_request(raw: &[u8]) -> Result<SendBundleRequest> {
    let req: BundleRequest = serde_json::from_slice(raw).wrap_err("request json")?;
    let packets = req
        .txs
        .iter()
        .map(|t| {
            let bytes = STANDARD.decode(t).wrap_err("base64")?;
            let tx: VersionedTransaction = bincode::deserialize(&bytes).wrap_err("bincode")?;
            Ok(proto_packet_from_versioned_tx(&tx))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SendBundleRequest {
        bundle: Some(Bundle {
            header: None,
            packets,
        }),
    })
}

async fn session(
    args: &Args,
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    tally: &mut Counts,
) -> Result<()> {
    let keypair = read_keypair_file(&args.keypair_path).map_err(|e| eyre::eyre!("keypair: {e}"))?;
    let keypair = Arc::new(keypair);
    let mut client = get_searcher_client_auth(&args.block_engine_url, &keypair)
        .await
        .wrap_err("searcher auth handshake")?;
    let mut stream = client
        .subscribe_bundle_results(SubscribeBundleResultsRequest {})
        .await
        .wrap_err("subscribe_bundle_results")?
        .into_inner();
    info!("subscribed to bundle results");
    let mut ticker = interval(Duration::from_secs(60));
    ticker.tick().await;

    loop {
        tokio::select! {
            msg = stream.message() => {
                let Some(msg) = msg.wrap_err("stream error")? else { return Ok(()) };
                let Some(result) = msg.result else { continue };
                let ev = to_event(msg.bundle_id, result);
                tally.observe(&ev);
                publish(producer, &args.results_topic, &ev.bundle_id, &ev).await;
            }
            msg = consumer.recv() => {
                let msg = msg.wrap_err("kafka consume")?;
                let key = msg.key().map_or_else(|| format!("{}:{}", msg.partition(), msg.offset()), |k| String::from_utf8_lossy(k).into_owned());
                let built = msg.payload().ok_or_else(|| eyre::eyre!("no payload")).and_then(build_request);
                let sent = async { client.send_bundle(built?).await.wrap_err("send_bundle") }.await;
                match sent {
                    Ok(resp) => {
                        let uuid = resp.into_inner().uuid;
                        tally.sent += 1;
                        tally.outstanding.insert(uuid.clone());
                        let ack = json!({ "request_key": key, "uuid": uuid });
                        publish(producer, &args.acks_topic, &key, &ack).await;
                    }
                    Err(e) => error!(%key, err = ?e, "bundle request failed"),
                }
            }
            _ = ticker.tick() => {
                let snap = tally.snapshot();
                publish(producer, &args.stats_topic, "land_rate", &snap).await;
            }
        }
    }
}

async fn run(args: &Args, producer: &FutureProducer, consumer: &StreamConsumer) {
    let mut tally = Counts::default();
    let mut backoff = Duration::from_millis(500);
    loop {
        match session(args, producer, consumer, &mut tally).await {
            Ok(()) => {
                warn!("stream closed by server");
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                error!(err = ?e, ?backoff, "session failed");
                backoff = (backoff * 2).min(Duration::from_secs(20));
            }
        }
        sleep(backoff).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().json().init();
    let args = Args::parse();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.bootstrap_servers)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()
        .wrap_err("creating kafka producer")?;
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.bootstrap_servers)
        .set("group.id", "jito-gateway")
        .set("auto.offset.reset", "latest")
        .create()
        .wrap_err("creating kafka consumer")?;
    consumer
        .subscribe(&[&args.requests_topic])
        .wrap_err("subscribing to requests topic")?;
    tokio::select! {
        _ = run(&args, &producer, &consumer) => {}
        _ = tokio::signal::ctrl_c() => info!("ctrl-c received, shutting down"),
    }
    producer.flush(Duration::from_secs(10))?;
    Ok(())
}
