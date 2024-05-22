use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use tokio_stream::StreamExt;
use tonic::transport::Uri;

use crate::orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;
use crate::orderbook::Empty;

mod types;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

#[derive(Parser, Debug)]
#[clap(version = "0.0.1", about = "Orderbook Aggregator Client")]
struct Opts {
    #[arg(short, long, default_value = "http://127.0.0.1:9876")]
    endpoint: String,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbosity: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();

    let log_level = match opts.verbosity {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    Builder::from_default_env().filter(None, log_level).init();

    let uri: Uri = opts.endpoint.parse::<Uri>()?;
    let channel = tonic::transport::Channel::builder(uri).connect().await?;
    let mut client = OrderbookAggregatorClient::new(channel);

    let request = tonic::Request::new(Empty {});
    let mut response = client.book_summary(request).await?.into_inner();

    loop {
        match response.next().await {
            None => {
                log::info!("Service stopped sending orderbook summaries");
                break;
            }
            Some(summary) => {
                match summary {
                    Ok(summary) => {
                        println!("{}", summary);
                    }
                    Err(status) => {
                        log::warn!("Received unexpected error status from orderbook aggregator service: {}", status)
                    }
                }
            }
        }
    }

    Ok(())
}
