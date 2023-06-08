extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["src/proto/orderbook_aggregator_service.proto"], &["src"])
        .unwrap_or_else(|e| panic!("Failed to compile proto {:?}", e));
}
