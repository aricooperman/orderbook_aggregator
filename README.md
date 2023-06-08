# Orderbook Aggregator

### Running the service

    $ cargo run -p orderbook_aggregator_service

##### Options
  
    $ cargo run -p orderbook_aggregator_service -- --help
    
    Usage: orderbook_aggregator_service [OPTIONS]
  
    Options:
      -s, --symbol <SYMBOL>        [default: BTCUSDT]
      -l, --listen-at <LISTEN_AT>  [default: 127.0.0.1:9876]
      -v, --verbosity...           
      -d, --depth <DEPTH>          [default: 10]
      -h, --help                   Print help
      -V, --version                Print version

### Running the simple client

    $ cargo run -p orderbook_aggregator_client

##### Options

    $ cargo run -p orderbook_aggregator_client -- --help
  
    Usage: orderbook_aggregator_client [OPTIONS]
  
    Options:
      -e, --endpoint <ENDPOINT>  [default: http://127.0.0.1:9876]
      -v, --verbosity...         
      -h, --help                 Print help
      -V, --version              Print version
