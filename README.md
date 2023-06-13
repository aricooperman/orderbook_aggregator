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

The client will output a stream of console orderbook dumps like so:

    Spread: 2.220000
    L :                                Bids | Asks                               
    1 : 26385.000000 @ 0.117906 on bitstamp | 26387.220000 @ 0.000040 on binance
    2 : 26384.000000 @ 0.268436 on bitstamp | 26387.820000 @ 0.000580 on binance
    3 : 26382.000000 @ 0.065000 on bitstamp | 26388.320000 @ 0.008520 on binance
    4 : 26380.000000 @ 0.284272 on bitstamp | 26390.000000 @ 0.208397 on bitstamp
    5 : 26379.000000 @ 0.159769 on bitstamp | 26390.400000 @ 0.001850 on binance
    6 : 26378.000000 @ 2.099163 on bitstamp | 26391.000000 @ 0.060000 on bitstamp
    7 : 26376.000000 @ 0.125000 on bitstamp | 26391.620000 @ 0.026620 on binance
    8 : 26375.000000 @ 0.046058 on bitstamp | 26392.000000 @ 0.046058 on bitstamp
    9 : 26373.000000 @ 0.065000 on bitstamp | 26393.000000 @ 0.757700 on bitstamp
    10: 26372.000000 @ 0.077091 on bitstamp | 26394.550000 @ 0.000430 on binance
