DROP TABLE IF EXISTS cdc_demo.asks_table;
DROP TABLE IF EXISTS cdc_demo.bids_table;
DROP TABLE IF EXISTS cdc_demo.klines_table;
DROP TABLE IF EXISTS cdc_demo.price_table;
DROP TABLE IF EXISTS cdc_demo.ticker_table;
DROP TABLE IF EXISTS cdc_demo.trades_table;


CREATE KEYSPACE IF NOT EXISTS cdc_demo
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};


USE cdc_demo;


-- Order Book: Asks
CREATE TABLE IF NOT EXISTS cdc_demo.asks_table (
    symbol      text,
    price       decimal,
    quantity    decimal,
    fetched_at  timestamp,
    PRIMARY KEY (symbol, price, fetched_at)
) WITH CLUSTERING ORDER BY (price ASC, fetched_at DESC);

-- Order Book: Bids
CREATE TABLE IF NOT EXISTS cdc_demo.bids_table (
    symbol      text,
    price       decimal,
    quantity    decimal,
    fetched_at  timestamp,
    PRIMARY KEY (symbol, price, fetched_at)
) WITH CLUSTERING ORDER BY (price DESC, fetched_at DESC);

-- Price Table (latest only)
CREATE TABLE IF NOT EXISTS cdc_demo.price_table (
    symbol      text,
    price       decimal,
    fetched_at  timestamp,
    PRIMARY KEY (symbol, fetched_at)
) WITH CLUSTERING ORDER BY (fetched_at DESC);

-- Trades
CREATE TABLE IF NOT EXISTS cdc_demo.trades_table (
    symbol          text,
    id              bigint,
    price           decimal,
    qty             decimal,
    quote_qty       decimal,
    time            bigint,
    is_buyer_maker  boolean,
    is_best_match   boolean,
    fetched_at      timestamp,
    PRIMARY KEY (symbol, id)
) WITH CLUSTERING ORDER BY (id DESC);

-- Klines (Candlesticks)
CREATE TABLE IF NOT EXISTS cdc_demo.klines_table (
    symbol                  text,
    open_time               bigint,
    open_price              decimal,
    high_price              decimal,
    low_price               decimal,
    close_price             decimal,
    volume                  decimal,
    close_time              bigint,
    quote_asset_volume      decimal,
    no_trades               int,
    base_asset_volume       decimal,
    buy_quote_asset_volume  decimal,
    unused                  int,
    fetched_at              timestamp,
    PRIMARY KEY (symbol, open_time)
) WITH CLUSTERING ORDER BY (open_time DESC);

-- Ticker Updates (history)
CREATE TABLE IF NOT EXISTS cdc_demo.ticker_table (
    symbol                text,
    fetched_at            timestamp,
    price_change          decimal,
    price_change_percent  decimal,
    weighted_avg_price    decimal,
    prev_close_price      decimal,
    last_price            decimal,
    last_qty              decimal,
    bid_price             decimal,
    bid_qty               decimal,
    ask_price             decimal,
    ask_qty               decimal,
    open_price            decimal,
    high_price            decimal,
    low_price             decimal,
    volume                decimal,
    quote_volume          decimal,
    open_time             bigint,
    close_time            bigint,
    first_id              bigint,
    last_id               bigint,
    count                 bigint,
    PRIMARY KEY (symbol, fetched_at)
) WITH CLUSTERING ORDER BY (fetched_at DESC);
