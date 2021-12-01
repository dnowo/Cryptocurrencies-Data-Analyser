CREATE TABLE IF NOT EXISTS crypto.cryptocurrencies (
    serialnumber int PRIMARY KEY,
    close decimal,
    high decimal,
    low decimal,
    marketcap decimal,
    name text,
    open decimal,
    symbol text,
    volume decimal,
    date timestamp
) WITH ID = acc50660-504d-11ec-9c48-8b0cca79f41d
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
ALTER TABLE crypto.cryptocurrencies DROP date USING TIMESTAMP 1638110488739000;
