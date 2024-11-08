CREATE TABLE IF NOT EXISTS {target_db}.fact_transaction
(
    account_id String,
    game_code String,
    os_type Int32,
    platform String,
    package_id String,
    topup_date Date,
    topup_desc String,
    payment_type String,
    payment_name String,
    revenue Int64,
    transaction Int32
)
ENGINE = ReplacingMergeTree
ORDER BY (account_id, game_code, os_type, platform, package_id, topup_date, topup_desc, payment_type, payment_name);
