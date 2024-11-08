CREATE TABLE IF NOT EXISTS {target_db}.fact_vip_level(
    account_id String,
    game_code String,
    topup_date Date,
    total_revenue Int32,
    revenue_by_date Int32,
    total_transaction Int32,
    transaction_by_date Int32,
    current_vip_level Int32
)
ENGINE = ReplacingMergeTree
ORDER BY (account_id, game_code, topup_date)
