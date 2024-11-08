CREATE TABLE IF NOT EXISTS {target_db}.fact_login(
    account_id String,
    game_code String,
    os_type Int32,
    platform String,
    login_date Date,
    number_login Int32
)
ENGINE = ReplacingMergeTree
ORDER BY (account_id, game_code, os_type, platform, login_date);
