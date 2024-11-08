create table IF NOT EXISTS {target_db}.fact_device(
    account_id String,
    game_code String,
    appsflyer_id String,
    af_web_id String,
    platform String,
    advertising_id String,
    idfv String,
    idfa String,
    install_time DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY (account_id, game_code, appsflyer_id, af_web_id);
