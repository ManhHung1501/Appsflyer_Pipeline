CREATE TABLE IF NOT EXISTS {target_db}.install(
    game_code String,
    install_date Date,
    media_source String,
    campaign String,
    adset String,
    platform String,
    install Int32
)
ENGINE = ReplacingMergeTree
ORDER BY (install_date, game_code, media_source, campaign, adset, platform);
