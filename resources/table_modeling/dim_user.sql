create table IF NOT EXISTS {target_db}.dim_user(
    account_id String,
    game_code String,
    regist_date Date,
    media_source String,
    campaign String,
    adset String,
    platform String,
    partner String,
    last_login_date Date,
    is_paid Int32,
    first_purchase_date Nullable(Date)
)
ENGINE= ReplacingMergeTree
ORDER BY (account_id, game_code);
