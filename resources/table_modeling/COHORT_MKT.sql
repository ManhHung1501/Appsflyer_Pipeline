create table IF NOT EXISTS {target_db}.COHORT_MKT(
    game_code String,
    cohort_date Date,
    media_source String,
    campaign String,
    adset String,
    users String,
    cost Float32,
    average_ecpi Float32
)
ENGINE = ReplacingMergeTree
ORDER BY (game_code, cohort_date, media_source, campaign, adset);
