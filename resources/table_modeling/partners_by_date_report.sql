CREATE TABLE IF NOT EXISTS  {target_db}.partners_by_date_report
(
    game_code                 String,
    report_date               Date,
    partner                   String,
    media_source              String,
    campaign                  String,
    impressions               Int32,
    clicks                    Int32,
    ctr                       Float64,
    installs                  Int32,
    conversion_rate           Float64,
    sessions                  Int32,
    loyal_users               Int32,
    loyal_users_installs_rate Float64,
    total_revenue             Float64,
    total_cost                Float64,
    roi                       Float64,
    arpu                      Float64,
    average_ecpi              Float64,
    platform                  String
)
    ENGINE = ReplacingMergeTree 
    ORDER BY (game_code, report_date, partner, media_source, campaign, platform)
;
