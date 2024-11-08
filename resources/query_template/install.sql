WITH result AS (select 
    '{game_code}' as game_code,
    toDate(event_time) as install_date,
    media_source, 
    if (campaign is null or campaign in ('', 'nan'), 'unknown', campaign) as campaign,
    if (adset is null or adset in ('', 'nan'), 'unknown', adset) as adset,
    (case
            when advertising_id <> '' then 'android'
            when (idfa <> '' or idfv <> '') then 'ios'
            else 'other'
        END) as platform,
    count(distinct appsflyer_id) as install
from da_cdp_{game_code}.appsflyer_install
GROUP BY install_date, game_code, media_source, campaign, adset, platform)
SELECT {order_col} FROM result
