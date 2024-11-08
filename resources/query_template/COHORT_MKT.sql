WITH result AS (select DISTINCT
    '{game_code}' as game_code,
    toDate(cohort_day) as cohort_date,
    if(media_source = 'Organic', 'organic', media_source) as media_source,
    case
        when media_source in ('Organic', 'organic') then 'organic'
        when campaign is null or campaign in ('', 'nan') then 'unknown'
        else campaign
    end as campaign,
    case
        when media_source in ('Organic', 'organic') then 'organic'
        when adset_name is null or adset_name in ('', 'nan') then 'unknown'
        else adset_name
    end as adset,
    if (isNaN(users), 0, users) as users,
    if (isNaN(cost), 0, cost) as cost,
    if (isNaN(average_ecpi), 0, average_ecpi) as average_ecpi
from da_cdp_{game_code}.COHORT_MKT)
SELECT {order_col} FROM result