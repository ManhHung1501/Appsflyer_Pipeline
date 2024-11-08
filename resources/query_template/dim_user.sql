WITH media_app AS (
    SELECT
        DISTINCT
        user_id,
        media_source,
        campaign,
        adset,
        platform,
        event_time,
        event_name,
        partner,
        '{game_code}' as game_code
    FROM da_cdp_{game_code}.appsflyer_user_registration
),
dual_event as (
    SELECT 
        user_id, game_code,
        MAX(IF(event_name = 'af_registration', media_source, NULL)) AS ms_registration,
        MAX(IF(event_name<>'af_registration', media_source, NULL)) as ms_normal,
        MAX(IF(event_name='af_registration', campaign, NULL)) as c_registration,
        MAX(IF(event_name<>'af_registration', campaign, NULL)) as c_normal,
        MAX(IF(event_name='af_registration', adset, NULL)) as a_registration,
        MAX(IF(event_name<>'af_registration', adset, NULL)) as a_normal,
        MAX(IF(event_name='af_registration', platform, NULL)) as os_registration,
        MAX(IF(event_name<>'af_registration', platform, NULL)) as os_normal,
        MAX(IF(event_name='af_registration', partner, NULL)) as partner_registration,
        MAX(IF(event_name<>'af_registration', partner, NULL)) as partner_normal
    FROM media_app
    GROUP BY user_id, game_code
),
media as (
    SELECT 
        user_id,
        COALESCE(ms_registration, ms_normal) AS media_source,
        COALESCE(c_registration, c_normal) as campaign,
        COALESCE(a_registration, a_normal) as adset,
        COALESCE(os_registration, os_normal) as platform,
        COALESCE(partner_registration, partner_normal) as partner,
        game_code
    FROM dual_event
),
login as (
    SELECT 
        account_id,
        game_code,
        MIN(login_date) AS regist_time,
        MAX(login_date) AS last_login_time
    FROM da_cdp_general.fact_login
    WHERE game_code = '{game_code}'
    GROUP BY account_id, game_code
),
login_join_media as (
    SELECT
        DISTINCT
        toString(account_id) as user_id,
        regist_time,
        last_login_time,
        (case when media_source is null or media_source='' then 'unknown' else media_source end)  as media_source,
        (case when campaign is null or campaign='' then 'unknown' else campaign end)  as campaign,
        (case when adset is null or adset='' then 'unknown' else adset end)  as adset,
        (case when platform is null or platform='' then 'other' else platform end) as platform,
        (case when partner is null or partner='' then 'unknown' else partner end) as partner,
        game_code
    FROM login
    LEFT OUTER JOIN media 
        on toString(login.account_id) = toString(media.user_id)
        and login.game_code = media.game_code
),
paid_user as (
    SELECT account_id as user_id, 1 as is_paid, game_code,
       MIN(topup_date) as first_purchase_date
    FROM da_cdp_general.fact_transaction
    GROUP BY account_id,game_code
), 
result AS (SELECT 
    u.user_id as account_id, 
    u.game_code as game_code,
    toDate(regist_time)  as regist_date,
    media_source, campaign,adset,platform,partner,
    toDate(last_login_time) as last_login_date, 
    (case when is_paid is not null then is_paid else 0 end) as is_paid,
    (case when first_purchase_date = '1970-01-01' then NULL ELSE first_purchase_date END) as first_purchase_date
from login_join_media u
LEFT JOIN paid_user p on p.user_id = u.user_id and p.game_code = u.game_code
)
SELECT {order_col} FROM result