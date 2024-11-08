WITH install AS (
    SELECT distinct idfa,idfv, advertising_id, appsflyer_id , event_time,'{game_code}' as game_code 
    FROM da_cdp_{game_code}.appsflyer_install
),
registration AS (
    SELECT distinct appsflyer_id, user_id, '{game_code}' as game_code 
    FROM da_cdp_{game_code}.appsflyer_user_registration res 
),
device_app as (
    SELECT distinct 
        user_id as account_id, 
        game_code, 
        (case
            when advertising_id <> '' then 'android'
            when (idfa <> '' or idfv <> '') then 'ios'
            else 'other'
        END) as platform,
        appsflyer_id,
        idfa, 
        idfv, 
        advertising_id, 
        '' as af_web_id,
        toDateTime(event_time) as install_time
    FROM install 
    JOIN registration reg 
        ON install.appsflyer_id = reg.appsflyer_id
        and install.game_code = reg.game_code
    WHERE account_id is NOT NULL AND account_id <> ''
),
device_web as (
    SELECT distinct 
        user_id as account_id, 
        '{game_code}' as game_code,
        'web' as platform,
        IF(appsflyer_id ='null', '', appsflyer_id) as appsflyer_id ,
        IF(idfa ='null', '', idfa) as idfa,
        IF(idfv ='null', '', idfv) as idfv,
        IF(advertising_id ='null', '', advertising_id) as advertising_id,
        af_web_id, 
        toDateTime(substring(event_time, 1, 19)) as install_time
    FROM da_cdp_{game_code}.af_web_user_registration
    WHERE account_id is NOT NULL AND account_id <> ''
),
result AS 
(
    SELECT account_id,game_code,appsflyer_id,af_web_id,platform,advertising_id,idfv,idfa,install_time from device_app 
    union all 
    SELECT account_id,game_code,appsflyer_id,af_web_id,platform,advertising_id,idfv,idfa,install_time from device_web
)
SELECT {order_col} FROM result