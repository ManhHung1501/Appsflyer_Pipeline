WITH install AS (
    SELECT distinct idfa,idfv,advertising_id, appsflyer_id , event_time,'{game_code}' as game_code 
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
)
SELECT {order_col} from device_app 
