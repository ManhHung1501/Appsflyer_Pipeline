with funzy_login AS (
    SELECT DISTINCT * FROM da_cdp_funzy.Vw_Account_Login
),
result AS (
    SELECT 
        toString(AccountID) as account_id,
        GameCode as game_code,
        OSType as os_type,
        (case 
            when OSType = 1 then 'android' 
            when OSType = 2 then 'ios'
            when OSType = 3 then 'web'
            else 'other'
        end) as platform,
        toDate(LoginDate) as login_date,
        count(*) as number_login
    from funzy_login
    where GameCode = '{game_code}'
    group by
        AccountID,
        GameCode,
        OSType,
        LoginDate
    )
SELECT {order_col} FROM result