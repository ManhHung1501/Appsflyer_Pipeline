WITH result AS (select
    AccountID as account_id,
    GameCode as game_code,
    OSType as os_type,
    lower(OsName) as platform,
    PackageID as package_id,
    toDate(TopupDate) as topup_date,
    TopupDesc as topup_desc,
    PaymentType as payment_type,
    PaymentName as payment_name,
    sum(TopupAmount * 500)  as revenue,
    count(*) as transaction
from da_cdp_funzy.vw_websale_prod
where GameCode = '{game_code}'
group by 
    AccountID,
    GameCode,
    PackageID,
    TopupDate,
    TopupDesc,
    OSType,
    OsName,
    PaymentType,
    PaymentName)
SELECT {order_col} FROM result