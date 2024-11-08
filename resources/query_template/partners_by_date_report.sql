WITH result AS
(
    SELECT '{game_code}' as game_code, *
    FROM da_cdp_{game_code}.appsflyer_partners_by_date 
)
SELECT {order_col} FROM result
