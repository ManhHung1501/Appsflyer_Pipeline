with transaction_by_date AS (
    SELECT 
        account_id,
        game_code,
        topup_date,
        SUM(revenue) AS revenue_by_date,
        SUM(transaction) AS transaction_by_date
    FROM da_cdp_general.fact_transaction
    GROUP BY 
        account_id,
        game_code,
        topup_date
), 
revenue_total AS(
    SELECT
        account_id,
        game_code,
        topup_date,
        SUM(revenue_by_date) over(PARTITION BY account_id, game_code ORDER BY topup_date ASC) AS total_revenue,
        revenue_by_date,
        SUM(transaction_by_date) over(PARTITION BY account_id, game_code ORDER BY topup_date ASC) AS total_transaction,
        transaction_by_date,
        CASE 
            WHEN game_code = '3qtrieuhoansu'THEN
                (
                    CASE
                        WHEN total_revenue >= 500000000 THEN 15 
                        WHEN total_revenue >= 300000000 THEN 14 
                        WHEN total_revenue >= 200000000 THEN 13 
                        WHEN total_revenue >= 150000000 THEN 12 
                        WHEN total_revenue >= 100000000 THEN 11 
                        WHEN total_revenue >= 50000000 THEN 10 
                        WHEN total_revenue >= 30000000 THEN 9
                        WHEN total_revenue >= 20000000 THEN 8
                        WHEN total_revenue >= 10000000 THEN 7
                        WHEN total_revenue >= 5000000 THEN 6
                        WHEN total_revenue >= 2500000 THEN 5
                        WHEN total_revenue >= 1500000 THEN 4
                        WHEN total_revenue >= 500000 THEN 3
                        WHEN total_revenue >= 125000 THEN 2
                        WHEN total_revenue >= 25000 THEN 1
                        ELSE 0
                    END
                )
            WHEN game_code = 'meow' THEN
                (
                    CASE
                        WHEN total_revenue < 25000 THEN 0
                        WHEN total_revenue >= 25000 AND total_revenue < 125000 THEN 1
                        WHEN total_revenue >= 125000 AND total_revenue < 416667 THEN 2
                        WHEN total_revenue >= 416667 AND total_revenue < 833333 THEN 3
                        WHEN total_revenue >= 833333 AND total_revenue < 2083333 THEN 4
                        WHEN total_revenue >= 2083333 AND total_revenue < 4166667 THEN 5
                        WHEN total_revenue >= 4166667 AND total_revenue < 8333333 THEN 6
                        WHEN total_revenue >= 8333333 AND total_revenue < 20833333 THEN 7
                        WHEN total_revenue >= 20833333 AND total_revenue < 41666667 THEN 8
                        WHEN total_revenue >= 41666667 AND total_revenue < 83333333 THEN 9
                        WHEN total_revenue >= 83333333 AND total_revenue < 166666667 THEN 10
                        WHEN total_revenue >= 166666667 AND total_revenue < 250000000 THEN 11
                        WHEN total_revenue >= 250000000 AND total_revenue < 416666667 THEN 12
                        WHEN total_revenue >= 416666667 THEN 13
                    END
                )
        END AS current_vip_level
    FROM transaction_by_date
 )       
SELECT {order_col} FROM revenue_total