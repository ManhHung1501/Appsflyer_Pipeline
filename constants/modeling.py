
table = {
    'COHORT_MKT': {
        'primary_key': 'game_code,cohort_date,media_source,campaign,adset',
        'filter_date_col': 'cohort_date',
        'order_col': 'game_code, cohort_date, media_source, campaign, adset, users, cost, average_ecpi'
    },
    'fact_device': {
        'primary_key': 'account_id,game_code,appsflyer_id,af_web_id',
        'filter_date_col': 'install_time',
        'order_col': 'account_id,game_code,appsflyer_id,af_web_id,platform,advertising_id,idfv,idfa,install_time'
    },
    'fact_login': {
        'primary_key': 'account_id,game_code,os_type,platform,login_date',
        'filter_date_col': 'login_date',
        'order_col': 'account_id, game_code, os_type, platform, login_date, number_login',
    },
    'fact_transaction': {
        'primary_key': 'account_id,game_code, os_type, platform, package_id, topup_date, topup_desc, payment_type, payment_name',
        'filter_date_col': 'topup_date',
        'order_col': 'account_id, game_code, os_type, platform, package_id, topup_date, topup_desc, payment_type, payment_name, revenue, transaction'
    },
    'install': {
        'primary_key': 'install_date, game_code, media_source, campaign, adset, platform',
        'filter_date_col': 'install_date',
        'order_col': 'game_code, install_date, media_source, campaign, adset, platform, install'
    },
    'dim_user': {
        'primary_key': 'account_id,game_code',
        'filter_date_col': 'regist_date',
        'order_col': 'account_id, game_code, regist_date, media_source, campaign, adset, platform, partner, last_login_date, is_paid, first_purchase_date'
    },
    'fact_vip_level': {
        'primary_key': 'account_id, game_code, topup_date',
        'filter_date_col': 'topup_date',
        'order_col': 'account_id, game_code, topup_date, total_revenue, revenue_by_date, total_transaction, transaction_by_date, current_vip_level'
    },
    'partners_by_date_report': {
        'primary_key': 'game_code, report_date, partner, media_source, campaign, platform',
        'filter_date_col': 'report_date',
        'order_col': 'game_code, report_date, partner, media_source, campaign,platform, impressions,clicks, ctr, installs, conversion_rate, sessions,loyal_users, loyal_users_installs_rate, total_revenue,total_cost, roi, arpu, average_ecpi'
    },
}