import pandas  as pd
import requests
import io
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from utils.clickhouse_utils import generate_create_table_query, connect_clickhouse
from config.appsflyer import appsflyer_email, appsflyer_password


class BrowserNavigator:

    def log_in(self):
        print("Logging in...")

        self.browser.find_element(By.ID, "user-email").send_keys(appsflyer_email)
        self.browser.find_element(By.ID, "password-field").send_keys(appsflyer_password)
        self.browser.find_element(By.XPATH, '//*[@id="login-form"]/div[6]/button').click()

        while self.browser.current_url == "https://hq1.appsflyer.com/auth/login":
            time.sleep(self.sleep_time)

        print("Logged")

    def __init__(self, browser):
        self.sleep_time = 1
        self.max_loading_attempts = 30
        self.browser = browser
        browser.get("https://hq1.appsflyer.com/auth/login")


def crawl_cohort_data(
        game_config: dict,
        target_db: str, 
        target_table: str, 
        engine: str, 
        primary_column: str,
        **context):

    date = context["ti"].xcom_pull(task_ids="load_config", key="start_date")
 
    print(f"config: {game_config}")


    app_ids = []
    if 'android_app_id' in game_config:
        app_ids.append(game_config['android_app_id'])
    if 'ios_app_id' in game_config:
        app_ids.append(game_config['ios_app_id'])
    print(f"app_ids: {app_ids}")

    # Open browser
    # chrome_driver_path = ChromeDriverManager(driver_version='129.0.6668.89').install()
    chrome_driver_path = "/usr/local/bin/chromedriver"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    browser = webdriver.Chrome(service=Service(chrome_driver_path), options=options)
    page = BrowserNavigator(browser)
    page.log_in()

    # Get all cookies
    cookies = browser.get_cookies()
    cookie_str = ''
    for cookie in cookies:
        cookie_str += f"{cookie['name']}={cookie['value']}; "

    headers = {
        'cookie': cookie_str.strip()
    }

    myobj = {
        "granularity": "day",
        "measure": {
            "event": "revenue",
            "metric": "revenue",
            "type": "calculated",
            "per_user": True
        },
        "alt_currency": True,
        "min_installs": 1,
        "from": date,
        "to": date,
        "groupings": ["media_source", "campaign", "adset_name", "cohort_day"],
        "filters": {
            "app_id": app_ids,
            "conversion_type": ["install"],
            "period": [3]
        },
        "view_mode": "cumulative",
        "skip": 0,
        "take": 25,
        "alt_timezone": False,
        "trend": True,
        "allow_partial_period_data": True
    }

    # Make request
    response = requests.post('https://hq1.appsflyer.com/cohort/export-csv', json=myobj, headers=headers)
    
    if response.status_code == 200:
        print(f"Crawl cohort data of {app_ids} from Appsflyer Success")
        data = response.json()['data']
        # Assuming the response contains CSV data
        df = pd.read_csv(io.StringIO(data), sep=",")
        
        df.rename(columns={'Media Source': 'media_source', 'Campaign': 'campaign', 'Adset Name': 'adset_name', 'Cohort Day': 'cohort_day', 'Users': 'users', 'Cost': 'cost', 'Average eCPI': 'average_ecpi'},
                          inplace=True)
        df = df[['media_source', 'campaign', 'adset_name', 'cohort_day', 'users', 'cost', 'average_ecpi']]
        df[['users', 'cost', 'average_ecpi']] = df[['users', 'cost', 'average_ecpi']].fillna(value=0)
        df[['media_source', 'campaign', 'adset_name']] = df[['media_source', 'campaign', 'adset_name']].fillna(value='unknown')

        # Create  database and table if not exist
        clickhouse_client = connect_clickhouse()
        clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        create_tbl_query = generate_create_table_query(df, target_db, target_table, engine, primary_column)
        clickhouse_client.execute(create_tbl_query)

        # Write DataFrame to ClickHouse
        clickhouse_client.insert_dataframe(f"INSERT INTO {target_db}.{target_table} VALUES", df)

        # Deduplicate after ingest data
        clickhouse_client.execute(f"OPTIMIZE TABLE {target_db}.{target_table} DEDUPLICATE BY {primary_column}")
        print(f"Write cohort data to clickhouse success")
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")

    browser.quit()


