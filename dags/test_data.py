from modules.db_worker import DBWorker
from modules.miner_currency import fiat_currencies, crypto_currencies, stable_coins
import requests
import configparser
import json

currencies = fiat_currencies + crypto_currencies + stable_coins + ['USD', 'EUR', 'RUB']

config = configparser.ConfigParser()
config.read('/creds/creds.ini')

db = DBWorker()
SLACK_HOOK = config['slack']["SLACK_HOOK"]

check_nulls_query = """
SELECT *
FROM public.currency_rates
WHERE rate <= 0
"""

check_duplicates_query = """
SELECT date, from_currency, to_currency, count(*)
FROM public.currency_rates
group by 1, 2, 3
having count(*)>1;
"""

check_empty_days = """
WITH DateRange AS (
    SELECT generate_series('2020-01-01'::date, CURRENT_DATE - interval '1 day', '1 day'::interval)::date AS date
)

SELECT
    DateRange.date,
    currencies.from_currency,
    currencies.to_currency
FROM
    DateRange
CROSS JOIN
    (SELECT DISTINCT from_currency, to_currency FROM public.currency_rates) AS currencies
LEFT JOIN
    public.currency_rates AS t ON DateRange.date = t.date AND currencies.from_currency = t.from_currency AND currencies.to_currency = t.to_currency
WHERE
    t.date IS NULL
AND not (
-- Исключение для ILS и ISK в январе 2018 - не отдает ECB
    (
     currencies.from_currency in ('ILS', 'ISK')
     or currencies.to_currency in ('ILS', 'ISK')
    )
    AND DateRange.date between '2018-01-01' and '2018-01-31'
)
"""

check_currency_existance = f"""
WITH currency_list AS (
    SELECT unnest(ARRAY{currencies}) AS currency
)

SELECT
    cl.currency
FROM
    currency_list cl
WHERE NOT EXISTS (
    SELECT 1
    FROM public.currency_rates t
    WHERE (t.from_currency = cl.currency AND (t.to_currency = 'USD' OR t.to_currency = 'EUR'))
       OR (t.to_currency = cl.currency AND (t.from_currency = 'USD' OR t.from_currency = 'EUR'))
)
ORDER BY
    cl.currency;
"""


def run_check(query, message):
    values = db.find_many(query)
    if values:
        params = {
            "text": f""":warning: *{message}* ```{query}```
            """
        }
        headers = {
            "Content-Type": "application/json"
        }
        requests.post(SLACK_HOOK, data=json.dumps(params), headers=headers)
        return 1
    else:
        return 0


def run_tests():
    nulls_flag = run_check(query=check_nulls_query,
                           message="Найдены нулевые или отрицательные значения в таблице курсов валют")

    duplicates_flag = run_check(query=check_duplicates_query,
                                message="Найдены дубликаты в таблице курсов валют")

    empty_days_flag = run_check(query=check_empty_days,
                                message="Обнаружены пустые дни в таблице курсов валют")

    currency_existance_flag = run_check(query=check_currency_existance,
                                        message="Обнаружены отсутствующие пары валют")

    if nulls_flag == 0 and duplicates_flag == 0 and empty_days_flag == 0 and currency_existance_flag == 0:
        resp = {"status": "OK"}
    else:
        resp = {
            "status": "TESTS_FAILED",
            "tests": {
                "nulls_flag": nulls_flag,
                "duplicates_flag": duplicates_flag,
                "empty_days_flag": empty_days_flag,
                "currency_existance_flag": currency_existance_flag
            }
        }

    return resp


run_tests()


CREATE DATABASE currency;
CREATE USER postgres WITH PASSWORD 'postgres';
GRANT ALL PRIVILEGES ON DATABASE currency TO postgres;


public.currency_rates
