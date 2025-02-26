from datetime import datetime, timedelta
import xmltodict
import requests
import copy
from typing import Dict, Optional, Union, Tuple, List
from workalendar.europe import EuropeanCentralBank, Russia, Ukraine
from airflow.models import Variable


# Получаем прокси-данные из Airflow Variables
PROXY_USER = Variable.get('PROXY_USER', default_var='user3726989')
PROXY_PASSWORD = Variable.get('PROXY_PASSWORD', default_var='PNPvnFsKTg')


# Если прокси-логин и пароль заданы, формируем словарь прокси
if PROXY_USER and PROXY_PASSWORD:
    proxy = {
        'http': f"http://{PROXY_USER}:{PROXY_PASSWORD}@171.22.110.104:42567"
    }
else:
    proxy = None  # Если прокси не заданы, не используем их


fiat_currencies = [
    'JPY', 'BGN', 'CZK', 'DKK', 'GBP', 'HUF', 'PLN', 'RON', 'SEK', 'CHF',
    'ISK', 'NOK', 'TRY', 'AUD', 'BRL', 'CAD', 'CNY', 'HKD', 'IDR', 'ILS',
    'INR', 'KRW', 'MXN', 'MYR', 'NZD', 'PHP', 'SGD', 'THB', 'ZAR'
]

crypto_currencies = ['BTC', 'BCH', 'ETH', 'LTC']
stable_coins = ['ERC', 'TRC', 'USDC', 'USDT']


class CurrencyMiner:
    def make_request(self, url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None,
                     auth: Tuple[str, str] = None, method: Optional[str] = 'get', **kwargs) -> Tuple[
        Union[Dict, str], int, Dict]:
        params = params or {}
        headers = headers or {}
        result_data, resp_status, resp_headers = None, None, None

        try:
            if method == 'post':
                response = requests.post(url, data=params, headers=headers, auth=auth, proxies=proxy, **kwargs)
            else:
                response = requests.get(url, params=params, headers=headers, auth=auth, proxies=proxy, **kwargs)

            resp_status = response.status_code
            resp_headers = response.headers

            try:
                result_data = response.json()
            except Exception:
                result_data = response.text
        except Exception as e:
            raise Exception(f'Malfunction during execution of "{method}" call: {e}')

        return result_data, resp_status, resp_headers

    def get_binance_rates(self, eur_to_usd: float, currency: Optional[str] = 'BTC', since: Optional[str] = '') -> List[
        Dict]:

        date = int(datetime.strptime(since, '%Y-%m-%d').timestamp() * 1000)
        next_date = int((datetime.strptime(since, '%Y-%m-%d') + timedelta(days=1)).timestamp() * 1000)

        result_data, resp_status, resp_headers = self.make_request(
            url=f'https://api.binance.com/api/v3/klines?symbol={currency}USDT&interval=1d&startTime={date}&endTime={next_date}'
        )
        if result_data:
            kline = result_data[0]
            try:
                crypto_volume = float(kline[10].replace(',', '.'))
                usdt_volume = float(kline[9].replace(',', '.'))
                rate = round(crypto_volume / usdt_volume, 6)
            except:
                rate = 0

            result_list = [
                {
                    'from_currency': currency,
                    'to_currency': 'USD',
                    'rate': rate
                },
                {
                    'from_currency': 'USD',
                    'to_currency': currency,
                    'rate': 1 / rate if rate else 0
                },
                {
                    'from_currency': currency,
                    'to_currency': 'EUR',
                    'rate': rate / eur_to_usd
                },
                {
                    'from_currency': 'EUR',
                    'to_currency': currency,
                    'rate': 1 / rate * eur_to_usd if rate else 0
                }
            ]
        else:
            print(f"There is no data for date {since}")
            raise Exception

        return result_list



    def get_cbr_rates(self, currency: Optional[str] = 'USD', since: Optional[str] = '') -> List[Dict]:
        currencies = {'USD': 'USD', 'EUR': 'EUR'}

        correct_date = self.correct_date(since, Russia)
        req_since = datetime.strptime(correct_date, '%Y-%m-%d').strftime('%d/%m/%Y')

        result_data, resp_status, resp_headers = self.make_request(
            url=f'http://www.cbr.ru/scripts/XML_daily.asp?date_req={req_since}',
            method='post'
        )

        if result_data:
            result_data = self.try_convert_xml(result_data)
            values = result_data.get('ValCurs', {}).get('Valute', [])

            rate_entry = next((x for x in values if x.get('CharCode') == currencies.get(currency)), None)

            if rate_entry:
                rate = rate_entry.get('Value').replace(',', '.')
                try:
                    rate = round(float(rate), 6)
                except ValueError:
                    rate = 0
            else:
                print(f"Курс валюты {currency} не найден в ответе ЦБ РФ за {since}")
                rate = 0

            result_list = [
                {'from_currency': currency, 'to_currency': 'RUB', 'rate': rate},
                {'from_currency': 'RUB', 'to_currency': currency, 'rate': 1 / rate if rate else 0}
            ]
        else:
            print(f"Нет данных для даты {since}")
            raise Exception

        return result_list

    def get_ecb_rates(self, to_currency: Optional[str] = 'USD', since: Optional[str] = '',
                      until: Optional[str] = '') -> Dict:
        curr_data = {}

        correct_date = self.correct_date(since, EuropeanCentralBank)

        params = {
            'startPeriod': correct_date,
            'endPeriod': correct_date
        }

        result_data, resp_status, resp_headers = self.make_request(
            url=f'https://data-api.ecb.europa.eu/service/data/EXR/D.{to_currency}.EUR.SP00.A', params=params
        )

        if result_data:
            result_data = self.try_convert_xml(result_data)
            rate = result_data.get('message:GenericData', {}).get('message:DataSet', {}).get('generic:Series', {}) \
                .get('generic:Obs', {}).get('generic:ObsValue', {}).get('@value', '')
            try:
                rate = round(float(rate), 6)
            except:
                rate = 0

            curr_data = {
                'from_currency': 'EUR',
                'to_currency': to_currency,
                'rate': rate,
            }
        else:
            print(f"There is no data for date {since}")
            raise Exception

        return curr_data

    def try_convert_xml(self, data: Union[dict, str]) -> Dict:
        try:
            return xmltodict.parse(data)
        except Exception:
            return data

    def correct_date(self, input_date_str, calendar):
        correct_date = datetime.strptime(input_date_str, '%Y-%m-%d').date()
        cal = calendar()

        while not cal.is_working_day(correct_date):
            correct_date -= timedelta(days=1)

        return correct_date.strftime("%Y-%m-%d")

    def extract_rate(self, rate_data: Dict, from_currency: str, to_currency: str, eur_to_usd: float) -> Dict:
        rate_cpy = copy.deepcopy(rate_data)
        rate_cpy.update(
            from_currency=from_currency,
            to_currency=to_currency
        )

        if from_currency == 'EUR':
            return rate_cpy
        elif from_currency == 'USD':
            rate_cpy.update(rate=round(rate_cpy.get('rate') / eur_to_usd, 6))
        elif rate_cpy.get('rate'):
            if to_currency == 'EUR':
                rate_cpy.update(rate=round(1 / rate_cpy.get('rate'), 6))
            elif to_currency == 'USD' and eur_to_usd:
                rate_cpy.update(rate=round(1 / rate_cpy.get('rate') * eur_to_usd, 6))
        else:
            rate_cpy.update(rate=0)

        return rate_cpy

    def get_fiat_data(self, since: Optional[str] = '') -> List:
        print(f'Mining fiat currencies for date: {since}')
        rate_list = []

        eur_to_usd = self.get_ecb_rates(since=since).get('rate')

        # Main fiat currencies
        for currency in fiat_currencies:
            rate_data = self.get_ecb_rates(currency, since=since)
            rate_list += [
                self.extract_rate(rate_data, from_currency='EUR', to_currency=currency, eur_to_usd=eur_to_usd),
                self.extract_rate(rate_data, from_currency=currency, to_currency='EUR', eur_to_usd=eur_to_usd),
                self.extract_rate(rate_data, from_currency='USD', to_currency=currency, eur_to_usd=eur_to_usd),
                self.extract_rate(rate_data, from_currency=currency, to_currency='USD', eur_to_usd=eur_to_usd)
            ]

        rate_list += [
            {

                'from_currency': 'EUR',
                'to_currency': 'USD',
                'rate': eur_to_usd
            },
            {
                'from_currency': 'USD',
                'to_currency': 'EUR',
                'rate': 1 / eur_to_usd
            },
            {
                'from_currency': 'USD',
                'to_currency': 'USD',
                'rate': 1
            },
            {
                'from_currency': 'EUR',
                'to_currency': 'EUR',
                'rate': 1
            }
        ]
        # RUB from CBR
        rate_list += [
            *self.get_cbr_rates(currency='USD', since=since),
            *self.get_cbr_rates(currency='EUR', since=since)
        ]

        # Add date in dicts
        [x.update({'date': since}) for x in rate_list]

        return rate_list

    def get_crypto_data(self, since: Optional[str] = '') -> List:

        print(f'Mining crypto currencies for date: {since}')

        rate_list = []

        eur_to_usd = self.get_ecb_rates(since=since).get('rate')

        for currency in crypto_currencies:
            rate_list += [
                *self.get_binance_rates(eur_to_usd, currency=currency, since=since)
            ]

        for coin in stable_coins:
            rate_list += [
                {
                    'from_currency': 'EUR',
                    'to_currency': coin,
                    'rate': eur_to_usd
                },
                {
                    'from_currency': coin,
                    'to_currency': 'EUR',
                    'rate': 1 / eur_to_usd
                },
                {
                    'from_currency': coin,
                    'to_currency': 'USD',
                    'rate': 1
                },
                {
                    'from_currency': 'USD',
                    'to_currency': coin,
                    'rate': 1
                }
            ]

        # Add date in dicts
        [x.update({'date': since}) for x in rate_list]

        return rate_list