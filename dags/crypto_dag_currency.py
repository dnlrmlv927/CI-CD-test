import sys
sys.path.append(r'/opt/airflow')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import traceback
from modules.miner_currency import CurrencyMiner
from modules.db_worker import DBWorker
# sadsd

# Определение дат
START_DATE = datetime(2025, 1, 1)  # Начало с 1 января 2025
SCHEDULE_INTERVAL = '0 6 * * *'  # Запуск каждый день в 6:00 утра (UTC)
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация объектов
miner = CurrencyMiner()
db = DBWorker()


def mine_data(**kwargs):
    """Функция для майнинга данных с валютного API."""
    execution_date = kwargs['ds']  # Получаем дату запуска DAG (YYYY-MM-DD)

    try:
        print(f"Запуск майнинга данных за {execution_date}")
        data = miner.get_crypto_data(since=execution_date)
        db.write_to_postgresql(data)
        print(f"Данные за {execution_date} успешно загружены в БД.")
    except Exception as e:
        print(f"Ошибка при майнинге фиатных валют за {execution_date}:")
        print(traceback.format_exc())


# Создание DAG
with DAG(
        'dag_crypto_mine_currency_data',
        default_args=DEFAULT_ARGS,
        schedule_interval=SCHEDULE_INTERVAL,
        catchup=True,  # Догоняет пропущенные дни, если DAG отключен
        tags=['binance', 'crypto'],
) as dag:
    task_mine_currency = PythonOperator(
        task_id='crypto_mine_currency_data',
        python_callable=mine_data,
        provide_context=True,  # Позволяет передавать execution_date
    )

    task_mine_currency  # Выполнение задачи
