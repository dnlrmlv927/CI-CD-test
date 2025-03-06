import sys
sys.path.append(r'/home/danilssau6364/airflow')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import traceback
from modules.miner_currency import CurrencyMiner
from modules.db_worker import DBWorker


# 🔹 Данные для Telegram
TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID", default_var=None)

# 🔹 Функция отправки ошибки в Telegram
def send_failure_alert(context):
    """Отправляет уведомление в Telegram при падении таски."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['ds']
    error_message = str(context['exception'])  # <-- Добавлен `str()`, чтобы избежать ошибок при форматировании

    message = f"""
❌ *Ошибка в DAG:* `{dag_id}`
📌 *Таска:* `{task_id}`
📅 *Дата выполнения:* `{execution_date}`
⚠️ *Ошибка:* `{error_message}`
    """

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})

# 🔹 Функция уведомления об успешном выполнении DAG
def send_success_alert(context):
    """Отправляет уведомление в Telegram при успешном завершении DAG."""
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['ds']

    message = f"""
✅ *DAG завершен успешно:* `{dag_id}`
🚀 *Run ID:* `{run_id}`
📅 *Дата:* `{execution_date}`
    """

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})


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

# jinja
def mine_data(**kwargs):
    """Функция для майнинга данных с валютного API."""
    execution_date = kwargs['ds']  # Получаем дату запуска DAG (YYYY-MM-DD)

    try:
        print(f"Запуск майнинга данных за {execution_date}")
        data = miner.get_fiat_data(since=execution_date)
        db.write_to_postgresql(data)
        print(f"Данные за {execution_date} успешно загружены в БД.")
    except Exception as e:
        print(f"Ошибка при майнинге фиатных валют за {execution_date}:")
        print(traceback.format_exc())


# Создание DAG выф
with DAG(
        'dag_fiat_mine_currency_data',
        default_args=DEFAULT_ARGS,
        schedule_interval=SCHEDULE_INTERVAL,
        catchup=True,
        tags=['currency', 'cbr', 'ecb'],
        dagrun_timeout=timedelta(minutes=60),
) as dag:
    task_mine_currency = PythonOperator(
        task_id='fiat_mine_currency_data',
        python_callable=mine_data,
        on_success_callback=send_success_alert,
        on_failure_callback=send_failure_alert
    )

    task_mine_currency  # Выполнение задачи
