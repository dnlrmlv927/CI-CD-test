import psycopg2
from psycopg2 import OperationalError


# Функция для проверки подключения к PostgreSQL
def check_postgres_connection(host, port, dbname, user, password):
    try:
        # Попытка подключения к базе данных PostgreSQL
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

        # Если подключение успешно, выводим сообщение
        print("Подключение к PostgreSQL успешно!")

        # Закрытие соединения
        connection.close()
    except OperationalError as e:
        print("Ошибка подключения:", e)


# Параметры подключения
host = 'localhost'  # Хост базы данных
port = 5432  # Порт, по умолчанию 5432
dbname = 'currency'  # Имя вашей базы данных
user = 'postgres'  # Имя пользователя PostgreSQL
password = 'postgres'  # Пароль пользователя PostgreSQL

# Вызов функции для проверки подключения
check_postgres_connection(host, port, dbname, user, password)
