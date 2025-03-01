from contextlib import closing
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict
from airflow.models import Variable


class DBWorker:
    def __init__(self):
        # Получаем переменные из Airflow Variables
        self.db_host = Variable.get("POSTGRES_HOST", default_var="host.docker.internal")
        self.db_port = int(Variable.get("POSTGRES_PORT", default_var=5432) or 5432)
        self.db_name = Variable.get("POSTGRES_DB", default_var="currency")
        self.db_user = Variable.get("POSTGRES_USER", default_var="postgres")
        self.db_password = Variable.get("POSTGRES_PASSWORD", default_var=2305)

    def get_pg_creds(self):
        return f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}"

    def prepare_data_for_insert(self, data_list):
        columns = list(data_list[0].keys())
        return [tuple(data[col] for col in columns) for data in data_list]

    def write_to_postgresql(self, data: Dict):
        tuples = self.prepare_data_for_insert(data)

        table_name = 'currency_rates'
        columns = ['from_currency', 'to_currency', 'rate', 'date']

        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s;
        """

        with closing(psycopg2.connect(self.get_pg_creds())) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, tuples, page_size=10000)
                conn.commit()

    def find_many(self, query: str):
        with closing(psycopg2.connect(self.get_pg_creds())) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def get_query(self, path, mapping):
        """
        Забирает SQL-запрос из файла и подставляет значения из mapping.
        """
        with open(path, 'r', encoding='UTF-8') as file:
            query = file.read()
        for key, value in mapping.items():
            query = query.replace(key, str(value))
        return query.split(';')[0]
