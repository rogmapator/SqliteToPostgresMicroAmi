import ast
import sqlite3

import psycopg2
from psycopg2.extras import execute_batch
import csv


class PostgresConnector:
    def __init__(self, db_name):
        self.dict_ip_addresses = None
        self.pg_conn, self.pg_cur = self._connect_to_db(db_name)

    @staticmethod
    def _connect_to_db(db_name):
        pg_conn = psycopg2.connect(
            dbname=db_name,
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        pg_cur = pg_conn.cursor()
        return pg_conn, pg_cur

    def drop_table_if_exists(self, table_name):
        self.pg_cur.execute(f"""
            DROP TABLE IF EXISTS {table_name} CASCADE;
        """)
        self.commit()

    def create_table_if_not_exists(self, table_name, sql_values):
        self.pg_cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {sql_values});
        """)
        self.commit()

    def insert_into(self, table_name, fields, values, conflict=False):
        if conflict:
            execute_batch(
                self.pg_cur,
                f"INSERT INTO {table_name} ({fields}) VALUES ({values}) ON CONFLICT ({fields}) DO NOTHING",
                self._prepare_data_from_csv(table_name, fields)
            )
        else:
            execute_batch(
                self.pg_cur,
                f"INSERT INTO {table_name} ({fields}) VALUES ({values})",
                self._prepare_data_from_csv(table_name, fields)
            )

        self.commit()

    def _prepare_data_from_csv(self, table_name, fields):
        with open(f"{table_name}.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if table_name == "rawPush":
                fields = fields.split(",")
                fields[0] = "session"
                fields.pop(3)
                fields.pop(-1)
                data = [
                    (*[row[field.strip()].strip() for field in fields],
                     self.get_ip_addresses_id(row['src'].strip(), row['dst'].strip(), self.dict_ip_addresses),
                     ast.literal_eval(row['data'].strip()),
                     )
                    for row in reader
                ]
            else:
                data = [
                    (*[row[field.strip()].strip() for field in fields.split(",")],)
                    for row in reader
                ]
            if table_name == "ip_addresses":
                self.dict_ip_addresses = {(src, dst): idx for idx, (src, dst) in enumerate(data, start=1)}
        data = data
        return data

    @staticmethod
    def get_ip_addresses_id(src, dst, ip_addr):
        id_num = ip_addr.get((src, dst))
        return id_num

    def commit(self):
        self.pg_conn.commit()

    def close(self):
        self.pg_conn.close()


class SQLiteConnector:
    def __init__(self, db_name):
        self.sqlite_con, self.sqlite_cur = self._connect_to_db(db_name)

    @staticmethod
    def _connect_to_db(db_name):
        sqlite_con = sqlite3.connect(f"input_file/{db_name}.sqlite3")
        sqlite_cur = sqlite_con.cursor()
        return sqlite_con, sqlite_cur

    def sqlite_to_csv(self, table_name, csv_name, fields, distinct=False):
        self._select_from(table_name, fields, distinct)
        with open(f"{csv_name}.csv", "w", newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([i[0] for i in self.sqlite_cur.description])
            writer.writerows(self.sqlite_cur.fetchall())

    def _select_from(self, table_name, fields, distinct=False):
        self.sqlite_cur.execute(f"SELECT {'DISTINCT' if distinct else ''} {fields} FROM {table_name}")

    def close(self):
        self.sqlite_con.close()