from tools import PostgresConnector, SQLiteConnector

if __name__ == "__main__":
    postgres_connector = PostgresConnector("micro_ami_db")

    postgres_connector.drop_table_if_exists("session")
    postgres_connector.drop_table_if_exists("ip_addresses")
    postgres_connector.drop_table_if_exists("rawPush")

    postgres_connector.create_table_if_not_exists("session",
                                                  """id SERIAL PRIMARY KEY,
                                                              date TIMESTAMPTZ""")


    postgres_connector.create_table_if_not_exists("ip_addresses",
                                                  """id SERIAL PRIMARY KEY,
                                                              src VARCHAR(64),dst VARCHAR(64),
                                                              CONSTRAINT unique_src_dst UNIQUE (src, dst)""")

    postgres_connector.create_table_if_not_exists("rawPush",
                                                  """id SERIAL PRIMARY KEY,
                                                                session_id INT REFERENCES session(id),
                                                                date TIMESTAMPTZ,
                                                                timestamp VARCHAR(16),
                                                                ip_addresses_id INT REFERENCES ip_addresses(id),
                                                                alarm BYTEA""")


    sqlite_connector = SQLiteConnector("database")
    sqlite_connector.sqlite_to_csv("rawPush", "rawPush", "*")
    sqlite_connector.sqlite_to_csv("rawPush", "ip_addresses",  "src, dst", distinct=True)
    sqlite_connector.sqlite_to_csv("session", "session", "*")


    postgres_connector.insert_into("ip_addresses", "src, dst", "%s, %s", conflict=True)
    postgres_connector.insert_into("session", "date", "%s")
    postgres_connector.insert_into("rawPush", "session_id, date, timestamp, ip_addresses_id, alarm", "%s, %s, %s, %s, %s")

    postgres_connector.close()
    sqlite_connector.close()
