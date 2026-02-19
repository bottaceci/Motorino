import oracledb

class DBUtils:
    def __init__(self, user, password, dsn):
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.cur = self.conn.cursor()

    def ensure_partition(self, table_name, partition_name, idper):
        """
        Truncate se la partizione esiste, altrimenti crea una nuova partizione LIST per idper.
        """
        # Controlla se la partizione esiste
        stmt_check = f"""
        SELECT COUNT(*)
        FROM user_tab_partitions
        WHERE table_name = '{table_name.upper()}'
          AND partition_name = '{partition_name.upper()}'
        """
        self.cur.execute(stmt_check)
        v_count = self.cur.fetchone()[0]

        if v_count == 1:
            # La partizione esiste → truncate
            self.cur.execute(f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}")
            print(f"Truncate partizione {partition_name}")
        else:
            # La partizione non esiste → add partition
            self.cur.execute(f"""
                ALTER TABLE {table_name}
                SPLIT PARTITION P_DEFAULT
                VALUES ({idper})
                INTO (PARTITION {partition_name}, PARTITION P_DEFAULT)
            """)
            print(f"Creata nuova partizione {partition_name}")

        self.conn.commit()



    def batch_insert(self, table_name, columns, rows):
        """
        Inserisce una lista di tuple in una tabella Oracle.
        """
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        sql_insert = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        self.cur.executemany(sql_insert, rows)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()