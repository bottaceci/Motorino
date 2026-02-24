import logging
from datetime import datetime
from functools import reduce
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length, lit, current_timestamp, to_date

from operazioni import Operazioni  # importa la tua classe Operazioni


class TabellaDipendenti:

    # Costruttore
    def __init__(self, path_csv, path_log, idper):
        self.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("FlussoGenerico")
            .getOrCreate()
        )

        self.idper = idper
        self.path_log = path_log

        # Lettura generica: header dalla prima riga, tutto come stringa
        self.df = (
            self.spark.read
            .option("header", True)  # la prima riga diventa header
            .option("inferSchema", False)  # tutto stringa, conversione dopo
            .csv(path_csv)
        )

        # Aggiungo data inserimento
        self.df = self.df.withColumn("DINS", current_timestamp())

        # Logging configurazione
        logging.basicConfig(
            filename=self.path_log,
            filemode='w',
            level=logging.INFO,
            format="%(message)s",
            force=True
        )

        # Log iniziale
        logging.info(
            "ID_PER=%s, Operazione=Costruttore, Stato=OK, File=%s, Data=%s",
            idper, path_csv, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

    # Funzione che mostra la "tabella".
    def show(self):
        if self.df:
            self.df.show(truncate=False)
            logging.info("ID_PER=%s, Operazione=Show, Stato=OK, Righe=%s, Data=%s",
                         self.idper, self.df.count(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            print("DataFrame vuoto.")
            logging.info("ID_PER=%s, Operazione=Show, Stato=VUOTO, Data=%s",
                         self.idper, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def printSchema(self):
        if self.df:
            self.df.printSchema()
            logging.info(
                "ID_PER=%s, Operazione=printSchema, Stato=OK, Data=%s",
                self.idper,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            print("DataFrame vuoto.")
            logging.info(
                "ID_PER=%s, Operazione=printSchema, Stato=VUOTO, Data=%s",
                self.idper,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

    def givemedataframe(self):
        logging.info("ID_PER=%s, Operazione=givemedataframe, Stato=OK, Data=%s",
                     self.idper, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return self.df

    def pulisci(self, config):
        df = self.df

        # 1. Applica le regole del JSON
        for col_name, rule in config.items():
            if col_name not in df.columns:
                logging.warning(f"Colonna {col_name} non trovata nel DataFrame, salto la validazione")
                continue
            if rule.get("skip_validation", False):
                continue  # salta completamente questa colonna

            rule_type = rule["type"]

            if rule_type == "positive_number":
                df = Operazioni.positive_number(df, col_name)
            elif rule_type == "string_no_numbers":
                df = Operazioni.string_no_numbers(df, col_name)
            elif rule_type == "date":
                df = Operazioni.date_format_regex(df, col_name)
            elif rule_type == "email":
                df = Operazioni.email(df, col_name)
            elif rule_type == "string_length":
                length_val = rule.get("length", 0)
                df = df.withColumn(
                    f"valid_{col_name}",
                    when(col(col_name).isNotNull() & (length(col(col_name)) == length_val), True).otherwise(False)
                )

            # Logging dinamico
            count_valid = df.filter(col(f"valid_{col_name}") == True).count()
            logging.info(
                "ID_PER=%s, Operazione=pulisci, Filtro=%s, Righe OK=%s, Data=%s",
                self.idper, col_name, count_valid, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

        # 2. Colonna finale valid = AND di tutte le valid_<col>
        valid_cols = [f"valid_{c}" for c in config.keys() if f"valid_{c}" in df.columns]
        if valid_cols:
            df = df.withColumn("valid", reduce(lambda a, b: a & b, [col(c) for c in valid_cols]))
        else:
            df = df.withColumn("valid", lit(True))  # se non ci sono colonne, tutto OK

        # 3. DF OK → righe valide con conversioni
        df_ok = df.filter(col("valid") == True)
        for col_name, rule in config.items():
            if col_name not in df_ok.columns:
                continue
            if rule["type"] == "positive_number":
                df_ok = df_ok.withColumn(col_name, col(col_name).cast("double"))
            elif rule["type"] == "date":
                df_ok = df_ok.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))

        df_ok = df_ok.withColumn("ID_PER", lit(self.idper)).withColumn("DINS", current_timestamp())
        df_ok = df_ok.drop(*valid_cols, "valid").select("ID_PER", *[c for c in self.df.columns if c != "DINS"], "DINS")

        # 4. DF SCARTI → righe non valide, tutte stringhe
        df_scarti = df.filter(col("valid") == False)
        df_scarti = df_scarti.withColumn("ID_PER", lit(self.idper)).withColumn("DINS", current_timestamp())
        df_scarti = df_scarti.drop(*valid_cols, "valid").select("ID_PER", *[c for c in self.df.columns if c != "DINS"],
                                                                "DINS")

        # 5. Creo nuovi oggetti TabellaDipendenti
        tabella_ok = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_ok.spark = self.spark
        tabella_ok.df = df_ok
        tabella_ok.idper = self.idper

        tabella_scarti = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_scarti.spark = self.spark
        tabella_scarti.df = df_scarti
        tabella_scarti.idper = self.idper

        return tabella_ok, tabella_scarti
    
class StagingTable:

    def __init__(self, conf, idper, user, pw, dsn, path_log):
        self.spark = (
            SparkSession.builder
                .master("local[1]")
                .appName("FlussoGenerico")
                .getOrCreate()
        )

        self.idper = idper
        self.path_log = path_log
        self.conf = conf
        self.user = user
        self.pw = pw
        self.dsn = dsn

        OJDBC = "/home/ceci/jars/ojdbc17.jar"

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f'--jars "{OJDBC}" '
            f'--driver-class-path "{OJDBC}" '
            "pyspark-shell"
        )

        # Creazione della staging table per il caricamento sul datamart
        self.table_list = []
        for table in self.conf["02_tables"]:
            self.table_list.append(table)

        # Lettura prima tabella
        sql = f"select {",".join(self.conf["02_tables"][self.table_list[0]]["columns"])} from {self.table_list[0]} PARTITION(P_{self.idper})"

        self.df = (
            self.spark.read
                .format("jdbc")
                .option("url", f"jdbc:oracle:thin:@//{self.dsn}")
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("query", sql)
                .option("user", self.user)
                .option("password", self.pw)
                .load()
        )

        # eventuale JOIN altre tabelle
        if len(self.table_list) > 1:
            for t in self.table_list[1:]:
                sql_1 = f"select {",".join(self.conf["02_tables"][t]["columns"])} from {t} PARTITION(P_{self.idper})"

                df_1 = (
                    self.spark.read
                        .format("jdbc")
                        .option("url", f"jdbc:oracle:thin:@//{self.dsn}")
                        .option("driver", "oracle.jdbc.OracleDriver")
                        .option("query", sql_1)
                        .option("user", self.user)
                        .option("password", self.pw)
                        .load()
                )

                self.df = self.df.join(df_1, on=self.conf["02_tables"][t]["join_key"])

        # Logging configurazione
        logging.basicConfig(
            filename=self.path_log,
            filemode='w',
            level=logging.INFO,
            format="%(message)s",
            force=True
        )

        # Log iniziale
        logging.info(
            "ID_PER=%s, Operazione=Costruttore, Stato=OK, Tabelle=%s, Data=%s",
            self.idper, self.table_list, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

    # Funzione che mostra la "tabella".
    def show(self):
        if self.df:
            self.df.show(truncate=False)
            logging.info("ID_PER=%s, Operazione=Show, Stato=OK, Righe=%s, Data=%s",
                         self.idper, self.df.count(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            print("DataFrame vuoto.")
            logging.info("ID_PER=%s, Operazione=Show, Stato=VUOTO, Data=%s",
                         self.idper, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def printSchema(self):
        if self.df:
            self.df.printSchema()
            logging.info(
                "ID_PER=%s, Operazione=printSchema, Stato=OK, Data=%s",
                self.idper,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            print("DataFrame vuoto.")
            logging.info(
                "ID_PER=%s, Operazione=printSchema, Stato=VUOTO, Data=%s",
                self.idper,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

    def givemedataframe(self):
        logging.info("ID_PER=%s, Operazione=givemedataframe, Stato=OK, Data=%s",
                     self.idper, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return self.df

    def prepara_dataset_SCD(self, n_user, n_pw):
        df = self.df
        conf = self.conf

        OLD_columns = [f"OLD_{x}" for x in conf["OLD_columns"]]
        OLD_columns_AS = [f"{x} AS OLD_{x}" for x in conf["OLD_columns"]]

        sql = f"select {",".join(OLD_columns_AS)} from {conf["dimension_table"]} where D_ENDVAL IS NULL"
        df_old = (
            self.spark.read
                .format("jdbc")
                .option("url", f"jdbc:oracle:thin:@//{self.dsn}")
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("query", sql)
                .option("user", n_user)
                .option("password", n_pw)
                .load()
        )

        # join the old data with the new one with a left join
        jk = self.conf["SCD_join_key"]
        joined = df.join(df_old, col(jk)==col(f"OLD_{jk}"), how='left')

        # check if id is null, when it is add those columns to the insert dataframe, and 
        # leave the others in another dataframe to check for SCD - also count them and log the number
        count_id_null = joined.filter(col(f"{OLD_columns[0]}").isNull()).count()
        df_ins = joined.filter(col(f"{OLD_columns[0]}").isNull())
        df_ins = df_ins.drop(*OLD_columns)
        logging.info(
            "ID_PER=%s, Operazione=insert_columns, Filtro=%s, Righe OK=%s, Data=%s",
            self.idper, OLD_columns[0], count_id_null, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        joined = joined.filter(col(f"{OLD_columns[0]}").isNotNull())

        # Check that the IDPER of all new columns is greater than the last one
        joined = Operazioni.greater_than(joined, "ID_PER", "OLD_LAST_ID_PER")

        # Filter out columns with smaller ID_PER and log it
        count_valid_idper = joined.filter(col("greater_ID_PER") == True).count()
        logging.info(
            "ID_PER=%s, Operazione=check_idper, Filtro=%s, Righe OK=%s, Data=%s",
            self.idper, "greater_ID_PER", count_valid_idper, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        joined = joined.filter(col("greater_ID_PER") == True)
        

        # check if the SCD columns have changed and add the respective change_ column
        for c in conf["SCD_columns"]:
            old_col = "OLD_" + c
            joined = Operazioni.equivalence(joined, c, old_col)

            # Logging dinamico
            count_change = joined.filter(col(f"change_{c}") == True).count()
            logging.info(
                "ID_PER=%s, Operazione=check_change, Filtro=%s, Righe cambiate=%s, Data=%s",
                self.idper, c, count_change, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

        # Colonna finale change = OR di tutte le change_<col>
        change_cols = [f"change_{c}" for c in conf["SCD_columns"] if f"change_{c}" in joined.columns]
        if change_cols:
            joined = joined.withColumn("change", reduce(lambda a, b: a | b, [col(c) for c in change_cols]))
        else:
            joined = joined.withColumn("change", lit(True))  # se non ci sono colonne, tutto OK

        # Rename ID column taking out the OLD
        joined = joined.withColumnRenamed(f"{OLD_columns[0]}", f"{conf["OLD_columns"][0]}")
        
        # DF HISTORICAL INSERT -> righe con colonne SCD cambiate
        df_hist_ins = joined.filter(col("change") == True)
        df_hist_ins = df_hist_ins.drop(*OLD_columns, *change_cols, "change", "greater_ID_PER")

        # DF UPDATE -> righe con colonne SCD non cambiate
        df_upd = joined.filter(col("change") == False)
        df_upd = df_upd.drop(*OLD_columns, *change_cols, "change", "greater_ID_PER")

        # DEBUG SHOW
        # joined.show()
        # df_upd.show()
        # df_hist_ins.show()
        # df_ins.show()

        # Creo nuovi oggetti StagingTables
        tabella_ins = StagingTable.__new__(StagingTable)
        tabella_ins.spark = self.spark
        tabella_ins.df = df_ins
        tabella_ins.idper = self.idper
        tabella_ins.conf = self.conf

        tabella_hist_ins = StagingTable.__new__(StagingTable)
        tabella_hist_ins.spark = self.spark
        tabella_hist_ins.df = df_hist_ins
        tabella_hist_ins.idper = self.idper
        tabella_hist_ins.conf = self.conf

        tabella_upd = StagingTable.__new__(StagingTable)
        tabella_upd.spark = self.spark
        tabella_upd.df = df_upd
        tabella_upd.idper = self.idper
        tabella_upd.conf = self.conf

        return tabella_ins, tabella_hist_ins, tabella_upd


