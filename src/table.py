import logging
from datetime import datetime
from functools import reduce

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