#!/usr/bin/env python
# coding: utf-8

# In[53]:


import csv
from datetime import datetime
import os
import re
import logging
import json
import oracledb
import tempfile
import shutil

from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import current_date, col, to_date, when, current_timestamp, monotonically_increasing_id, lit, upper, lower, length


# In[54]:


class GestoreFlussoDipendenti:
    def __init__(self, path_log_gestore, path_config_json):
        self.contatore_idrun = 0
        self.path_log_gestore = path_log_gestore

        # Logger
        self.logger = logging.getLogger("gestore")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.path_log_gestore, mode='w')
        formatter = logging.Formatter("%(message)s")
        file_handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

        # Carico la configurazione JSON
        with open(path_config_json, 'r') as f:
            self.config_all = json.load(f)

    def genera_idrun(self):
        self.contatore_idrun += 1
        return self.contatore_idrun

    def processa_flusso(self, path_csv, path_log, flusso_corrente):
        idrun = self.genera_idrun()

        # Creo oggetto TabellaDipendenti
        tabella = TabellaDipendenti(path_csv, path_log, idrun)

        # Recupero il dataframe grezzo
        df_grezzo = tabella.givemedataframe()

        # Recupero la configurazione per il flusso corrente
        if flusso_corrente not in self.config_all:
            raise ValueError(f"Flusso {flusso_corrente} non trovato nella configurazione JSON")

        config_corrente = self.config_all[flusso_corrente]["columns"]

        # Applico pulizia automatica
        tab_ok, tab_scarti = tabella.pulisci(config_corrente)

        # Log
        self.logger.info(
            f"IDRUN={idrun} - Tabelle create: df_grezzo={df_grezzo.count()}, "
            f"tab_ok={tab_ok.df.count()}, tab_scarti={tab_scarti.df.count()}, Data={datetime.now()}"
        )

        return df_grezzo, tab_ok, tab_scarti


# In[55]:


class Operazioni:
    
    '''Static method = Significa che puoi chiamare il metodo senza creare un oggetto della classe!'''
    
    @staticmethod
    def is_number(df, col_name):
        return df.withColumn(
            f"valid_{col_name}_number",
            when(col(col_name).rlike("^[0-9]+(\\.[0-9]+)?$"), True).otherwise(False)
        )
    
    @staticmethod
    def positive_number(df, col_name):
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).rlike("^[0-9]+(\\.[0-9]+)?$") & (col(col_name).cast("double") >= 0), True).otherwise(False)
        )

    @staticmethod
    def string_no_numbers(df, col_name):
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).isNotNull() & (~col(col_name).rlike("[0-9]")), True).otherwise(False)
        )

    @staticmethod
    def date_format_regex(df, col_name):
        # Estrazione anno, mese, giorno come interi
        anno = col(col_name).substr(1,4).cast("int")
        mese = col(col_name).substr(6,2).cast("int")
        giorno = col(col_name).substr(9,2).cast("int")

        # Funzione di validazione mese/giorno
        valid_day = (
            ((mese.isin([1,3,5,7,8,10,12])) & (giorno.between(1,31))) |  # mesi con 31 giorni
            ((mese.isin([4,6,9,11])) & (giorno.between(1,30))) |          # mesi con 30 giorni
            ((mese == 2) & (giorno.between(1,28))) |                      # febbraio normale
            ((mese == 2) & (giorno == 29) & ((anno % 4 == 0) & ((anno % 100 != 0) | (anno % 400 == 0))))  # bisestile
            )

        return df.withColumn(
            f"valid_{col_name}",
            when(
                (col(col_name).rlike(r'^\d{4}-\d{2}-\d{2}$')) & valid_day,
                True
            ).otherwise(False)
        )

    @staticmethod
    def email(df, col_name):
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return df.withColumn(
            f"valid_{col_name}",
            when(col(col_name).rlike(pattern), True).otherwise(False)
        )
    
    @staticmethod
    def string_length(df, col_name, length_val):
        """
        Controlla che la colonna col_name abbia esattamente length_val caratteri.
        """
        return df.withColumn(
            f"valid_{col_name}",
            when(
                col(col_name).isNotNull() & (length(col(col_name)) == length_val),
                True
            ).otherwise(False)
        )


# In[68]:


class TabellaDipendenti:

    # Costruttore
    def __init__(self, path_csv, path_log, idrun):
            self.spark = (
                SparkSession.builder
                .master("local[1]")
                .appName("FlussoGenerico")
                .getOrCreate()
            )

            self.idrun = idrun
            self.path_log = path_log

            # Lettura generica: header dalla prima riga, tutto come stringa
            self.df = (
                self.spark.read
                .option("header", True)   # la prima riga diventa header
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
                "IDRUN=%s, Operazione=Costruttore, Stato=OK, File=%s, Data=%s",
                idrun, path_csv, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        
    # Funzione che mostra la "tabella". 
    def show(self):
        if self.df:
            self.df.show(truncate=False)
            logging.info("IDRUN=%s, Operazione=Show, Stato=OK, Righe=%s, Data=%s", 
             self.idrun, self.df.count(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            print("DataFrame vuoto.")
            logging.info("IDRUN=%s, Operazione=Show, Stato=VUOTO, Data=%s", 
             self.idrun, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def printSchema(self):
        if self.df:
            self.df.printSchema()
            logging.info(
                "IDRUN=%s, Operazione=printSchema, Stato=OK, Data=%s",
                self.idrun,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            print("DataFrame vuoto.")
            logging.info(
                "IDRUN=%s, Operazione=printSchema, Stato=VUOTO, Data=%s",
                self.idrun,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            
    def givemedataframe(self):
        logging.info("IDRUN=%s, Operazione=givemedataframe, Stato=OK, Data=%s", 
             self.idrun, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return self.df
    
    def pulisci(self, config):
        df = self.df

        # 1. Applica le regole del JSON
        for col_name, rule in config.items():
            if col_name not in df.columns:
                logging.warning(f"Colonna {col_name} non trovata nel DataFrame, salto la validazione")
                continue

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
                "IDRUN=%s, Operazione=pulisci, Filtro=%s, Righe OK=%s, Data=%s",
                self.idrun, col_name, count_valid, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

        df_ok = df_ok.withColumn("IDRUN", lit(self.idrun)).withColumn("DINS", current_timestamp())
        df_ok = df_ok.drop(*valid_cols, "valid").select("IDRUN", *[c for c in self.df.columns if c != "DINS"], "DINS")

        # 4. DF SCARTI → righe non valide, tutte stringhe
        df_scarti = df.filter(col("valid") == False)
        df_scarti = df_scarti.withColumn("IDRUN", lit(self.idrun)).withColumn("DINS", current_timestamp())
        df_scarti = df_scarti.drop(*valid_cols, "valid").select("IDRUN", *[c for c in self.df.columns if c != "DINS"], "DINS")

        # 5. Creo nuovi oggetti TabellaDipendenti
        tabella_ok = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_ok.spark = self.spark
        tabella_ok.df = df_ok
        tabella_ok.idrun = self.idrun

        tabella_scarti = TabellaDipendenti.__new__(TabellaDipendenti)
        tabella_scarti.spark = self.spark
        tabella_scarti.df = df_scarti
        tabella_scarti.idrun = self.idrun

        return tabella_ok, tabella_scarti


# In[71]:


path_csv = "../data/unitacr.csv"
path_log = "../logs/tlog.log"
idrun = 1
tabella_grezzo = TabellaDipendenti(path_csv, path_log, idrun)


# In[72]:


tabella_grezzo.show()


# In[73]:


path_config_json = "../config/config_flussi.json"
path_log_gestore = "../logs/tlog_gestore.log"
gestore = GestoreFlussoDipendenti(path_log_gestore, path_config_json)

path_log = "../logs/tlog.log"
path_flusso = "../data/unitacr.csv"
chiave_json = "unitacr"
df_grezzo, tab_ok, tab_scarti = gestore.processa_flusso(path_flusso, path_log, chiave_json)


# In[74]:


tab_ok.show()


# In[75]:


unitacr = tab_ok.givemedataframe()
unitacr.select("*").where(col("COD_ACR") == "RSA").show()


# In[76]:


tab_ok.printSchema()


# In[77]:


tab_ok = tab_ok.givemedataframe()


# In[28]:


table_name = "GRP02_UNT"
IDRUN = 1
partition_name = f"P_{IDRUN}"

# Connessione e cursore gestiti automaticamente
with oracledb.connect(user="VGLSA", password="VGLSA", dsn="localhost:1521/orcl") as conn:
    with conn.cursor() as cur:

        stmt_check = f"""
        SELECT COUNT(*)
        FROM user_tab_partitions
        WHERE table_name = '{table_name.upper()}'
          AND partition_name = '{partition_name.upper()}'
        """

        cur.execute(stmt_check)
        v_count = cur.fetchone()[0]
        print("Partizione trovata:", v_count)

        if v_count == 1:
            cur.execute(f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}")
            print(f"Truncate partizione {partition_name}")
        else:
            cur.execute(f"""
                ALTER TABLE {table_name}
                SPLIT PARTITION P_DEFAULT
                VALUES ({IDRUN})
                INTO (PARTITION {partition_name}, PARTITION P_DEFAULT)
            """)
            print(f"Creata nuova partizione {partition_name}")

        conn.commit()  # commit finale


# In[29]:


# STEP 1 - Adeguo le colonne al modello Oracle
# Oracle vuole: ID_PER, COD_UNT, DESC_UNT
tab_ok_for_oracle = (
    tab_ok
    .withColumnRenamed("IDRUN", "ID_PER")
    .withColumnRenamed("COD_ACR", "DESC_UNT")
    .select("ID_PER", "COD_UNT", "DESC_UNT")
)


# In[30]:


# Converte da Spark DataFrame a lista di tuple Python
rows = [ (row.ID_PER, row.COD_UNT, row.DESC_UNT) for row in tab_ok_for_oracle.collect() ]


# In[31]:


conn = oracledb.connect(
    user="VGLSA",
    password="VGLSA",
    dsn="localhost:1521/orcl"
)
cur = conn.cursor()

# STEP 3 - Inserimento batch
sql_insert = f"""
    INSERT INTO GRP02_UNT (ID_PER, COD_UNT, DESC_UNT)
    VALUES (:1, :2, :3)
"""

cur.executemany(sql_insert, rows)
conn.commit()

# STEP 4 - Chiudi connessione
cur.close()
conn.close()

print("Caricamento completato!")


# In[ ]:




