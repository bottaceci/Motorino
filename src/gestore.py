import logging
import json
import os
from datetime import datetime
from table import TabellaDipendenti
from db_utils import DBUtils
from pyspark.sql.functions import current_date, col, to_date, when, current_timestamp, monotonically_increasing_id, lit, upper, lower, length, udf


class GestoreFlussoDipendenti:
    def __init__(self, path_log_gestore, path_config_json):
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

    def genera_idper(self, path_csv):
        """
        Estrae ID_PER dai primi 6 caratteri del nome del file CSV.
        Se non è un numero valido, logga l’errore e interrompe l’esecuzione.
        """
        base_name = os.path.basename(path_csv)  # es. '202501_Anagrafica.csv'
        idper_str = base_name.split("_")[0]  # prende '202501'

        if not idper_str.isdigit() or len(idper_str) != 6:
            msg = f"ERRORE: Il file '{base_name}' non contiene un ID_PER valido (YYYYMM) all’inizio."
            self.logger.error(msg)
            raise ValueError(msg)  # interrompe l’esecuzione

        return int(idper_str)

    def processa_flusso(self, path_csv, path_log, flusso_corrente):
        # Genera l’ID_PER dal nome del file CSV
        id_per = self.genera_idper(path_csv)

        # Creo oggetto TabellaDipendenti passando ID_PER
        tabella = TabellaDipendenti(path_csv, path_log, id_per)

        # Recupero il dataframe grezzo
        df_grezzo = tabella.givemedataframe()

        # Recupero la configurazione per il flusso corrente
        if flusso_corrente not in self.config_all:
            msg = f"Flusso {flusso_corrente} non trovato nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)

        config_corrente = self.config_all[flusso_corrente]["columns"]

        # Estraggo il nome della tabella dalla configurazione
        nome_tabella = self.config_all[flusso_corrente]["table"]

        # Estraggo il nome della tabella scarti dalla configurazione
        nome_tabella_scarti = self.config_all[flusso_corrente]["table_scarti"]

        # Applico pulizia automatica
        tab_ok, tab_scarti = tabella.pulisci(config_corrente)

        # Log
        self.logger.info(
            f"ID_PER={id_per} - Tabelle create: df_grezzo={df_grezzo.count()}, "
            f"tab_ok={tab_ok.df.count()}, tab_scarti={tab_scarti.df.count()}, Data={datetime.now()}"
        )

        return df_grezzo, tab_ok, tab_scarti, nome_tabella, nome_tabella_scarti

    def load_to_oracle(self, tab_ok, flusso_corrente, table_name, path_csv, scarti=False, user="VGLSA", password="VGLSA",
                       dsn="localhost:1521/orcl"):
        """
        Carica su Oracle un DataFrame pulito tab_ok, prendendo ID_PER dal nome del file
        e i nomi delle colonne direttamente dal JSON.
        """
        if flusso_corrente not in self.config_all:
            msg = f"Flusso {flusso_corrente} non trovato nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)

        config_corrente = self.config_all[flusso_corrente]["columns"]
        columns_oracle = list(config_corrente.keys())  # nomi colonne come su Oracle

        # Controllo consistenza colonne
        missing_cols = [c for c in columns_oracle if c not in tab_ok.df.columns]
        if missing_cols:
            msg = f"Incoerenza colonne: c'è incoerenza tra il .json e le tabelle oracle {missing_cols}"
            self.logger.error(msg)
            raise ValueError(msg)  # oppure solo log, a seconda di cosa vuoi fare

        tab_ok_df = tab_ok.givemedataframe()

        # Aggiungo ID_PER dal file CSV
        id_per = self.genera_idper(path_csv)
        partition_name = f"P_{id_per}"
        tab_ok_df = tab_ok_df.withColumn("ID_PER", lit(id_per))

        if scarti:
            # Aggiungo data di inserimento
            tab_ok_df = tab_ok_df.withColumn("D_INS", lit(datetime.now()))

            # Aggiungo IDRUN (dummy, vedere se riesco ad inserirlo bene)
            tab_ok_df = tab_ok_df.withColumn("ID_RUN", lit(1))

            # Seleziono solo le colonne indicate nel JSON + ID_PER
            tab_ok_oracle = tab_ok_df.select("ID_RUN", "D_INS", "ID_PER", *columns_oracle)
        else:
            tab_ok_oracle = tab_ok_df.select("ID_PER", *columns_oracle)

        # Converto in lista di tuple Python
        rows = [tuple(row) for row in tab_ok_oracle.collect()]

        # Connessione Oracle e gestione partizione
        db = DBUtils(user=user, password=password, dsn=dsn)
        if scarti:
            db.batch_insert(table_name, ["ID_RUN"] + ["D_INS"] + ["ID_PER"] + columns_oracle, rows)
        else:
            db.ensure_partition(table_name, partition_name, id_per)
            db.batch_insert(table_name, ["ID_PER"] + columns_oracle, rows)
        db.close()

        self.logger.info(f"ID_PER={id_per} - Caricamento completato su Oracle tabella {table_name}")
        print("Caricamento completato!")