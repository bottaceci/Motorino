import logging
import json
import os
from datetime import datetime
from table import TabellaDipendenti, StagingTable, FactTable
from db_utils import DBUtils
from pyspark.sql.functions import current_date, col, to_date, when, current_timestamp, monotonically_increasing_id, lit, upper, lower, length, udf
from pyspark.sql import types as T


class GestoreFlusso:
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


class GestoreDimensioni:
    def __init__(self, path_log_gestore, path_config_json, idper, user, pw, dsn, n_user, n_pw):
        self.path_log_gestore = path_log_gestore
        self.idper = idper
        self.user = user
        self.pw = pw
        self.dsn = dsn
        self.n_user = n_user
        self.n_pw = n_pw
        self.db = DBUtils(user=self.n_user, password=self.n_pw, dsn=self.dsn) 

        # Logger 
        self.logger = logging.getLogger("gestore")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.path_log_gestore, mode='w')
        formatter = logging.Formatter("%(message)s")
        file_handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

        # Load JSON configuration
        with open(path_config_json, 'r') as f:
            self.config_all = json.load(f)

        # Enable reading and writing from Oracle
        OJDBC = "/home/ceci/jars/ojdbc17.jar"

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f'--jars "{OJDBC}" '
            f'--driver-class-path "{OJDBC}" '
            "pyspark-shell"
        )

    def process_dimension(self, path_log, current_dimension):
        # Get configuration for current dimension
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]

        # Creare oggetto StagingTable
        tabella = StagingTable(config_corrente,
                               self.idper,
                               user = self.user,
                               pw = self.pw,
                               dsn = self.dsn,
                               path_log=path_log)

        # Ricavare dataframe
        df_grezzo = tabella.givemedataframe()

        # Estrarre nome tabella sul DM
        nome_tabella = config_corrente["dimension_table"]

        # Eventualmente ricavare i vari dataset (se esiste la key 'SCD_columns')
        if "SCD_columns" in config_corrente:
            tab_ins, tab_hist_ins, tab_upd = tabella.prepara_dataset_SCD(n_user=self.n_user, n_pw=self.n_pw)

            # Log
            self.logger.info(
                f"ID_PER={self.idper} - Tabelle create: tab_ins={tab_ins.df.count()}, "
                f"tab_hist_ins={tab_hist_ins.df.count()}, tab_upd={tab_upd.df.count()}, Data={datetime.now()}"
            )

            return {"df_grezzo":df_grezzo, "tab_ins":tab_ins, 
                    "tab_hist_ins":tab_hist_ins, "tab_upd":tab_upd, 
                    "nome_tabella":nome_tabella}

        else:
            # Log
            self.logger.info(
                f"ID_PER={self.idper} - Tabelle create: tabella={tabella.df.count()}, "
                f"Data={datetime.now()}"
            )

            return {"tab_mrg":tabella, "nome_tabella":nome_tabella}



    def load_ins_to_oracle(self, table, table_name, current_dimension):
        """
        Carica su Oracle i record dati in input in modalita INSERT
        """
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]
        columns_oracle = config_corrente["dimension_columns"]

        table_df = table.givemedataframe()

        # Aggiungo D_INIVAL
        table_df = table_df.withColumn("D_INIVAL", lit(self.idper))

        # Aggiungo D_ENDVAL 
        table_df = table_df.withColumn("D_ENDVAL", lit(None).cast(T.IntegerType()))

        # Aggiungo D_INS
        table_df = table_df.withColumn("D_INS", lit(datetime.now()))

        # Aggiungo D_UPD
        table_df = table_df.withColumn("D_UPD", lit(None).cast("date"))

        # Aggiungo LAST_ID_PER
        table_df = table_df.withColumn("LAST_ID_PER", lit(self.idper))

        # Elimino ID_PER
        table_df = table_df.drop("ID_PER")


        # Controllo consistenza colonne
        missing_cols = [c for c in columns_oracle[1:] if c not in table_df.columns]
        if missing_cols:
            msg = f"Incoerenza colonne: c'è incoerenza tra il .json e le tabelle oracle {missing_cols}"
            self.logger.error(msg)
            raise ValueError(msg)  # oppure solo log, a seconda di cosa vuoi fare

        # Write to database
        table_df.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=table_name,
                            mode='append',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })

    def load_hist_ins_to_oracle(self, table, table_name, current_dimension):
        """
        Carica su Oracle i record dati in input come insert storico, ossia
        inserisce la nuova versione del record e aggiorna la precedente per
        storicizzarla
        """
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]
        columns_oracle = config_corrente["dimension_columns"]

        table_df_ins = table.givemedataframe()
        table_df_upd = table.givemedataframe()

        # 1. INSERT

        # Elimino l'ID
        table_df_ins = table_df_ins.drop(config_corrente["dimension_columns"][0])

        # Elimino ID_PER
        table_df_ins = table_df_ins.drop("ID_PER")

        # Aggiungo D_INIVAL
        table_df_ins = table_df_ins.withColumn("D_INIVAL", lit(self.idper))

        # Aggiungo D_ENDVAL 
        table_df_ins = table_df_ins.withColumn("D_ENDVAL", lit(None).cast(T.IntegerType()))

        # Aggiungo D_INS
        table_df_ins = table_df_ins.withColumn("D_INS", lit(datetime.now()))

        # Aggiungo D_UPD
        table_df_ins = table_df_ins.withColumn("D_UPD", lit(None).cast("date"))

        # Aggiungo LAST_ID_PER
        table_df_ins = table_df_ins.withColumn("LAST_ID_PER", lit(self.idper))

        # Controllo consistenza colonne
        missing_cols = [c for c in columns_oracle[1:] if c not in table_df_ins.columns]
        if missing_cols:
            msg = f"Incoerenza colonne: c'è incoerenza tra il .json e le tabelle oracle {missing_cols}"
            self.logger.error(msg)
            raise ValueError(msg)  # oppure solo log, a seconda di cosa vuoi fare

        # Write to database
        table_df_ins.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=table_name,
                            mode='append',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })
        
        # 2. UPDATE

        ID_NAME = config_corrente["dimension_columns"][0]

        # Put update set on staging table
        table_df_upd.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=f"{table_name}_STG",
                            mode='overwrite',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })

        # Run UPDATE statement in Oracle with OracleDB
        update_sql = f"""
            UPDATE {table_name} t
            SET
                t.D_ENDVAL = (
                    SELECT TO_NUMBER(
                            TO_CHAR(
                            ADD_MONTHS(TO_DATE(s.ID_PER, 'YYYYMM'), -1),
                            'YYYYMM'
                            )
                        )
                    FROM {table_name}_STG s
                    WHERE s.{ID_NAME} = t.{ID_NAME}
                ),
                t.D_UPD = SYSDATE
            WHERE EXISTS (
                SELECT 1
                FROM {table_name}_STG s
                WHERE s.{ID_NAME} = t.{ID_NAME}
                )
        """

        self.db.run_statement(update_sql)


    def load_upd_to_oracle(self, table, table_name, current_dimension):
        """
        Carica su Oracle i record dati in input in modalita UPDATE
        """
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]

        temp = config_corrente["SCD_columns"]
        temp.append(config_corrente["SCD_join_key"])
        SCD_columns = temp + ["D_UPD", "D_INS", "D_INIVAL", "D_ENDVAL", "LAST_ID_PER"]
        columns_oracle = [x for x in config_corrente["dimension_columns"] if x not in SCD_columns]

        table_df_upd = table.givemedataframe()

        ID_NAME = config_corrente["dimension_columns"][0]

        # Put update set on staging table
        table_df_upd.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=f"{table_name}_STG",
                            mode='overwrite',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })

        # Run UPDATE statement in Oracle with OracleDB

        #table_df_upd.show()

        update_sql = f"""
            MERGE INTO {table_name} A
            USING {table_name}_STG B
            ON (A.{ID_NAME} = B.{ID_NAME})
            WHEN MATCHED THEN
            UPDATE SET
                {",".join([f"A.{x} = B.{x}" for x in columns_oracle[1:]])},
                A.LAST_ID_PER = B.ID_PER,
                A.D_UPD = SYSDATE
        """

        self.db.run_statement(update_sql)

    def load_merge_to_oracle(self, table, table_name, current_dimension):
        """
        Carica su Oracle i record dati con una MERGE, in modalita UPDATE, then INSERT
        """
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]

        MERGE_columns = [config_corrente["merge_key"]] + ["D_UPD", "D_INS", "LAST_ID_PER"]
        columns_oracle_upd = [x for x in config_corrente["dimension_columns"] if x not in MERGE_columns]
        columns_oracle_ins = [x for x in config_corrente["dimension_columns"] if x not in ["D_UPD", "D_INS", "LAST_ID_PER"]]

        table_df_mrg = table.givemedataframe()

        ID_NAME = config_corrente["dimension_columns"][0]

        # Put merge set on staging table
        table_df_mrg.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=f"{table_name}_STG",
                            mode='overwrite',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })

        # Run MERGE statement in Oracle with OracleDB

        merge_sql = f"""
            MERGE INTO {table_name} A
            USING {table_name}_STG B
            ON (A.{config_corrente["merge_key"]} = B.{config_corrente["merge_key"]})
            WHEN MATCHED THEN
            UPDATE SET
                {",".join([f"A.{x} = B.{x}" for x in columns_oracle_upd])},
                A.LAST_ID_PER = B.ID_PER,
                A.D_UPD = SYSDATE
            WHEN NOT MATCHED THEN
            INSERT (
                {",".join(config_corrente["dimension_columns"])}
                )
            VALUES (
                {",".join([f"B.{x}" for x in columns_oracle_ins])},
                SYSDATE,
                NULL,
                B.ID_PER
            )
        """

        self.db.run_statement(merge_sql)

    def load_periodo(self, initend):
        config_corrente = self.config_all["periodo"]
        self.db.run_procedure("P_LOAD_PERIOD", config_corrente["initend"])

    def process_fact_table(self, path_log, current_dimension):
        if current_dimension not in self.config_all:
            msg = f"Dimensione {current_dimension} non trovata nella configurazione JSON"
            self.logger.error(msg)
            raise ValueError(msg)
        
        config_corrente = self.config_all[current_dimension]

        # Creare oggetto FactTable
        tabella = FactTable(config_corrente,
                            self.idper,
                            user = self.user,
                            pw = self.pw,
                            n_user=self.n_user,
                            n_pw=self.n_pw,
                            dsn = self.dsn,
                            path_log=path_log)
        
        # Ricavare dataframe
        df_grezzo = tabella.givemedataframe()

        # Controllo consistenza colonne
        missing_cols = [c for c in config_corrente["fact_columns"] if c not in df_grezzo.columns]
        if missing_cols:
            msg = f"Incoerenza colonne: c'è incoerenza tra il .json e le tabelle oracle {missing_cols}"
            self.logger.error(msg)
            raise ValueError(msg)

        # Selezionare colonne corrette
        fact_table = df_grezzo.select(*config_corrente["fact_columns"])

        # Estrarre nome tabella sul DM
        nome_tabella = config_corrente["fact_table"]

        # Write to database
        fact_table.write.jdbc(url=f"jdbc:oracle:thin:@//{self.dsn}", 
                            table=nome_tabella,
                            mode='append',
                            properties={"driver": "oracle.jdbc.OracleDriver",
                                        "user": self.n_user,
                                        "password": self.n_pw
                            })

        # return tabella, nome_tabella




