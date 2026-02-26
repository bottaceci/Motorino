import argparse
from gestore import GestoreDimensioni
from db_utils import DBUtils

parser = argparse.ArgumentParser()
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--n_user", required=True)
parser.add_argument("--n_password", required=True)
parser.add_argument("--dsn", required=True)
parser.add_argument("--idper", required=True)
args = parser.parse_args()

path_log_gestore = "../logs/tlog_gestore_dm.log"
path_config_json = "../config/config_flussi_dm.json"
path_file_dimensioni = "../config/anagrafica_flussi_dm.txt"
path_log = "../logs/tlog_dm.log"

gestore = GestoreDimensioni(path_log_gestore, 
                            path_config_json, 
                            args.idper,
                            args.user,
                            args.password,
                            args.dsn,
                            args.n_user,
                            args.n_password)

with open(path_file_dimensioni) as f:
    for dimensione in f: # L'ultima dimensione dell'elenco e la tabella dei fatti
        # get json configuration
        # Se la dimensione è uguale alla tabella dei fatti, usare funzione apposita, lo stesso per il periodo. Tutti gli altri sono flussi generici.

        dimensione = dimensione.strip()

        if dimensione == "periodo":
            gestore.load_periodo()
        elif dimensione == "presenze":  # chiamare funzione tabella fatti
            fact_table, nome_fact_table = gestore.process_fact_table(path_log, dimensione)
        else:
            dataframes = gestore.process_dimension(path_log, dimensione)

            if 'tab_ins' in dataframes:
                gestore.load_ins_to_oracle(dataframes["tab_ins"],
                                        dataframes["nome_tabella"],
                                        dimensione)
            if 'tab_hist_ins' in dataframes:
                gestore.load_hist_ins_to_oracle(dataframes["tab_hist_ins"],
                                        dataframes["nome_tabella"],
                                        dimensione)
            if 'tab_upd' in dataframes:
                gestore.load_upd_to_oracle(dataframes["tab_upd"],
                                        dataframes["nome_tabella"],
                                        dimensione)
            if 'tab_mrg' in dataframes:
                gestore.load_merge_to_oracle(dataframes["tab_mrg"],
                                        dataframes["nome_tabella"],
                                        dimensione)

        print(f"[INFO] Dimensione '{dimensione}' caricato correttamente su Oracle.")

# Close the spark session cleanly
gestore.close_spark_session()