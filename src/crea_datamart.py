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

        # apply process_dimension function contained in the json config, return a tuple called 'dataframes'
        # apply load_dimension function contained in the json config, where the tuple gets split accordingly inside
        # I guess it could be only one function for the processing and the loading?


        # check SCD flag - flag can have value SCD, MERGE, STAT (as in static)
        #   if SCD, load 02 table from VGLSA, apply lookup, add validation column and filter, creating 3 dataframes, historical insert, update, and insert
        #   then insert them accordingly, looking at what I did with datastage
        #   actually, since the unita table needs denormalization, it would just be better to write a distinct function for every dimension, it's harder to put them together
        #   still, copy the procedures I did in datastage, but to get exercise in pyspark joins, do not do the denormalization in the select statement, 
        #   get all the necessary tables and do it in pyspark

        dimensione = dimensione.strip()
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