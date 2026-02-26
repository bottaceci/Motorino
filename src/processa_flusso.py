import argparse
from gestore import GestoreFlusso

parser = argparse.ArgumentParser()
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--dsn", required=True)
parser.add_argument("--idper", required=True)
args = parser.parse_args()

path_log_gestore = "../logs/tlog_gestore.log"
path_config_json = "../config/config_flussi.json"
path_file_flussi = "../config/anagrafica_flussi.txt"
path_log = "../logs/tlog.log"

gestore = GestoreFlusso(path_log_gestore, path_config_json)

with open(path_file_flussi) as f:
    for flusso in f:
        flusso = flusso.strip()  # take out line break
        path_csv = f"../data/{args.idper}_{flusso}.csv"
        df_grezzo, tab_ok, tab_scarti, nome_tabella, nome_tabella_scarti = gestore.processa_flusso(path_csv, path_log, flusso)
        gestore.load_to_oracle(tab_ok, 
                               flusso, 
                               nome_tabella, 
                               path_csv,
                               user=args.user, 
                               password=args.password, 
                               dsn=args.dsn)
        gestore.load_to_oracle(tab_scarti,
                               flusso, 
                               nome_tabella_scarti, 
                               path_csv,
                               scarti=True,
                               user=args.user, 
                               password=args.password, 
                               dsn=args.dsn)
        print(f"[INFO] Flusso '{flusso}' caricato correttamente su Oracle.")

gestore.close_spark_session()

