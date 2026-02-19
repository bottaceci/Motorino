import oracledb
from pyspark.sql.functions import col

from table import TabellaDipendenti
from gestore import GestoreFlussoDipendenti
from db_utils import DBUtils

def main():
    path_log_gestore = "../logs/tlog_gestore.log"
    path_config_json = "../config/config_flussi.json"

    # Gestore flusso
    gestore = GestoreFlussoDipendenti(path_log_gestore, path_config_json)

    path_csv = "../data/202501_presenze.csv"
    path_log = "../logs/tlog.log"
    chiave_json = "presenze"

    df_grezzo, tab_ok, tab_scarti = gestore.processa_flusso(path_csv, path_log, chiave_json)
    gestore.load_to_oracle(tab_ok, chiave_json, "GRP02_PRS", path_csv)

if __name__ == "__main__":
    main()
