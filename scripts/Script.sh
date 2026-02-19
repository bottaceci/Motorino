#!/bin/bash

# -----------------------------
# Parametri da linea di comando
# -----------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --user) O_USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --dsn) DSN="$2"; shift 2 ;;
        --idper) IDPER="$2"; shift 2 ;;
        *) echo "Parametro sconosciuto $1"; exit 1 ;;
    esac
done

# # If DSN does not work, try setting it with these lines - it might have changed
# WIN_WSL_IP="$(
#   powershell.exe -NoProfile -Command \
#     "(Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias 'vEthernet (WSL (Hyper-V firewall))' -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty IPAddress)" \
#   | tr -d '\r'
# )"

# if [ -z "$WIN_WSL_IP" ]; then
#   WIN_WSL_IP="$(awk '/^nameserver[[:space:]]+/ {print $2; exit}' /etc/resolv.conf)"
# fi

# DSN="${DSN:-${WIN_WSL_IP:-172.23.64.1}:1521/orcl}"

# Parametri di default se mancanti
O_USER="${O_USER:-VGLSA}"
PASSWORD="${PASSWORD:-VGLSA}"
DSN="${DSN:-172.23.64.1:1521/orcl}"  # switched 'localhost' with '172.23.64.1', the Windows host/gateway IP for wsl - use localhost if on linux

# -----------------------------
# Percorsi
# -----------------------------
PATH_BASE="../"
PATH_LOG="${PATH_BASE}logs/script_log_$(date +%Y%m%d_%H%M%S).log"
PATH_CONFIG_JSON="${PATH_BASE}config/config_flussi.json"
PATH_FILE_FLUSSI="${PATH_BASE}config/anagrafica_flussi.txt"

# -----------------------------
# Funzione log
# -----------------------------
function write_log() {
    local MESSAGE="$1"
    local TYPE="${2:-INFO}"
    local TIMESTAMP
    TIMESTAMP=$(date "+%Y/%m/%d %H:%M:%S:%3N")
    echo "[$TIMESTAMP - ${0##*/} - $TYPE] $MESSAGE" | tee -a "$PATH_LOG"
}

# -----------------------------
# Start script
# -----------------------------
write_log "Script avviato." "INFO"
echo -e "\n\033[1;30m==============================\033[0m"
echo -e "\033[1;32m       SCRIPT AVVIATO         \033[0m"
echo -e "\033[1;30m==============================\033[0m\n"

# -----------------------------
# Controllo parametri - questo alla fine si puo togliere, o trasformare in un controllo esistenza del solo file flussi. Anche se se uso anagrafica di default, 
# sara meglio aggiungere in python un controllo per la sua esistenza
# -----------------------------
if [[ ! -f "$PATH_FILE_FLUSSI" ]]; then
    echo -e "\033[1;31mErrore: file flussi ($PATH_FILE_FLUSSI) mancante.\033[0m"
    write_log "File mancante: file flussi=$PATH_FILE_FLUSSI" "ERROR"
    exit 1
fi

# -----------------------------
# Controllo esistenza file CSV e JSON
# -----------------------------
while read -r line; do
    if [[ ! -f "${PATH_BASE}data/${IDPER}_${line}.csv" ]]; then
        echo -e "\033[1;31mFile mancante: ${PATH_BASE}data/${IDPER}_${line}.csv\033[0m"
        write_log "File mancante: ${PATH_BASE}data/${IDPER}_${line}.csv" "ERROR"
        exit 1
    fi
    write_log "File CSV (${PATH_BASE}data/${IDPER}_${line}.csv) trovato." "INFO"
done < $PATH_FILE_FLUSSI

for f in "$PATH_CONFIG_JSON"; do
    if [[ ! -f "$f" ]]; then
        echo -e "\033[1;31mFile mancante: $f\033[0m"
        write_log "File mancante: $f" "ERROR"
        exit 1
    fi
    write_log "File JSON ($PATH_CONFIG_JSON) trovato." "INFO"
done
echo -e "\033[1;34mFile CSV e JSON trovati correttamente.\033[0m"
# write_log "File CSV ($PATH_CSV) e JSON ($PATH_CONFIG_JSON) trovati." "INFO"

# -----------------------------
# Esecuzione script Python
# -----------------------------
PYTHON_EXE="/mnt/c/Users/botta/Desktop/Work/adv_env/bin/python"  # wsl path 
# "C:/Users/botta/Desktop/Work/adv_env_win/Scripts/python.exe" # windows path
PYTHON_SCRIPT="${PATH_BASE}src/processa_flusso.py"

CMD="$PYTHON_EXE $PYTHON_SCRIPT --user $O_USER --password $PASSWORD --dsn $DSN --idper $IDPER"

echo -e "\033[1;36mAvvio processo Python...\033[0m"
write_log "Avvio processo
Python con comando: $CMD" "INFO"

eval "$CMD"

# -----------------------------
# Fine esecuzione
# -----------------------------
echo -e "\033[1;30m==============================\033[0m"
echo -e "\033[1;32m       SCRIPT COMPLETATO      \033[0m"
echo -e "\033[1;30m==============================\033[0m"
write_log "Script completato con successo." "INFO"
