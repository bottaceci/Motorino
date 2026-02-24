#!/bin/bash

# -----------------------------
# Parametri da linea di comando
# -----------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --user) O_USER="$2"; shift 2 ;;
        --password) PASSWORD="$2"; shift 2 ;;
        --n_user) N_USER="$2"; shift 2 ;;
        --n_password) N_PASSWORD="$2"; shift 2 ;;
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
O_USER="${O_USER:-VGLSA_P}"
PASSWORD="${PASSWORD:-VGLSA_P}"
N_USER="${N_USER:-VGLDM_P}"
N_PASSWORD="${N_PASSWORD:-VGLDM_P}"
DSN="${DSN:-172.23.64.1:1521/orcl}"  # switched 'localhost' with '172.23.64.1', the Windows host/gateway IP for wsl - use localhost if on linux

# -----------------------------
# Percorsi
# -----------------------------
PATH_BASE="../"
PATH_LOG="${PATH_BASE}logs/script_log_$(date +%Y%m%d_%H%M%S).log"
PATH_CONFIG_JSON="${PATH_BASE}config/config_flussi_dm.json"

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
# Controllo esistenza file JSON
# -----------------------------
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
PYTHON_SCRIPT="${PATH_BASE}src/crea_datamart.py"

CMD="$PYTHON_EXE $PYTHON_SCRIPT --user $O_USER --password $PASSWORD --n_user $N_USER --n_password $N_PASSWORD --dsn $DSN --idper $IDPER"

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
