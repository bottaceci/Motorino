# while [[ $# -gt 0 ]]; do
#     case $1 in
#         --idper) IDPER="$2"; shift 2 ;;
#         --file_flussi) FILE_FLUSSI="$2"; shift 2 ;;
#         *) echo "Parametro sconosciuto $1"; exit 1 ;;
#     esac
# done

# PATH_BASE=$"../"

# while read -r line; do
#     if [[ ! -f "${PATH_BASE}data/${IDPER}_${line}.csv" ]]; then
#         echo -e "\033[1;31mFile mancante: ${PATH_BASE}data/${IDPER}_${line}.csv\033[0m"
#         write_log "File mancante: ${PATH_BASE}data/${IDPER}_${line}.csv" "ERROR"
#         exit 1
#     else
#         echo "File CSV (${PATH_BASE}data/${IDPER}_${line}.csv) trovato."
#     fi 
# done < ${PATH_BASE}config/${FILE_FLUSSI}.txt

WIN_WSL_IP="$(
  powershell.exe -NoProfile -Command \
    "(Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias 'vEthernet (WSL (Hyper-V firewall))' -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty IPAddress)" \
  | tr -d '\r'
)"

if [ -z "$WIN_WSL_IP" ]; then
  WIN_WSL_IP="$(awk '/^nameserver[[:space:]]+/ {print $2; exit}' /etc/resolv.conf)"
fi

DSN="${DSN:-${WIN_WSL_IP:-172.23.64.1}:1521/orcl}"

echo $WIN_WSL_IP