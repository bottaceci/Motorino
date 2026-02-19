param(
    [string]$user = "VGLSA",
    [string]$password = "VGLSA",
    [string]$dsn = "localhost:1521/orcl",
    [string]$flusso,
    [string]$chiave_json,
    [string]$tabella
)

# --- Percorsi base ---
$pathBase = "C:..\"
$pathCsv = Join-Path $pathBase ("data\" + $flusso + ".csv")
$pathLog = Join-Path $pathBase "logs\script_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$pathConfigJson = Join-Path $pathBase "config\config_flussi.json"

# --- Funzione di log ---
function Write-Log {
    param(
        [string]$Message,
        [string]$Type = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy/MM/dd HH:mm:ss:fff"
    $scriptName = $MyInvocation.MyCommand.Name
    if (-not $scriptName) { $scriptName = "ConsoleHost" }
    $fullMessage = "[$timestamp - $scriptName - $Type] $Message"
    Add-Content -Path $pathLog -Value $fullMessage
}

# --- Start script ---
Write-Log "Script avviato." "INFO"
Write-Host ""
Write-Host "==============================" -ForegroundColor DarkGray
Write-Host "       SCRIPT AVVIATO         " -ForegroundColor Green
Write-Host "==============================" -ForegroundColor DarkGray
Write-Host ""

# --- Controllo parametri ---
if (-not $flusso -or -not $chiave_json) {
    Write-Host "Errore: parametri mancanti (flusso o chiave_json)." -ForegroundColor Red
    Write-Log "Parametri mancanti: flusso=$flusso, chiave_json=$chiave_json" "ERROR"
    exit 1
}

# --- Controllo esistenza file CSV e JSON ---
foreach ($f in @($pathCsv, $pathConfigJson)) {
    if (-not (Test-Path $f)) {
        Write-Host "File mancante: $f" -ForegroundColor Red
        Write-Log "File mancante: $f" "ERROR"
        exit 1
    }
}
Write-Host "File CSV e JSON trovati correttamente." -ForegroundColor Blue
Write-Log "File CSV ($pathCsv) e JSON ($pathConfigJson) trovati." "INFO"

# --- Controllo chiave JSON ---
try {
    $jsonContent = Get-Content $pathConfigJson -Raw | ConvertFrom-Json
    if (-not $jsonContent.PSObject.Properties.Name -contains $chiave_json) {
        Write-Host "Chiave JSON '$chiave_json' non trovata nel file di configurazione." -ForegroundColor Red
        Write-Log "Chiave JSON '$chiave_json' non trovata." "ERROR"
        exit 1
    }
    Write-Host "Chiave JSON '$chiave_json' valida." -ForegroundColor Blue
    Write-Log "Chiave JSON '$chiave_json' presente nel file di configurazione." "INFO"
} catch {
    Write-Host "Errore nel parsing del JSON." -ForegroundColor Red
    Write-Log "Errore nel parsing del JSON: $_" "ERROR"
    exit 1
}

# --- Esecuzione script Python ---
$pythonExe = "C:\Users\botta\Desktop\Work\adv_env_win\Scripts\python.exe"
$pythonScript = Join-Path $pathBase "scr\processa_flusso.py"

$cmd = "$pythonExe $pythonScript --user $user --password $password --dsn $dsn --flusso $flusso --chiave_json $chiave_json --tabella $tabella"

Write-Host "Avvio processo Python..." -ForegroundColor Cyan
Write-Log "Avvio processo Python con comando: $cmd" "INFO"

# Esegue lo script Python
Invoke-Expression $cmd

# --- Fine esecuzione ---
Write-Host "==============================" -ForegroundColor DarkGray
Write-Host "       SCRIPT COMPLETATO      " -ForegroundColor Green
Write-Host "==============================" -ForegroundColor DarkGray
Write-Log "Script completato con successo." "INFO"


# Per eseguirlo: .\nome_script.ps1 -user ... -password ... -dsn ... -flusso ... -chiave_json ...
