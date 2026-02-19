# PySpark — Microprogetto di pulizia e validazione dati dipendenti

Questo repository contiene un **microprogetto in PySpark** per la lettura, la pulizia e la validazione di dataset relativi ai dipendenti.  
L’obiettivo è mostrare come organizzare una pipeline dinamica di data processing con Spark, mantenendo log delle operazioni e distinguendo i dati validi da quelli errati.
Si è implementato poi, tramite **OracleDB**, un metodo per inserire la tabelle filtrate nelle tabelle 02 del motorino.
L’intero processo è automatizzato tramite uno **script Bash** (presente anche la versione per Windows scritta con **PowerShell**), che gestisce:
- il controllo dei parametri e dei file CSV/JSON,
- la verifica della chiave di configurazione JSON,
- l’esecuzione del processo Python con i parametri corretti,
- il logging di tutte le operazioni, con messaggi colorati per evidenziare lo stato delle operazioni.

Consiglio di usare lo Script.sh

---

## Ambiente di lavoro

Questo progetto è stato sviluppato principalmente in **PyCharm**, utilizzando **Git Bash** per l’esecuzione degli script di automazione.  

**Vantaggi principali:**
- **PyCharm**: gestione avanzata dei progetti Python, completamento automatico del codice, debugging integrato e supporto per virtual environment.  
- **Git Bash**: esecuzione di script Bash in ambiente Windows con comportamento coerente rispetto a Linux/Unix.  
- Pipeline automatizzate testabili direttamente dall’IDE senza dover cambiare terminale o ambiente.


## Funzionalità principali
- **Lettura di file CSV di input**: (es. `flusso.csv`, `unitacr.csv`)
- **Validazione dei campi principali**: Codice Fiscale, Data di Nascita, Nome, Salario, e altre colonne configurabili
- **Configurazione dinamica tramite file JSON**: (`config_flussi.json`) per gestire più flussi con regole diverse
- **Separazione automatica dei record**: Buoni / Scarti
- **Due livelli dedicati per i file di Log**: `tlog.log` per i dettagli delle singole operazioni, `tlog_gestore.log` per gestire diversi flussi
- **Possibilità di aggiungere nuovi flussi** senza modificare il codice
- **Unione e gestione di flussi multipli**

---

## Configurazione dinamica
Tutti i flussi e le regole di validazione sono definiti in `config/config_flussi.json`. Il dizionario di ogni flusso contiene anche il nome della tabella sul database dove verranno caricati i dati.
Esempio di struttura:
``` 
{
  "flusso": {
    "columns": {
      "SALARIO": {"type": "positive_number"},
      "DN": {"type": "date", "format": "yyyy-MM-dd"},
      "NOME": {"type": "string_no_numbers"},
      "CF": {"type": "string_length", "length": 16}
    },
    "table": "GRP02_FLS"
  },
    "unitacr": {
    "columns": {
      "COD_UNT": {"type": "positive_number"},
      "COD_ACR": {"type": "string_length", "length": 3}
    },
    "table": "COGE02_UNTACR"
  }
 }
``` 

## Struttura del progetto
``` 
PySpark/
├─ src/ # codice sorgente (es. tabella_dipendenti.py, gestore_flussi.py)
├─ data/ # CSV di esempio (Flusso.csv, Flusso2.csv)
├─ config/ # configurazione json e anagrafica flussi
├─ logs/ # log di esempio (non committare log runtime)
├─ notebooks/ # eventuali Jupyter notebook
├─ tests/ # test (se aggiunti)
├─ docs/ # documentazione aggiuntiva
├─ .gitignore
├─ README.md
└─ requirements.txt
```

---

## Prerequisiti
- **Python 3.8+**
- [Apache Spark](https://spark.apache.org/) con PySpark installato 
- OracleDB
- JDK (Java Developer Kit) 17 (check current version using `java --version` in the shell) 

Installazione rapida di PySpark:
```bash```
pip install pyspark

## Istruzioni per l'esecuzione

1. **Preparazione dei dati**  
   - Inserire i file CSV di input nella cartella `data/`.  
   - Il file deve rispettare le seguenti regole:  
     - Separatore dei campi coerente (es. `,`).  
     - La prima riga deve contenere i nomi delle colonne (header).  
     - Il nome del file deve coincidere con l’ID del periodo (`ID_PER`), ad esempio:  
       ```
       202501_presenze.csv
       ```

2. **Configurazione dei flussi (JSON) e Anagrafica**  
   - Modificare/aggiungere i flussi in `config/config_flussi.json`.  
   - Per ogni colonna si specificano le regole di validazione, richiamando i metodi disponibili nella classe `Operazioni`  
     (es. `positive_number`, `string_no_numbers`, `date`, `email`, `string_length`).  
   - Più regole possono essere applicate sequenzialmente sulla stessa colonna.
   - inserire nel dizionario di ogni flusso il nome della corrispondente tabella sul database sotto `table`
   - inserire nel file `config/anagrafica_flussi.txt` il nome dei flussi che si vuole caricare, uno per riga, senza `idper` o `.csv`, ad esempio
      ```
      presenze
      unitacr
      ```

3. **Installazione dipendenze**  
   Da eseguire una sola volta:  
   ```bash
   pip install -r requirements.txt
   ```

4. **Esecuzione tramite script automatizzato**  
   Dalla cartella `scripts/`, avviare lo script passando i parametri richiesti:  

   - **Esempio (Git Bash / Linux):**
     ```bash
     ./Script.sh --idper 202501
     ```

   - **Esempio (PowerShell):**
     ```powershell
     .\Script.ps1 -flusso 202501_presenze -chiave_json presenze -tabella GRP02_PRS
     ```
     ⚠️ **Nota:** Lo script PowerShell non è stato ancora adattato per usare il file anagrafica_flussi.txt, quindi al momento non funziona.

   **Parametri disponibili:**  
   - `--idper`: ID periodo dei flussi che si vuole caricare
   - `--flusso` / `-flusso`: nome del file CSV (senza estensione) da elaborare.  
   - `--chiave_json` / `-chiave_json`: chiave corrispondente nel file `config_flussi.json`.  
   - `--tabella` / `-tabella`: tabella Oracle di destinazione (es. `GRP02_PRS`, `GRP02_ANA`).  
   - `--user` / `-user`: username per connessione al database Oracle.  
   - `--password` / `-password`: password per connessione al database Oracle.  
   - `--dsn` / `-dsn`: Data Source Name per la connessione (es. `localhost:1521/orcl`).  

   ⚠️ **Nota:** se non vengono specificati, i parametri `user`, `password` e `dsn` assumono i valori di default utilizzati dal sistema *motorino*:  
   - `user = VGLSA`  
   - `password = VGLSA`  
   - `dsn = localhost:1521/orcl`  

5. **Log e monitoraggio**  
   - I log di esecuzione vengono salvati automaticamente in `logs/`.  
   - Sono previsti due livelli di log:  
     - **tlog_gestore.log** → log generale dei flussi. Implementati dalle classi.  
     - **script_log_*.log** → log specifico di ogni esecuzione. Creato dinamicamente in base alla data.

6. **Identificazione dei flussi**  
   Ogni elaborazione aggiunge automaticamente un campo `IDRUN` incrementale per tracciare i singoli processi eseguiti.

## Note

- Tutti i CSV devono essere disponibili nella cartella `data/` prima dell’esecuzione.  
- Il codice è modulare: è possibile aggiungere nuovi flussi o regole di validazione modificando i file in `src/`.  
- La separazione dei record validi e non validi avviene automaticamente in base alle regole definite.  
- Attualmente non è prevista storicizzazione dei dati: la colonna `IDRUN` serve solo per distinguere i flussi durante la singola esecuzione.
- Provato solo con tabella unità, ma è un pò diverso dalla logica del motorino. Non ho ID_PER e la struttura del flusso è leggermente diversa. 

## Possibili Miglioramenti

- Estendere le regole di validazione e migliorare i messaggi di log in caso di errore di configurazione JSON.  
- Valutare se utilizzare un singolo file JSON con tutte le configurazioni o un file dedicato per ciascun flusso.  
- Ottimizzare le funzioni nella classe `Operazioni`, valutando se suddividere alcune validazioni in più metodi indipendenti (es. controllo numerico e controllo di positività separati).
- Aggiungere un constraint al file .json. Ad esempio {"type": "foreign_key"} dove mi assicuro **integrità referenziale**. 