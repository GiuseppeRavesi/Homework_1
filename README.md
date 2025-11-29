# DSBD – Distributed Systems and Big Data
## Microservizi per Gestione Utenti e Monitoraggio Traffico Aereo

---

## Descrizione del Progetto

Questo repository contiene l'implementazione di un sistema distribuito basato su microservizi containerizzati.
L'applicazione consente di:

- registrare e gestire utenti (User Manager)
- permettere agli utenti di selezionare aeroporti di interesse
- raccogliere periodicamente dati sui voli tramite OpenSky Network
- memorizzare e analizzare i voli in arrivo/partenza (Data Collector)

Il sistema espone API **REST** verso i client e utilizza **gRPC** per la comunicazione interna tra microservizi.
Tutto è orchestrato tramite **Docker Compose**.

## Architettura ad Alto Livello

L’applicazione è composta da due microservizi principali:

### **1. User Manager**
- Gestisce l’intero ciclo di vita degli utenti (registrazione, verifica, cancellazione)
- Implementa una politica **At-Most-Once** per prevenire registrazioni duplicate
- Esporta API **REST** verso il client
- Espone un servizio **gRPC** utilizzato internamente dal Data Collector per verificare l’esistenza degli utenti
- Utilizza un database dedicato, isolato in rete Docker privata

### **2. Data Collector**
- Gestisce la lista degli aeroporti di interesse associati a ciascun utente
- Raccoglie ciclicamente i dati delle API pubbliche di **OpenSky Network**
- Memorizza partenze e arrivi in un proprio database
- Fornisce endpoint **REST** per interrogare e analizzare i dati salvati
- Interagisce via **gRPC** con lo User Manager per validare gli utenti

### **Isolamento e Comunicazione tra Servizi**
- Ogni microservizio ha un **database isolato**, accessibile solo sulla rete Docker interna
- I database **non sono esposti verso l’esterno**
- La comunicazione diretta tra i due microservizi avviene esclusivamente tramite **gRPC**
- Tutti i diagrammi (architetturali e di interazione) sono disponibili nella **relazione tecnica** allegata

## API Principali

Di seguito una panoramica sintetica delle API esposte dai due microservizi.
I dettagli completi sono riportati nella relazione tecnica.

---

### User Manager (porta 5000)

- `POST /register`
  Registra un nuovo utente applicando la politica **At-Most-Once** (richiede un `request_id` univoco).

- `GET /exists/<email>`
  Verifica l'esistenza di un utente tramite query diretta al database.

- `DELETE /delete/<email>`
  Cancella un utente dal sistema.

- `GET /users`
  Restituisce la lista degli utenti registrati.

- `GET /health`
  Health check del servizio User Manager.

---

### Data Collector (porta 5001)

- `POST /airports`  
  Aggiunge un aeroporto di interesse per un utente esistente (validato via gRPC).

- `DELETE /airports`  
  Rimuove un interesse specifico usando query parameters: `email`, `airport_code`.

- `GET /airports/<email>`  
  Elenca tutti gli aeroporti di interesse associati a un utente.

- `POST /collect`  
  Avvia manualmente un ciclo di raccolta dati dai servizi OpenSky.

- `GET /flights/<airport>`  
  Restituisce i voli memorizzati per un dato aeroporto (filtrati per utente).

- `GET /flights/<airport>/latest`  
  Recupera l’ultimo volo salvato per un determinato aeroporto.

- `GET /flights/<airport>/average?days=N`  
  Calcola la media giornaliera dei voli negli ultimi `N` giorni.

- `GET /health`
  Health check del Data Collector.

## Setup e Deploy con Docker

I microservizi e i relativi database sono completamente containerizzati.
È sufficiente disporre di **Docker** e **Docker Compose** per eseguire l'intero sistema.

---

### Clonare il repository

```bash
git clone <URL_REPOSITORY>
cd <NOME_CARTELLA_PROGETTO>
```
---

### Configurare l'ambiente

Creare un file `.env` nella root del progetto. Al suo interno vanno specificate le seguenti variabili:

```bash
# Credenziali e configurazione database
DB_HOST=user_db
DB_PORT=5432
DB_NAME=users
DB_USER=postgres
DB_PASSWORD=postgres

# Host servizi interni
USER_MANAGER_HOST=user_manager
DATA_COLLECTOR_HOST=data_collector

# Credenziali OpenSky Network
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret

# Intervallo raccolta dati (ore)
COLLECTION_INTERVAL_HOURS=12
```

La relazione tecnica include una descrizione dettagliata di tutte le variabili supportate.

## 📂 Struttura del Repository

La seguente struttura riassume l’organizzazione dei file principali del progetto:

```
Homework_1/
├── user_manager/                    # Microservizio User Manager
│   ├── app.py                       # API REST + server gRPC
│   ├── grpc_definitions/            # File .proto e stub generati
│   │   ├── __init__.py
│   │   ├── user.proto
│   │   ├── user_pb2.py
│   │   └── user_pb2_grpc.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── data_collector/                  # Microservizio Data Collector
│   ├── app.py                       # Entry point e API REST
│   ├── opensky_client.py            # Integrazione con OpenSky Network
│   ├── grpc_client.py               # Client gRPC verso User Manager
│   ├── database.py                  # Accesso al database locale
│   ├── grpc_definitions/            # Stub gRPC generati
│   ├── Dockerfile
│   └── requirements.txt
│
├── database/                        # Script SQL inizializzazione DB
│   ├── user_db_init.sql
│   └── data_db_init.sql
│
├── docker-compose.yaml              # Orchestrazione dei microservizi
├── README.md                        # Questo file
└── .env.example                     # Esempio configurazione ambiente
```
## Testing

Il sistema può essere testato tramite Postman, cURL o qualsiasi client HTTP.

### User Manager (porta 5000)
- `POST /register` — registra un utente (politica At-Most-Once)
- `GET /exists/<email>` — verifica se l'utente esiste
- `DELETE /delete/<email>` — rimuove un utente
- `GET /users` — elenca gli utenti registrati

### Data Collector (porta 5001)
- `POST /airports` — aggiunge un aeroporto di interesse
- `DELETE /airports` — rimuove un interesse
- `GET /airports/<email>` — elenca gli aeroporti associati a un utente
- `POST /collect` — avvia la raccolta manuale dei voli
- `GET /flights/<airport>` — restituisce i voli registrati
- `GET /flights/<airport>/latest` — recupera l’ultimo volo
- `GET /flights/<airport>/average` — calcola la media giornaliera degli ultimi N giorni

Una descrizione più dettagliata delle sequenze di test è disponibile nella **relazione tecnica**.

## Licenza

Questo progetto è stato sviluppato come parte dell’insegnamento  
**Distributed Systems and Big Data (LM-32)**  
presso l’Università degli Studi di Catania.

Il codice è fornito esclusivamente per scopi accademici ed esercitativi.  
Non è destinato all’uso in produzione.
