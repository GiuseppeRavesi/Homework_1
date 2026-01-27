# DSBD – Sistemi Distribuiti e Big Data
## Homework #2 – Estensioni: Circuit Breaker, Kafka, Alert System, API Gateway

### Descrizione
Questa versione del progetto estende il sistema di Homework #1 introducendo nuove funzionalità per migliorare la robustezza e l'efficienza del sistema distribuito.

#### Cosa cambia rispetto a Homework #1
- **Circuit Breaker**: Protegge tutte le chiamate verso OpenSky Network.
- **Message Broker Kafka**: Abilita comunicazione asincrona tra microservizi.
- **Alert System**: Consumer Kafka che valuta le soglie (high/low) per utente/aeroporto.
- **Alert Notifier**: Consumer Kafka che invia notifiche email via SMTP quando scatta un alert.
- **API Gateway NGINX**: Punto di ingresso unico verso il sistema.

Il sistema mantiene:
- Database separati (Database-per-Service).
- Isolamento tramite reti Docker dedicate.
- Comunicazione interna via gRPC tra Data Collector e User Manager.

### Architettura ad Alto Livello
#### Kafka
Kafka introduce 2 topic principali:
- `to-alert-system`: Pubblicato dal Data Collector al termine di una raccolta (scheduler/manuale), contiene i dati aggiornati `<email, airport, departures, arrivals, timestamp>`.
- `to-notifier`: Pubblicato da Alert System quando rileva una soglia superata, contiene `<email, airport, condition>`.

#### Alert System
- Consumer di `to-alert-system`.
- Recupera da Data DB le soglie `high_value` / `low_value` associate a `(email, airport)`.
- Se una soglia è violata, produce un messaggio su `to-notifier`.

#### Alert Notifier
- Consumer di `to-notifier`.
- Invia notifica via SMTP (email) con:
  - To: `email`
  - Subject: `airport`
  - Body: `condition`

#### API Gateway (NGINX)
- Espone un singolo punto di ingresso (porta 8080).
- Esegue reverse proxy verso:
  - `/users/*` → User Manager (porta 5000)
  - `/collector/*` → Data Collector (porta 5001)

### API Principali
#### Data Collector (porta 5001)
Estende la gestione degli interessi aeroportuali introducendo i parametri:
- `high_value` (soglia superiore)
- `low_value` (soglia inferiore)

Vincoli:
- Entrambi opzionali.
- Se presenti entrambi: `high_value > low_value`.

Nuove/aggiornate API:
- `POST /airports` (ora accetta anche `high_value`, `low_value`)
- `PUT /airports/preferences` (aggiorna soglie per un interesse esistente)

Le altre API di HW1 rimangono disponibili.

### Setup e Deploy con Docker
#### Prerequisiti
- Docker
- Docker Compose

**Nota**: Kafka usa una rete Docker esterna (`kafka_network`). Se non esiste, va creata manualmente.

#### Passi per l'Installazione
1. Creare la rete Kafka (solo la prima volta):
   ```bash
   docker network create kafka_network
   ```

2. Configurare l'ambiente:
   Crea un file `.env` nella root (o aggiorna il tuo) con le variabili di HW1 + le nuove per HW2.

   Esempio minimo (HW2):
   ```env
   # --- DB Data Collector ---
   DATA_DB_HOST=data_db
   DATA_DB_PORT=5432
   DATA_DB_NAME=flights
   DATA_DB_USER=postgres
   DATA_DB_PASSWORD=postgres

   # --- User Manager (gRPC) ---
   USER_MANAGER_HOST=user_manager
   USER_MANAGER_GRPC_PORT=50051

   # --- OpenSky ---
   CLIENT_ID=your_client_id
   CLIENT_SECRET=your_client_secret

   # --- Scheduler ---
   COLLECTION_INTERVAL_HOURS=12

   # --- Kafka ---
   KAFKA_BOOTSTRAP_SERVERS=kafka:9092
   KAFKA_TOPIC_TO_ALERT=to-alert-system
   KAFKA_TOPIC_TO_NOTIFIER=to-notifier

   # --- SMTP (Alert Notifier) ---
   SMTP_HOST=smtp.example.com
   SMTP_PORT=587
   SMTP_USER=your_email@example.com
   SMTP_PASSWORD=your_password_or_app_password
   SMTP_FROM=your_email@example.com
   SMTP_USE_TLS=true
   ```

   *(Le variabili effettive usate dai container dipendono dal docker-compose: assicurati che i nomi ENV dei servizi coincidano con quelli letti dal codice.)*

3. Avvio del sistema:
   ```bash
   docker compose up --build
   ```

#### Accesso tramite API Gateway (NGINX)
Una volta attivo il gateway sulla porta 8080, puoi chiamare le API senza conoscere le porte interne:

- **User Manager**: `http://localhost:8080/users/...`
- **Data Collector**: `http://localhost:8080/collector/...`

Esempi:
```bash
# Health User Manager
curl http://localhost:8080/users/health

# Health Data Collector
curl http://localhost:8080/collector/health
```

### Testing Rapido della Pipeline Kafka (senza email)
1. Registra utente (via gateway o porta diretta).
2. Aggiungi aeroporto con soglie.
3. Avvia raccolta manuale:
   ```bash
   curl -X POST http://localhost:8080/collector/collect
   ```
4. Verifica che Alert System produca alert su `to-notifier` (guardando i log di `alert_system`).
5. (Opzionale) Verifica topic con console consumer dentro container Kafka:
   ```bash
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server kafka:9092 \
     --topic to-notifier \
     --from-beginning
   ```

### Struttura del Repository (HW2)
Oltre alla struttura HW1, sono stati aggiunti:
```
Homework_1/
├── user_manager/                  # HW1
├── data_collector/                # HW1 + HW2 (CB + producer Kafka)
├── database/                      # init SQL HW1/HW2
│   ├── user_db_init.sql
│   └── data_db_init.sql
│
├── alert_system/                  # HW2 (consumer to-alert-system + producer to-notifier)
│   ├── app.py
│   ├── kafka_consumer.py
│   ├── kafka_producer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── alert_notifier/                # HW2 (consumer to-notifier + invio SMTP)
│   ├── app.py            # oppure main.py (entrypoint)
│   ├── smtp_notifier.py   # logica SMTP
│   ├── kafka_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── infra/
│   ├── nginx/                     # HW2 API Gateway
│   │   ├── Dockerfile
│   │   └── nginx.conf
│   │
│   └── kafka/                     # HW2 Kafka esterno
│       └── docker-compose.yaml    # compose standalone del broker
│
├── docker-compose.yaml            # compose principale (microservizi + db + gateway)
├── .env
└── README.md
```

### Come Fermare il Sistema
Per fermare e rimuovere i container:
```bash
docker compose down
```

Per fermare e rimuovere anche i volumi (perdita dati):
```bash
docker compose down -v
```
