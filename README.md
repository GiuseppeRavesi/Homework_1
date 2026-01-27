# DSBD – Sistemi Distribuiti e Big Data  
## Homework #3 – Kubernetes, Kafka, Alerting, Observability e API Gateway

---

## Descrizione

Questa versione del progetto migra l’intero sistema sviluppato in Homework #1 e Homework #2 da **Docker Compose a Kubernetes**, introducendo un’architettura **cloud-native**, osservabile e resiliente.

Il sistema è ora distribuito su un **cluster Kubernetes (Kind)** e utilizza primitive Kubernetes standard (Deployment, StatefulSet, Service, ConfigMap, Secret, Ingress) per il deploy e la gestione dei microservizi.

---

## Cosa cambia rispetto a Homework #2

- **Migrazione completa a Kubernetes**
  - Tutti i servizi sono deployati tramite manifest YAML.
  - Database e Kafka gestiti tramite StatefulSet con volumi persistenti.
- **Ingress Controller (NGINX)**
  - Punto di ingresso unico al sistema.
  - Routing HTTP verso i microservizi applicativi.
- **Prometheus**
  - Raccolta centralizzata delle metriche.
  - Esposizione endpoint `/metrics` per ogni microservizio.
- **Configurazione centralizzata**
  - `ConfigMap` per configurazioni non sensibili.
  - `Secret` per credenziali (DB, SMTP, ecc.).
- **Persistenza dei dati**
  - Volumi Kubernetes per database e Kafka.
- **Osservabilità**
  - Metriche custom su Kafka, DB, HTTP, Alerting.

---

## Architettura ad Alto Livello

### Panoramica

```
Client
  ↓
[ Ingress NGINX ]
  ↓
  ├─→ User Manager (REST + gRPC)
  │
  └─→ Data Collector (REST + Scheduler)
         ↓
      Kafka
         ↓
      ├─→ Alert System
      │      ↓
      └─→ Alert Notifier (SMTP)
```


---

## Kafka

Kafka opera in **modalità KRaft (senza ZooKeeper)** ed è deployato come **StatefulSet**.

### Topic principali
- **`to-alert-system`**
  - Prodotto da Data Collector.
  - Contiene:
    `<email, airport, departures, arrivals, timestamp>`
- **`to-notifier`**
  - Prodotto da Alert System.
  - Contiene:
    `<email, airport, condition>`

---

## Alert System

- Consumer Kafka (`to-alert-system`).
- Recupera le soglie `(high_value, low_value)` dal **Data DB**.
- Valuta le condizioni:
  - superamento soglia alta
  - superamento soglia bassa
- Produce un evento su `to-notifier` quando una soglia è violata.
- Espone metriche Prometheus:
  - messaggi Kafka consumati
  - query DB
  - alert generati

---

## Alert Notifier

- Consumer Kafka (`to-notifier`).
- Invia notifiche email via **SMTP**.
- Configurazione SMTP fornita tramite `ConfigMap` + `Secret`.
- Gestisce errori di:
  - autenticazione SMTP
  - destinatario inesistente
- Espone metriche Prometheus:
  - email inviate
  - errori SMTP

---

## API Gateway – Ingress NGINX

Il sistema espone **un solo punto di ingresso HTTP** tramite Ingress:

| Path | Servizio |
|-----|---------|
| `/users/*` | User Manager |
| `/collector/*` | Data Collector |
| `/metrics` | Prometheus (UI) |

---

## API Principali

### User Manager
- `GET /users/health`
- `POST /users/register`
- `POST /users/login`

### Data Collector
- `GET /collector/health`
- `POST /collector/airports`
- `PUT /collector/airports/preferences`
- `POST /collector/collect`

Le API di HW1 e HW2 sono pienamente supportate.

---

## Osservabilità – Prometheus

Ogni microservizio espone `/metrics` in formato Prometheus.

Metriche incluse:
- HTTP requests
- Kafka produced/consumed messages
- DB queries
- Alert generati
- Email inviate
- Tempo di elaborazione

Prometheus è accessibile via Ingress:

```text
http://localhost/metrics
```

---

## Setup e Deploy con Kubernetes (Kind)

### Prerequisiti

- Docker
- Kind
- kubectl

### Avvio del Cluster

```bash
kind create cluster --name dsbd-cluster
```

Verifica:

```bash
kubectl get nodes
```

### Installazione Ingress Controller

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.9.6/deploy/static/provider/kind/deploy.yaml
```

Attendere che il controller sia Running.

### Deploy del Sistema

Applicare i manifest nell’ordine corretto:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/app-config.yaml
kubectl apply -f k8s/app-secrets.yaml

kubectl apply -f k8s/database.yaml
kubectl apply -f k8s/kafka.yaml

kubectl apply -f k8s/user-manager.yaml
kubectl apply -f k8s/data-collector.yaml
kubectl apply -f k8s/alert-system.yaml
kubectl apply -f k8s/alert-notifier.yaml

kubectl apply -f k8s/prometheus.yaml
kubectl apply -f k8s/ingress.yaml
```

Verifica:

```bash
kubectl get pods -n dsbd-ns
```

---

## Accesso dal Browser / Postman

Per esporre l'Ingress su macchina locale:

```bash
kubectl port-forward -n ingress-nginx deploy/ingress-nginx-controller 8080:80
```
```

Esempi:

```bash
curl http://localhost:8080/users/health
curl http://localhost:8080/collector/health
```

---

## Testing della Pipeline Kafka

1. Registrare un utente.
2. Aggiungere un aeroporto con soglie.
3. Avviare una raccolta manuale:

```bash
curl -X POST http://localhost:8080/collector/collect
```

Verificare:
- log di alert-system
- invio email da alert-notifier
- metriche Prometheus

---

## Persistenza dei Dati

Database e Kafka utilizzano `PersistentVolumeClaim`.

I dati persistono anche se:
- i pod vengono riavviati
- il cluster viene fermato e riavviato

---

## Come Fermare / Riavviare il Sistema

### Fermare il cluster (senza perdere dati)

```bash
docker stop $(kind get nodes --name dsbd-cluster)
```

### Riavviare il cluster

```bash
docker start $(kind get nodes --name dsbd-cluster)
```

### Eliminare completamente il cluster (perdita dati)

```bash
kind delete cluster --name dsbd-cluster
```

---

## Struttura del Repository (HW3)

```
Homework_1/
├── user-manager/
├── data-collector/
├── alert-system/
├── alert-notifier/
│
├── k8s/
│   ├── namespace.yaml
│   ├── app-config.yaml
│   ├── app-secrets.yaml
│   ├── database.yaml
│   ├── kafka.yaml
│   ├── user-manager.yaml
│   ├── data-collector.yaml
│   ├── alert-system.yaml
│   ├── alert-notifier.yaml
│   ├── prometheus.yaml
│   └── ingress.yaml
│
├── README.md
```

---

## Conclusione

Homework #3 completa l'evoluzione del sistema verso un'architettura scalabile, osservabile e production-ready, applicando principi di **cloud-native computing**, **event-driven architecture** e **monitoring avanzato** tramite **Kubernetes** e **Prometheus**.
├── README.md

Conclusione

Homework #3 completa l’evoluzione del sistema verso un’architettura scalabile, osservabile e production-ready, applicando principi di cloud-native computing, event-driven architecture e monitoring avanzato tramite Kubernetes e Prometheus.


---

Se vuoi, nel prossimo messaggio posso:
- **semplificarlo per l’orale** (versione “racconto al prof”)
- aggiungere una **checklist rapida da esame**
- oppure adattarlo esattamente allo **stile del tuo corso/prof**