# DSBD – Sistemi Distribuiti e Big Data
## Homework #3 – Orchestrazione Kubernetes: Deployment, ConfigMap, Secret & Ingress

### Descrizione Progetto
Sistema di monitoraggio e notifica di traffico aereo distribuito e containerizzato. Il progetto evolve da Docker Compose (HW2) a **orchestrazione Kubernetes** (HW3), introducendo gestione centralizzata della configurazione, secret management sicuro, e routing avanzato tramite Ingress controller.

#### Evoluzione tra le versioni
- **Homework #1**: Microservizi base con Docker, database separati, comunicazione gRPC
- **Homework #2**: Circuit Breaker, Message Broker Kafka, Alert System, API Gateway NGINX, comunicazione asincrona
- **Homework #3**: **Orchestrazione Kubernetes**, ConfigMap/Secret management, Ingress routing, scalabilità orizzontale, health checks evoluti

#### Componenti Principali (HW3)
- **Kubernetes Deployment**: Ogni microservizio gestito come Deployment con auto-healing
- **ConfigMap**: Centralizza la configurazione dell'ambiente (`app-config.yaml`)
- **Secret**: Gestione sicura di credenziali sensibili (`app-secrets.yaml`)
- **Ingress**: Routing HTTP/S con hostname-based routing
- **Prometheus**: Monitoraggio e metriche di sistema
- **Kafka**: Message broker per comunicazione asincrona tra servizi
- **PostgreSQL**: Database per persistenza dati

Il sistema mantiene dalla versione precedente:
- **Circuit Breaker**: Protezione su OpenSky Network
- **Kafka Topics**: Comunicazione event-driven (`to-alert-system`, `to-notifier`)
- **Alert System**: Valutazione soglie e produzione alert
- **Alert Notifier**: Notifiche email via SMTP
- **Database-per-Service**: Isolamento dati tra servizi
- **gRPC**: Comunicazione sincrona Data Collector ↔ User Manager

### Architettura ad Alto Livello

#### Topology Kubernetes
```
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster (HW3)                       │
├─────────────────────────────────────────────────────────────────┤
│  Ingress (NGINX Controller)                                       │
│  ├── dsbd.local/users/*        → User Manager Service             │
│  ├── dsbd.local/collector/*    → Data Collector Service           │
│  └── dsbd.local/              → Health/Info Endpoints             │
├─────────────────────────────────────────────────────────────────┤
│  Microservices (Deployments):                                     │
│  ├── User Manager (gRPC Server, REST API)                        │
│  ├── Data Collector (gRPC Client, REST API, Kafka Producer)     │
│  ├── Alert System (Kafka Consumer/Producer)                      │
│  ├── Alert Notifier (Kafka Consumer, SMTP Client)                │
│  └── API Gateway (opzionale, deprecato in HW3)                  │
├─────────────────────────────────────────────────────────────────┤
│  Data Layer:                                                      │
│  ├── User DB (PostgreSQL)                                         │
│  ├── Data DB (PostgreSQL)                                         │
│  └── Kafka Cluster (Zookeeper + Broker)                          │
├─────────────────────────────────────────────────────────────────┤
│  Configuration:                                                   │
│  ├── ConfigMap (app-config.yaml) - Variabili non-sensibili      │
│  ├── Secret (app-secrets.yaml)   - Credenziali DB, SMTP, etc   │
│  └── Namespace (dsbd)            - Isolamento risorse            │
├─────────────────────────────────────────────────────────────────┤
│  Monitoring:                                                      │
│  └── Prometheus + ServiceMonitor (metriche applicative)          │
└─────────────────────────────────────────────────────────────────┘
```

#### Event Flow
1. **Data Collection**: Data Collector raccoglie dati OpenSky ogni N ore
2. **Event Publishing**: Pubblica su topic Kafka `to-alert-system`
3. **Alert Processing**: Alert System consuma, valuta soglie, pubblica su `to-notifier`
4. **Notification**: Alert Notifier consuma e invia email via SMTP
5. **User Queries**: Utenti accedono via Ingress → Services → Microservizi

#### Kafka Topics
- **`to-alert-system`**: Dati raccolti `{email, airport, departures, arrivals, timestamp}`
- **`to-notifier`**: Alert elaborati `{email, airport, condition}`

#### Service Communication
- **Sincrona**: User Manager (gRPC port 50051) ← Data Collector
- **Asincrona**: Kafka event streaming tra Alert System e Alert Notifier
- **HTTP REST**: Accesso utenti tramite Ingress

### API Principali
#### Data Collector (Service: 5001)
Gestisce interessi aeroportuali con soglie di allerta:
- **POST** `/airports` - Registra nuovo interesse con soglie (opzionali)
- **PUT** `/airports/preferences` - Aggiorna soglie per interesse esistente
- **GET** `/airports` - Lista aeroporti dell'utente
- **DELETE** `/airports/{airport}` - Rimuove interesse
- **POST** `/collect` - Trigger raccolta manuale
- **GET** `/health` - Health check

**Parametri soglie**:
- `high_value` (opzionale): Soglia massima partenze/arrivi
- `low_value` (opzionale): Soglia minima partenze/arrivi
- Vincolo: Se entrambi presenti, `high_value > low_value`

#### User Manager (Service: 5000)
Gestisce profili utenti:
- **POST** `/users` - Crea nuovo utente
- **GET** `/users/{email}` - Recupera profilo utente
- **PUT** `/users/{email}` - Aggiorna profilo
- **DELETE** `/users/{email}` - Elimina utente
- **GET** `/health` - Health check

#### Alert System (Internal)
- Consumer Kafka `to-alert-system`
- Valuta soglie rispetto dati raccolti
- Producer Kafka `to-notifier`

#### Alert Notifier (Internal)
- Consumer Kafka `to-notifier`
- Invia email SMTP con dettagli alert

### Setup e Deploy con Kubernetes (HW3)

#### Prerequisiti
- **Kubernetes Cluster** (v1.24+): minikube, Docker Desktop, o cloud provider (EKS, GKE, AKS)
- **kubectl**: CLI per gestione Kubernetes
- **Helm** (opzionale): Per deployments avanzati
- **Docker Images**: Prebuilt per ogni microservizio

#### Passi per l'Installazione

##### 1. Preparazione del Cluster
```bash
# Assicurati che il cluster sia attivo
kubectl cluster-info

# Crea il namespace dedicato
kubectl apply -f infra/k8s/namespace.yaml

# Verifica namespace
kubectl get namespaces
```

##### 2. Configurazione Variabili di Ambiente
Crea il ConfigMap e Secret:

```bash
# ConfigMap con variabili non-sensibili
kubectl apply -f infra/k8s/app-config.yaml -n dsbd

# Secret con credenziali (IMPORTANTE: Mai committare secrets in versione unencrypted!)
# Opzione A: Applicare il file preesistente (se disponibile e sicuro)
# kubectl apply -f infra/k8s/app-secrets.yaml -n dsbd

# Opzione B: Creare Secret da variabili ambiente
kubectl create secret generic app-secrets \
  --from-literal=data-db-password=postgres \
  --from-literal=smtp-host=smtp.example.com \
  --from-literal=smtp-port=587 \
  --from-literal=smtp-user=your_email@example.com \
  --from-literal=smtp-password=your_password \
  --from-literal=opensky-client-id=your_id \
  --from-literal=opensky-client-secret=your_secret \
  -n dsbd
```

##### 3. Deploy Database Layer
```bash
# Deploy PostgreSQL per User Manager
kubectl apply -f infra/k8s/database.yaml -n dsbd

# Deploy Kafka e Zookeeper
kubectl apply -f infra/k8s/kafka.yaml -n dsbd

# Verifica che i pod siano ready
kubectl get pods -n dsbd -w
```

##### 4. Deploy Microservizi
```bash
# Deploy User Manager
kubectl apply -f infra/k8s/user-manager.yaml -n dsbd

# Deploy Data Collector
kubectl apply -f infra/k8s/data-collector.yaml -n dsbd

# Deploy Alert System
kubectl apply -f infra/k8s/alert-system.yaml -n dsbd

# Deploy Alert Notifier
kubectl apply -f infra/k8s/alert-notifier.yaml -n dsbd

# Verifica stato Deployments
kubectl get deployments -n dsbd
kubectl get pods -n dsbd
```

##### 5. Configurazione Ingress
```bash
# Assicurati che Ingress Controller sia installato
# Per minikube:
minikube addons enable ingress

# Deploy Ingress
kubectl apply -f infra/k8s/ingress.yaml -n dsbd

# Verifica Ingress
kubectl get ingress -n dsbd
kubectl describe ingress dsbd-ingress -n dsbd
```

##### 6. (Opzionale) Deploy Prometheus
```bash
# Installa Prometheus per monitoraggio
kubectl apply -f infra/k8s/prometheus.yaml -n dsbd
```

#### Accesso alle Applicazioni

##### Via Ingress (Consigliato)
Aggiungi alla tua `hosts` file (Windows: `C:\Windows\System32\drivers\etc\hosts`):
```
127.0.0.1 dsbd.local
```

Poi accedi da browser o curl:
```bash
# User Manager
curl http://dsbd.local/users/health

# Data Collector
curl http://dsbd.local/collector/health
```

##### Port-Forward (Alternativa locale)
```bash
# User Manager
kubectl port-forward svc/user-manager 5000:5000 -n dsbd

# Data Collector
kubectl port-forward svc/data-collector 5001:5001 -n dsbd

# Kafka
kubectl port-forward svc/kafka 9092:9092 -n dsbd

# Prometheus (se installato)
kubectl port-forward svc/prometheus 9090:9090 -n dsbd
```

#### File Configurazione (infra/k8s/)
| File | Descrizione |
|------|-------------|
| `namespace.yaml` | Namespace DSBD per isolamento risorse |
| `app-config.yaml` | ConfigMap con variabili non-sensibili (KAFKA_BOOTSTRAP_SERVERS, DB_HOST, ecc) |
| `app-secrets.yaml` | Secret per credenziali database, SMTP, OpenSky (⚠️ NON committare in versione unencrypted) |
| `database.yaml` | StatefulSet PostgreSQL per persistenza dati |
| `kafka.yaml` | StatefulSet Kafka + Zookeeper per message broker |
| `user-manager.yaml` | Deployment User Manager (gRPC server + REST API) |
| `data-collector.yaml` | Deployment Data Collector (Kafka producer, gRPC client) |
| `alert-system.yaml` | Deployment Alert System (Kafka consumer/producer) |
| `alert-notifier.yaml` | Deployment Alert Notifier (Kafka consumer, SMTP client) |
| `ingress.yaml` | Ingress controller per routing HTTP hostname-based |
| `prometheus.yaml` | Prometheus + ServiceMonitor per metriche |

### Testing e Validazione

#### Verificare Stato Cluster
```bash
# Tutti i pod in namespace dsbd
kubectl get pods -n dsbd

# Dettagli di uno specifico deployment
kubectl describe deployment user-manager -n dsbd

# Log di un pod
kubectl logs deployment/data-collector -n dsbd -f

# Accesso interattivo a un pod
kubectl exec -it <pod-name> -n dsbd -- /bin/bash
```

#### Test Pipeline Kafka
```bash
# 1. Creare un utente
curl -X POST http://dsbd.local/users \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "name": "Test User"}'

# 2. Aggiungere aeroporto con soglie
curl -X POST http://dsbd.local/collector/airports \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "airport": "MXP",
    "high_value": 50,
    "low_value": 10
  }'

# 3. Trigger raccolta manuale
curl -X POST http://dsbd.local/collector/collect

# 4. Verificare messaggi Kafka
kubectl exec -it <kafka-pod> -n dsbd -- \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic to-notifier \
  --from-beginning
```

#### Monitoring con Prometheus
Se Prometheus è deployato:
```bash
# Accedi a http://localhost:9090 (dopo port-forward)
kubectl port-forward svc/prometheus 9090:9090 -n dsbd
```

Metriche disponibili (se esposta dai servizi):
- `microservice_requests_total`
- `microservice_request_duration_seconds`
- `kafka_consumer_lag`

#### Troubleshooting
| Problema | Soluzione |
|----------|-----------|
| Pod in `CrashLoopBackOff` | Verifica log: `kubectl logs <pod> -n dsbd` |
| Ingress non raggiungibile | Verifica Ingress Controller: `kubectl get ingress -n dsbd` |
| Connessione Kafka fallita | Verifica servizio Kafka: `kubectl get svc kafka -n dsbd` |
| Secret non trovato | Ricrea Secret: `kubectl create secret ...` |
| Database non pronto | Attendi StatefulSet PostgreSQL: `kubectl get statefulset -n dsbd` |

### Struttura Repository (HW3)
```
Homework_1/
├── user_manager/                     # Microservizio User Manager
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── data_collector/                   # Microservizio Data Collector
│   ├── app.py
│   ├── circuit_breaker.py
│   ├── database.py
│   ├── grpc_client.py
│   ├── kafka_producer.py
│   ├── opensky_client.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── grpc_definitions/
│       ├── user_pb2.py
│       ├── user_pb2_grpc.py
│       └── user.proto
│
├── alert_system/                     # Microservizio Alert System
│   ├── app.py
│   ├── kafka_consumer.py
│   ├── kafka_producer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── alert_notifier/                   # Microservizio Alert Notifier
│   ├── app.py
│   ├── kafka_consumer.py
│   ├── smtp_notifier.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── database/                         # Script inizializzazione DB
│   ├── user_db_init.sql
│   └── data_db_init.sql
│
├── infra/
│   ├── k8s/                         # Kubernetes manifests (HW3)
│   │   ├── namespace.yaml
│   │   ├── app-config.yaml
│   │   ├── app-secrets.yaml        # ⚠️ SENSIBILE - usare Secret management
│   │   ├── database.yaml
│   │   ├── kafka.yaml
│   │   ├── user-manager.yaml
│   │   ├── data-collector.yaml
│   │   ├── alert-system.yaml
│   │   ├── alert-notifier.yaml
│   │   ├── ingress.yaml
│   │   └── prometheus.yaml
│   │
│   ├── nginx/                       # API Gateway NGINX (legacy HW2)
│   │   ├── Dockerfile
│   │   └── nginx.conf
│   │
│   └── kafka/                       # Standalone Kafka compose
│       └── docker-compose.yaml
│
├── docker-compose.yaml              # Compose HW2 (legacy, per testing locale)
├── README.md                        # Questo file
└── .gitignore                       # Con esclusione secret files
```

### Migration da Docker Compose (HW2) a Kubernetes (HW3)

#### Differenze Principali
| Aspetto | Docker Compose (HW2) | Kubernetes (HW3) |
|---------|---------------------|-----------------|
| Orchestrazione | docker-compose | kubectl + Deployments |
| Configurazione | .env file | ConfigMap |
| Segreti | .env (insicuro) | Secret (managed) |
| Routing | NGINX container | Ingress controller |
| Scalabilità | Manuale (docker-compose scale) | Automatica (HPA) |
| Health Check | healthcheck: | livenessProbe + readinessProbe |
| Persistenza | Docker volumes | PersistentVolume + PVC |
| Multi-host | Compose non lo supporta | Cluster distribuito |

#### Comandi Equivalenti
```bash
# HW2: Avviare il sistema
docker compose up --build

# HW3: Equivalente
kubectl apply -f infra/k8s/*.yaml -n dsbd

---

# HW2: Fermare
docker compose down

# HW3: Equivalente
kubectl delete namespace dsbd

---

# HW2: Consultare log
docker logs <container>

# HW3: Equivalente
kubectl logs deployment/<service> -n dsbd

---

# HW2: Entrare in container
docker exec -it <container> /bin/bash

# HW3: Equivalente
kubectl exec -it deployment/<service> -n dsbd -- /bin/bash
```

### Gestione Secret Sicura

⚠️ **ATTENZIONE**: Non committare `app-secrets.yaml` in versione plain text!

#### Opzioni Consigliate
1. **Sealed Secrets** (SealedSecrets):
   ```bash
   kubeseal -f app-secrets.yaml -w app-secrets-sealed.yaml
   ```

2. **External Secrets Operator**:
   - Integrazione con AWS Secrets Manager, Azure KeyVault, HashiCorp Vault

3. **Helm + templates**:
   - Template `app-secrets.yaml` con placeholder
   - Valorizzare in fase di deploy da variabili environment

4. **SOPS (Secrets Operations)**:
   - Encrypting/decrypting YAML files

### Comandi Comuni Kubernetes (kubectl)

```bash
# Namespace
kubectl get namespaces
kubectl create namespace dsbd
kubectl delete namespace dsbd

# Pod & Deployments
kubectl get pods -n dsbd
kubectl get deployments -n dsbd
kubectl describe pod <pod-name> -n dsbd
kubectl logs deployment/<deployment-name> -n dsbd
kubectl exec -it <pod-name> -n dsbd -- /bin/bash

# Services & Ingress
kubectl get services -n dsbd
kubectl get ingress -n dsbd

# ConfigMap & Secret
kubectl get configmap -n dsbd
kubectl get secret -n dsbd
kubectl describe configmap app-config -n dsbd

# Delete/Cleanup
kubectl delete deployment <name> -n dsbd
kubectl delete all --all -n dsbd
```

---

## Riferimenti e Documentazione

### Documentazione Ufficiale
- [Kubernetes Official Documentation](https://kubernetes.io/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [gRPC Documentation](https://grpc.io/docs/)

### Risorse Utili
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
- [Kafka Topic Design Best Practices](https://kafka.apache.org/documentation/#design)
- [Container Security Guide](https://kubernetes.io/docs/concepts/security/)

### Best Practices Implementate
✅ **Database-per-Service**: Ogni microservizio ha database isolato (user_manager ↔ user_db, data_collector ↔ data_db)

✅ **Circuit Breaker**: Protezione contro cascading failures verso OpenSky Network

✅ **Event Sourcing**: Kafka topics traccia tutte le transazioni (to-alert-system, to-notifier)

✅ **Health Checks**: Deployments Kubernetes con livenessProbe + readinessProbe

✅ **Configuration Management**: Separazione tra ConfigMap (non-sensitive) e Secret (credenziali)

✅ **Namespace Isolation**: Tutte le risorse nel namespace `dsbd`

✅ **Resource Limits**: Requests/Limits per CPU e Memory (definiti nei manifest)

✅ **Graceful Shutdown**: Pre-stop hooks per drain connection prima termination

### Note Importanti

#### Per Ambienti Production
1. **Ingress TLS**: Configurare certificati SSL/TLS in `ingress.yaml`
2. **Network Policies**: Applicare restrizioni firewall tra pod
3. **RBAC**: Definire ruoli e permessi per accesso cluster
4. **Audit Logging**: Abilitare audit log Kubernetes
5. **Secret Encryption**: Usare Sealed Secrets o External Secrets Operator
6. **Monitoring Avanzato**: Implementare AlertManager con Prometheus
7. **Backup**: Configurare backup regolari di PersistentVolume

#### Scalabilità Orizzontale
```bash
# Aumentare repliche di un Deployment
kubectl scale deployment data-collector --replicas=3 -n dsbd

# Abilitare Horizontal Pod Autoscaler (HPA)
kubectl autoscale deployment data-collector \
  --min=1 --max=5 \
  --cpu-percent=80 -n dsbd
```

#### Rollout e Versionamento
```bash
# Rollout di nuova versione
kubectl set image deployment/data-collector \
  data-collector=<docker-registry>/data-collector:v1.1 -n dsbd

# Verificare rollout status
kubectl rollout status deployment/data-collector -n dsbd

# Rollback su versione precedente
kubectl rollout undo deployment/data-collector -n dsbd
```

---

## Autori e Contatti
**Progetto**: Homework DSBD - Università di Catania
**Branch**: homework3 (Kubernetes Implementation)
**Ultimo aggiornamento**: Gennaio 2026

Per domande o segnalazioni, fare riferimento alla repository GitHub ufficiale.
