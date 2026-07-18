# CTLM — CNF Topology Lifecycle Manager

A production-style Spring Boot microservice that manages the full lifecycle of **Cloud-Native Network Functions (CNFs)** using an **event-driven state machine** backed by Kafka, Neo4j, and MySQL.

Built as a portfolio project to demonstrate backend engineering patterns: async command processing, finite state machines, graph-based dependency modeling, audit logging, and containerized deployment.

---

## Why This Project Exists

Modern infrastructure management systems need to track **what state a resource is in** and **how it got there**. A simple boolean `isRunning` flag doesn't cut it — you need:

- A **state machine** to enforce valid lifecycle transitions (can't scale something that isn't running)
- **Async processing** so API callers aren't blocked waiting for long operations
- A **graph DB** to model dependencies (if CNF-A depends on CNF-B, only terminate B after A is gone)
- An **audit trail** to answer "who changed what, when, and why"

CTLM solves all of these in a single, containerized service.

---

## Architecture

```
HTTP Request
     │
     ▼
┌──────────────┐   publishes    ┌─────────────────┐
│ LcmController│ ─────────────► │  Kafka Topic     │
│              │   LcmCommand   │  lcm-commands    │
└──────────────┘                └────────┬────────┘
                                         │ @KafkaListener
                                         ▼
                               ┌──────────────────────┐
                               │  LcmCommandConsumer  │
                               │  (async processing)  │
                               └──────────┬───────────┘
                                          │
                          ┌───────────────▼───────────────┐
                          │     LcmStateMachineService     │
                          │  (validates state transitions) │
                          └──────────┬────────────────────┘
                                     │
              ┌──────────────────────┼────────────────────┐
              ▼                      ▼                     ▼
   ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │  TopologyService│   │   AuditService   │   │  HTTP Response   │
   │  (Neo4j graph)  │   │  (MySQL log)     │   │  202 Accepted    │
   └─────────────────┘   └──────────────────┘   └──────────────────┘
```

**Key design choice:** HTTP responds `202 Accepted` immediately. Actual state transition happens asynchronously via Kafka — the API never blocks on processing time.

---

## LCM State Machine

```
                   ┌──────────────────────────────────────────┐
                   │          FAIL (from any active state)     │
                   ▼                                           │
 ┌──────────────┐  INSTANTIATE  ┌─────────────┐               │
 │UNINSTANTIATED│ ────────────► │ INSTANTIATING│ ─────────────►┤
 └──────────────┘               └──────┬──────┘               │
                                       │ INSTANTIATION_COMPLETE│  ┌────────┐
                                       ▼                       │  │ FAILED │
                                 ┌─────────┐                   │  └────────┘
                      ┌──────────│ RUNNING │──────────┐        │
                      │ SCALE    └─────────┘ TERMINATE│        │
                      ▼                               ▼        │
                 ┌─────────┐                 ┌────────────┐    │
                 │ SCALING │                 │ TERMINATING│────┘
                 └────┬────┘                 └─────┬──────┘
                      │ SCALE_COMPLETE              │ TERMINATION_COMPLETE
                      └──────► RUNNING             ▼
                                             ┌────────────┐
                                             │ TERMINATED │
                                             └────────────┘
```

### Valid Transitions

| Current State  | Event                  | Next State    |
|----------------|------------------------|---------------|
| UNINSTANTIATED | INSTANTIATE            | INSTANTIATING |
| INSTANTIATING  | INSTANTIATION_COMPLETE | RUNNING       |
| INSTANTIATING  | FAIL                   | FAILED        |
| RUNNING        | SCALE                  | SCALING       |
| RUNNING        | TERMINATE              | TERMINATING   |
| SCALING        | SCALE_COMPLETE         | RUNNING       |
| SCALING        | FAIL                   | FAILED        |
| TERMINATING    | TERMINATION_COMPLETE   | TERMINATED    |
| TERMINATING    | FAIL                   | FAILED        |

Any unlisted event throws `IllegalStateException` — invalid transitions are rejected by design.

---

## Tech Stack

| Layer       | Technology                      | Why                                              |
|-------------|---------------------------------|--------------------------------------------------|
| Backend     | Java 17, Spring Boot 3.2.3      | Industry standard, rich ecosystem                |
| Messaging   | Apache Kafka 3.6 (KRaft)        | Async decoupling, durable command log            |
| Graph DB    | Neo4j 5 Community               | Natural fit for dependency relationships         |
| Relational  | MySQL 8.0                       | Structured audit logs, SQL queryable             |
| Build       | Maven 3.9.6                     | Dependency management, lifecycle                 |
| Testing     | JUnit 5, Mockito                | Unit + integration test coverage                 |
| Container   | Docker, Docker Compose          | Zero-setup local dev                             |
| K8s         | Kubernetes manifests            | Production deployment                            |
| CI/CD       | GitHub Actions                  | Automated test gate on every push                |

---

## Option 1: Run with Docker Compose (Recommended for local dev)

### Prerequisites
- Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- Nothing else — no local Java, Maven, or databases needed

### Steps

```bash
# 1. Clone the repo
git clone <repo-url>
cd ctlm

# 2. Start everything
docker-compose up --build
```

Docker will:
1. Build the app image using Maven inside Docker (downloads deps, compiles, packages JAR)
2. Start MySQL, Neo4j, Kafka with health checks
3. Wait for all three to be healthy before starting the app
4. App is ready when you see: `Started CtlmApplication`

First run: ~2-3 minutes (image pulls). Subsequent runs: ~30-60 seconds.

### Service URLs (Docker)

| Service       | URL                                    |
|---------------|----------------------------------------|
| REST API      | http://localhost:8080                  |
| Health check  | http://localhost:8080/actuator/health  |
| Neo4j Browser | http://localhost:7474                  |
| MySQL         | localhost:3306 (user: root / root)     |

### Stop

```bash
# Stop containers, keep data
docker-compose down

# Stop and wipe all data (fresh start)
docker-compose down -v
```

### Port conflicts

If you see `bind: address already in use`, one of these ports is taken on your machine:

| Port | Service |
|------|---------|
| 8080 | App     |
| 3306 | MySQL   |
| 7474 | Neo4j HTTP |
| 7687 | Neo4j Bolt |
| 9092 | Kafka   |

Fix: stop the conflicting local service, or change the host port in `docker-compose.yml` (left side of `ports:` mapping).

---

## Option 2: Run on Kubernetes (Minikube)

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### Steps

```bash
# 1. Start minikube
minikube start

# 2. Point your local Docker to minikube's Docker daemon
#    This lets minikube use locally built images without a registry
eval $(minikube docker-env)

# 3. Build the image inside minikube's Docker context
docker build -t ctlm-app:latest .

# 4. Apply all K8s manifests in order
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-mysql.yaml
kubectl apply -f k8s/02-neo4j.yaml
kubectl apply -f k8s/03-kafka.yaml
kubectl apply -f k8s/04-app.yaml
```

### Watch startup progress

```bash
# Watch all pods come up (takes 2-4 minutes)
kubectl get pods -n ctlm -w

# What you should see eventually:
# NAME                         READY   STATUS    RESTARTS
# mysql-xxx                    1/1     Running   0
# neo4j-xxx                    1/1     Running   0
# kafka-xxx                    1/1     Running   0
# ctlm-app-xxx                 1/1     Running   0
```

The `ctlm-app` pod has 3 initContainers that wait for MySQL, Kafka, and Neo4j ports to be reachable before starting the app.

### Access the app

```bash
# Get the minikube URL for the app
minikube service ctlm-app -n ctlm --url
# Output: http://192.168.xx.xx:30080  ← use this as your base URL
```

Or use port-forward for direct access:

```bash
kubectl port-forward svc/ctlm-app 8080:8080 -n ctlm
# App now at http://localhost:8080
```

### Access Neo4j Browser on K8s

```bash
kubectl port-forward svc/neo4j 7474:7474 7687:7687 -n ctlm
# Browser at http://localhost:7474
```

### Teardown

```bash
# Delete all resources
kubectl delete namespace ctlm

# Or delete piece by piece
kubectl delete -f k8s/
```

### Notes on K8s setup

| Item | Detail |
|------|--------|
| `imagePullPolicy: Never` | Image must be built locally inside minikube's Docker context (step 2-3 above). If skipped, pod stays in `ErrImagePull`. |
| Neo4j & Kafka storage | No PVC — data is ephemeral. Pod restart = data loss. Fine for dev/demo. |
| MySQL storage | Has a 1Gi PVC — data survives pod restarts. |
| Secrets | Passwords in ConfigMap as plaintext. Acceptable for dev; use `Secret` resource in production. |
| App restart on init | If app pod restarts once (CrashLoopBackOff) on first deploy, that's normal — DB may need a few extra seconds after port opens. K8s will restart and it'll succeed. |

---

## Test the API — Step by Step

These commands work for both Docker (`localhost:8080`) and K8s (replace with your minikube URL).

Set your base URL:
```bash
BASE=http://localhost:8080   # Docker
# BASE=http://192.168.xx.xx:30080  # K8s
```

### Step 1 — Register CNFs in topology

```bash
curl -X POST $BASE/api/topology/cnf \
  -H "Content-Type: application/json" \
  -d '{"cnfId": "cnf-001", "name": "Sub6-NR-1", "type": "Sub6"}'

curl -X POST $BASE/api/topology/cnf \
  -H "Content-Type: application/json" \
  -d '{"cnfId": "cnf-002", "name": "NGDU-1", "type": "NGDU"}'
```

### Step 2 — Define dependency

```bash
# cnf-002 depends on cnf-001
curl -X POST $BASE/api/topology/cnf-002/dependencies/cnf-001
```

### Step 3 — Check initial state

```bash
curl $BASE/api/lcm/cnf-001/state
# Expected: {"currentState":"UNINSTANTIATED"}
```

### Step 4 — Instantiate

```bash
curl -X POST $BASE/api/lcm/cnf-001/instantiate \
  -H "Content-Type: application/json" \
  -d '{"cnfId": "cnf-001", "cnfName": "Sub6-NR-1", "message": "Initial deployment"}'

curl $BASE/api/lcm/cnf-001/state
# Expected: INSTANTIATING
```

### Step 5 — Complete instantiation

```bash
curl -X POST $BASE/api/lcm/cnf-001/complete-instantiation

curl $BASE/api/lcm/cnf-001/state
# Expected: RUNNING
```

### Step 6 — Scale

```bash
curl -X POST $BASE/api/lcm/cnf-001/scale \
  -H "Content-Type: application/json" \
  -d '{"message": "Scale out for peak load"}'

curl -X POST $BASE/api/lcm/cnf-001/complete-scale

curl $BASE/api/lcm/cnf-001/state
# Expected: RUNNING (back from SCALING)
```

### Step 7 — Terminate

```bash
curl -X POST $BASE/api/lcm/cnf-001/terminate
curl -X POST $BASE/api/lcm/cnf-001/complete-termination

curl $BASE/api/lcm/cnf-001/state
# Expected: TERMINATED
```

### Step 8 — View full audit trail

```bash
curl $BASE/api/lcm/cnf-001/history
# Returns all transitions with timestamps, newest first
```

### Step 9 — Test invalid transition (should fail)

```bash
curl -X POST $BASE/api/lcm/cnf-001/scale \
  -H "Content-Type: application/json" \
  -d '{"message": "scale a terminated CNF - should be rejected"}'
# Expected: error response — IllegalStateException
```

### Step 10 — View graph in Neo4j Browser

1. Open http://localhost:7474 (Docker) or port-forward 7474 (K8s)
2. No login needed (auth disabled in dev)
3. Run: `MATCH (n:CnfNode) RETURN n` — all CNF nodes
4. Run: `MATCH (a)-[r:DEPENDS_ON]->(b) RETURN a,r,b` — dependency graph

---

## Project Structure

```
ctlm/
├── src/main/java/com/ctlm/
│   ├── CtlmApplication.java           # Entry point (@SpringBootApplication)
│   ├── controller/
│   │   ├── LcmController.java         # /api/lcm — lifecycle commands
│   │   └── TopologyController.java    # /api/topology — graph management
│   ├── statemachine/
│   │   ├── LcmStateMachineService.java # Core FSM — ConcurrentHashMap + transitions map
│   │   ├── LcmState.java              # 7 states enum
│   │   └── LcmEvent.java              # 7 events enum
│   ├── topology/
│   │   ├── TopologyService.java       # Neo4j CRUD + dependency management
│   │   ├── CnfNode.java               # @Node entity with DEPENDS_ON relationship
│   │   └── CnfNodeRepository.java     # Neo4jRepository
│   ├── audit/
│   │   ├── AuditService.java          # Logs every state transition
│   │   ├── AuditLog.java              # @Entity — MySQL table lcm_audit_log
│   │   └── AuditLogRepository.java    # JpaRepository with custom query
│   ├── kafka/
│   │   ├── LcmCommandConsumer.java    # @KafkaListener — async command processor
│   │   ├── KafkaProducerService.java  # KafkaTemplate wrapper
│   │   └── LcmCommand.java            # Message POJO (cnfId, event, message)
│   ├── dto/
│   │   ├── LcmRequest.java            # POST body
│   │   ├── LcmResponse.java           # API response
│   │   └── CnfNodeRequest.java        # Topology registration body
│   └── config/
│       └── KafkaConfig.java           # Topic bean definitions
├── src/test/java/com/ctlm/
│   ├── CtlmApplicationTests.java      # Basic FSM tests
│   └── statemachine/
│       └── LcmStateMachineServiceTest.java  # Full transition matrix tests
├── k8s/
│   ├── 00-namespace.yaml              # ctlm namespace
│   ├── 01-mysql.yaml                  # MySQL Deployment + Service + PVC
│   ├── 02-neo4j.yaml                  # Neo4j Deployment + Service
│   ├── 03-kafka.yaml                  # Kafka Deployment + Service (KRaft)
│   └── 04-app.yaml                    # App Deployment + Service + ConfigMap
├── .github/workflows/ci.yml           # GitHub Actions — mvn test on push
├── Dockerfile                         # Multi-stage: Maven builder + Alpine JRE
└── docker-compose.yml                 # Full local stack with health checks
```

---

## Running Tests

```bash
# If you have Maven locally
mvn test

# Without local Maven (Docker)
docker run --rm -v $(pwd):/app -w /app maven:3.9-eclipse-temurin-17 mvn test
```

Tests cover:
- Initial state is `UNINSTANTIATED`
- All 9 valid transitions
- Full lifecycle sequence: instantiate → running → scale → terminate
- Fail path from INSTANTIATING and SCALING
- `IllegalStateException` on invalid transitions

---

## Interview Talking Points

### "Tell me about this project"

> "CTLM manages the lifecycle of distributed resources — tracking them through states like UNINSTANTIATED, RUNNING, SCALING, TERMINATED. The core challenge is enforcing valid state transitions while keeping the API non-blocking.
>
> I built it with a state machine for transition validation, Kafka for async decoupling, Neo4j for dependency graph, and MySQL for audit trail. It's fully containerized — Docker Compose for local dev, Kubernetes manifests for production. The API returns 202 immediately; the actual processing happens async via Kafka."

---

### "Why Kafka instead of calling the state machine directly?"

> "Two reasons: decoupling and durability. The controller doesn't need to know about the state machine internals. And if the consumer is temporarily down, the command stays in Kafka and gets processed when it recovers — a direct call would just fail and lose the command."

---

### "Why Neo4j for topology?"

> "Dependencies between resources are a graph problem. In MySQL I'd need a self-join table and recursive queries. In Neo4j a dependency is just a DEPENDS_ON relationship between nodes — and traversals like 'find everything that depends on this CNF' are a single Cypher query."

---

### "How does the state machine work internally?"

> "It's a ConcurrentHashMap keyed by CNF ID storing the current state. A static transitions map defines which events are valid from each state. When an event arrives via Kafka, I look up the current state, check if the event is valid, apply it, and persist. If the event isn't in the map for that state, I throw IllegalStateException. Simple, thread-safe, no external library."

---

### "What design patterns did you use?"

| Pattern | Where |
|---|---|
| State Machine | `LcmStateMachineService` — all transition logic in one place |
| Command Pattern | `LcmCommand` — serializable command sent over Kafka |
| Repository Pattern | `AuditLogRepository`, `CnfNodeRepository` — data access abstraction |
| DTO Pattern | `LcmRequest`, `LcmResponse` — clean API contracts, never expose entities |

---

### "What's a bug you fixed?"

> "When a CNF operation failed due to a missing implementation class, the state stayed INPROGRESS instead of transitioning to FAILED. Root cause: ClassNotFoundException was thrown before the catch block that publishes the FAILED event. I added an explicit catch for ClassNotFoundException that marks the state FAILED and publishes the failure event — same as what happens for runtime failures."

---

### "How would you scale this in production?"

> "State is currently in-memory per instance. To run multiple replicas: move state to Redis — fast, distributed, survives restarts. Kafka already handles fanout with consumer groups. Neo4j and MySQL scale independently. After that the app is stateless — horizontal scaling is trivial. I'd also add a Dead Letter Queue for failed Kafka messages and Prometheus metrics on state transitions."

---

## Environment Variables

| Variable                | Default        | Purpose               |
|-------------------------|----------------|-----------------------|
| MYSQL_HOST              | localhost      | MySQL hostname        |
| MYSQL_PORT              | 3306           | MySQL port            |
| MYSQL_DB                | ctlmdb         | Database name         |
| MYSQL_USER              | root           | MySQL username        |
| MYSQL_PASSWORD          | root           | MySQL password        |
| NEO4J_HOST              | localhost      | Neo4j hostname        |
| NEO4J_PORT              | 7687           | Neo4j Bolt port       |
| NEO4J_PASSWORD          | (empty)        | Neo4j password        |
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Kafka broker address  |
