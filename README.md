# CTLM — CNF Topology Lifecycle Manager

A production-style Spring Boot microservice that manages the full lifecycle of **Cloud-Native Network Functions (CNFs)** in a 5G RAN topology. Implements an **event-driven architecture** with Kafka, Neo4j (topology graph), and MySQL (audit trail).

---

## What It Does

When **Site Management** (IPTX / ZTP) creates a Network Slice (NS), CTLM:

1. Receives the NS create trigger via REST → publishes to Kafka
2. Creates the NS and all child CNFs in Neo4j (`INSTANTIATION_IN_PROGRESS`) with characteristics and relationships
3. Automatically fires `INSTANTIATE` + `INSTANTIATION_COMPLETE` for each CNF via Kafka (same key → ordered)
4. Tracks state of every CNF: `INSTANTIATION_IN_PROGRESS → INSTANTIATED` or `INSTANTIATION_FAILED`
5. When all CNFs are `INSTANTIATED` → sends a callback to Site Manager via Kafka

---

## NS / CNF Hierarchy

```
── GnodeB Sub6 (MACRO_SUB6) ──────────────────────────────────────
NS  (NetworkService — super parent)
├── CUCP  [NS_CONTAINS] → subType: MACRO_SUB6_CU_CP, neType: CU-CP
│     ├── CUUP  [PARENT_OF] → subType: MACRO_SUB6_CU_UP, neType: CU-UP
│     └── DU    [PARENT_OF] → subType: MACRO_SUB6_DU,    neType: DU
├── CUUP  [NS_CONTAINS]
└── DU    [NS_CONTAINS]

── GnodeB NGDU_Sub6 (NGDU_SUB6) — same structure, different prefix ─
NS  (NetworkService — super parent)
├── CUCP  [NS_CONTAINS] → subType: NGDU_SUB6_CU_CP, neType: CU-CP
│     ├── CUUP  [PARENT_OF] → subType: NGDU_SUB6_CU_UP, neType: CU-UP
│     └── DU    [PARENT_OF] → subType: NGDU_SUB6_DU,    neType: DU
├── CUUP  [NS_CONTAINS]
└── DU    [NS_CONTAINS]
```

**NS type → Auto-created CNFs:**

Both solutions follow the **same 3-CNF hierarchy**. The only difference is the `subType` prefix.

| nsType | CNFs created | subType prefix |
|---|---|---|
| `GnodeB Sub6` | CUCP (parent) + CUUP + DU | `MACRO_SUB6_*` |
| `GnodeB NGDU_Sub6` | CUCP (parent) + CUUP + DU | `NGDU_SUB6_*` |

**connect.CNF.out** — returns the parent of a node:
- CUUP / DU → CUCP
- CUCP → NS

**connect.CNF.in** — returns the children of a node:
- NS → [CUCP, CUUP, DU]
- CUCP → [CUUP, DU]
- CUUP / DU → []

---

## Full Event-Driven Flow

```
Site Management (IPTX / ZTP)
        │
        │  POST /api/lcm/ns/trigger
        │  { nsId, name, nsType, characteristics }
        ▼
  LcmController
        │  202 Accepted (returns immediately)
        │  publishes to Kafka ──────────────────────────────────────────────────┐
        ▼                                                                        │
Kafka: "lcm-ns-create-trigger"  ◄───────────────────────────────────────────────┘
        │
        ▼
  NsCreateConsumer.consume()
        │
        ├─ TopologyService.triggerNsCreate()
        │       Neo4j creates:
        │         NS   (entityType=NetworkService, subType=GnodeB Sub6)
        │         CUCP (entityType=CNF, subType=MACRO_SUB6_CU_CP, neType=CU-CP)
        │         CUUP (entityType=CNF, subType=MACRO_SUB6_CU_UP, neType=CU-UP)
        │         DU   (entityType=CNF, subType=MACRO_SUB6_DU,    neType=DU)
        │       Relationships:
        │         NS  -[NS_CONTAINS]→ CUCP, CUUP, DU
        │         CUCP-[PARENT_OF]→  CUUP, DU
        │
        ├─ NS state → INSTANTIATION_IN_PROGRESS (Neo4j)
        │
        ├─ NsStateService.registerNsCnfs(nsId, [CUCP, CUUP, DU])
        │
        └─ for each CNF: publish INSTANTIATE + INSTANTIATION_COMPLETE (same key → ordered)
           ──────────────────────────────────────────────────────────────────────────────────┐
                                                                                             │
Kafka: "lcm-commands"  ◄─────────────────────────────────────────────────────────────────────┘
        │
        │  Message 1 per CNF (INSTANTIATE):
        ▼
  LcmCommandConsumer.consume()
        ├─ StateMachine: UNINSTANTIATED → INSTANTIATION_IN_PROGRESS
        ├─ Neo4j: CNF state = INSTANTIATION_IN_PROGRESS  (idempotent)
        ├─ AuditLog: MySQL record written
        └─ NsStateService.onCnfStateChange(cnfId, INSTANTIATION_IN_PROGRESS)

        │  Message 2 per CNF (INSTANTIATION_COMPLETE):
        ▼
  LcmCommandConsumer.consume()
        ├─ StateMachine: INSTANTIATION_IN_PROGRESS → INSTANTIATED
        ├─ Neo4j: CNF state = INSTANTIATED
        ├─ AuditLog: MySQL record written
        └─ NsStateService.onCnfStateChange(cnfId, INSTANTIATED)
                    │
                    └─ all 3 CNFs INSTANTIATED?
                          │ YES
                          ├─ NS state → INSTANTIATED (Neo4j)
                          └─ publish NsCallbackEvent ─────────────────────────┐
                                                                               │
Kafka: "lcm-ns-callback"  ◄────────────────────────────────────────────────────┘
        │
        ▼
  Site Management Kafka consumer
        receives: { nsId, status:"INSTANTIATED", cnfStatuses:{...} }

  ─────────────────────────────────────────────────────────
  Failure path: POST /api/lcm/{cnfId}/fail
  ─────────────────────────────────────────────────────────
        │
        ▼
  NsStateService → NS state = INSTANTIATION_FAILED
                 → publish NsCallbackEvent { status:"INSTANTIATION_FAILED" }
```

---

## Architecture

```
                  ┌──────────────────────────────────────────┐
                  │           Site Management                │
                  │         (IPTX / ZTP system)              │
                  └──────┬───────────────────────────────────┘
                         │ REST trigger               ▲ Kafka callback
                         ▼                            │
                  ┌──────────────┐          ┌─────────────────────┐
                  │ LcmController│          │ "lcm-ns-callback"   │
                  └──────┬───────┘          └─────────────────────┘
                         │ publish                    ▲
                         ▼                            │ NsStateService
              ┌──────────────────────┐      ┌─────────────────────┐
              │ "lcm-ns-create-      │      │  KafkaProducer      │
              │  trigger" topic      │      │  Service            │
              └──────────┬───────────┘      └─────────────────────┘
                         │                            ▲
                         ▼                            │
              ┌──────────────────────┐      ┌─────────────────────┐
              │  NsCreateConsumer    │─────►│  NsStateService     │
              └──────────┬───────────┘      │  (tracks all 3 CNF  │
                         │                  │   states per NS)    │
              ┌──────────▼───────────┐      └─────────────────────┘
              │  TopologyService     │                ▲
              │  Neo4j: NS + CNFs +  │                │
              │  relationships       │      ┌─────────┴───────────┐
              └──────────┬───────────┘      │ LcmCommandConsumer  │
                         │ publish × 3      └─────────────────────┘
                         ▼                            ▲
              ┌──────────────────────┐                │
              │   "lcm-commands"     │────────────────┘
              │   topic              │
              └──────────────────────┘
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `lcm-ns-create-trigger` | LcmController | NsCreateConsumer | NS instantiation trigger from Site Manager |
| `lcm-commands` | LcmController, NsCreateConsumer | LcmCommandConsumer | Per-CNF LCM events |
| `lcm-ns-callback` | NsStateService | Site Manager | NS result: INSTANTIATED / INSTANTIATION_FAILED |
| `lcm-state-changes` | — | — | Reserved for future use |

---

## State Machine (CNF + NS)

Same states apply to both individual CNFs and the NS as a whole.

```
                              INSTANTIATE
 ┌───────────────┐  ─────────────────────────►  ┌─────────────────────────┐
 │ UNINSTANTIATED│                               │ INSTANTIATION_IN_PROGRESS│
 └───────────────┘                               └────────────┬────────────┘
                                                              │               │
                                              INSTANTIATION_  │          FAIL │
                                              COMPLETE        │               ▼
                                                              ▼        ┌──────────────────┐
                                                       ┌─────────────┐ │ INSTANTIATION_   │
                                             SCALE ◄───│ INSTANTIATED│ │ FAILED           │
                                          TERMINATE ◄──└──────┬──────┘ └──────────────────┘
                                                      ▲       │                  ▲
                                               ┌──────┴──┐ ┌──▼──────────┐      │ FAIL
                                               │ SCALING │ │ TERMINATING │──────┘
                                               └────┬────┘ └──────┬──────┘
                                      SCALE_COMPLETE │             │ TERMINATION_COMPLETE
                                                     ▼             ▼
                                               INSTANTIATED  ┌────────────┐
                                                             │ TERMINATED │
                                                             └────────────┘
```

| State | Meaning |
|---|---|
| `UNINSTANTIATED` | Initial — CNF not yet deployed |
| `INSTANTIATION_IN_PROGRESS` | INSTANTIATE fired — deploying now |
| `INSTANTIATED` | Deployment complete — CNF is live |
| `SCALING` | Scale operation running |
| `TERMINATING` | Termination running |
| `TERMINATED` | Permanently stopped |
| `INSTANTIATION_FAILED` | Any FAIL event occurred |

**NS-level state** (stored in Neo4j, updated by `NsStateService`):

| NS State | Trigger |
|---|---|
| `INSTANTIATION_IN_PROGRESS` | NS trigger received, CNFs deploying |
| `INSTANTIATED` | All 3 CNFs reach `INSTANTIATED` |
| `INSTANTIATION_FAILED` | Any CNF reaches `INSTANTIATION_FAILED` |

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Backend | Java 21, Spring Boot 3.2.3 | Core service |
| Messaging | Apache Kafka | Async decoupling, durable command log |
| Graph DB | Neo4j 5 | Topology: NS/CNF nodes + relationships |
| Relational | MySQL 8 | Audit trail (every state transition) |
| Build | Maven 3.9 | Dependency management |
| Container | Docker, Docker Compose | Zero-setup local dev |
| K8s | Kubernetes manifests | Production deployment |
| CI/CD | GitHub Actions | Test gate on every push |

---

## Project Structure

```
ctlm/
├── src/main/java/com/ctlm/
│   ├── CtlmApplication.java
│   ├── controller/
│   │   ├── LcmController.java          # /api/lcm — lifecycle + NS trigger
│   │   └── TopologyController.java     # /api/topology — graph management
│   ├── statemachine/
│   │   ├── LcmStateMachineService.java # FSM: ConcurrentHashMap + transitions
│   │   ├── LcmState.java               # UNINSTANTIATED|INSTANTIATION_IN_PROGRESS|INSTANTIATED|...
│   │   └── LcmEvent.java               # INSTANTIATE|INSTANTIATION_COMPLETE|FAIL|...
│   ├── topology/
│   │   ├── CnfNode.java                # @Node: entityType, subType, neType, characteristics
│   │   │                               #   + PARENT_OF + NS_CONTAINS + DEPENDS_ON
│   │   ├── CnfNodeRepository.java      # Cypher queries: findDirectParent, findNsParent, ...
│   │   ├── TopologyService.java        # NS trigger, connectOut/In, addChild, addNsCnf
│   │   └── NsStateService.java         # Tracks CNF states per NS, fires Kafka callback
│   ├── kafka/
│   │   ├── LcmCommand.java             # Per-CNF command: {cnfId, event, message}
│   │   ├── NsCallbackEvent.java        # NS result callback: {nsId, status, cnfStatuses}
│   │   ├── KafkaProducerService.java   # publishCommand | publishNsCreateTrigger | publishNsCallback
│   │   ├── LcmCommandConsumer.java     # "lcm-commands" listener → state machine + NsStateService
│   │   └── NsCreateConsumer.java       # "lcm-ns-create-trigger" listener → creates topology
│   ├── audit/
│   │   ├── AuditService.java           # Logs every CNF state transition
│   │   ├── AuditLog.java               # @Entity: MySQL table
│   │   └── AuditLogRepository.java
│   ├── dto/
│   │   ├── NsTriggerRequest.java       # NS trigger body: nsId, name, nsType, characteristics[]
│   │   ├── NsRequest.java              # Manual NS registration
│   │   ├── EntityResponse.java         # Unified entity response: node + connectOut + connectIn
│   │   ├── CnfNodeRequest.java
│   │   ├── LcmRequest.java
│   │   └── LcmResponse.java
│   └── config/
│       └── KafkaConfig.java            # 4 topic beans
├── k8s/
│   ├── 00-namespace.yaml
│   ├── 01-mysql.yaml
│   ├── 02-neo4j.yaml
│   ├── 03-kafka.yaml
│   └── 04-app.yaml
├── Dockerfile
├── docker-compose.yml
└── .github/workflows/ci.yml
```

---

## Run Locally (Docker Compose)

```bash
git clone <repo-url> && cd ctlm
docker-compose up --build
```

| Service | URL |
|---|---|
| REST API | http://localhost:8080 |
| Health | http://localhost:8080/actuator/health |
| Neo4j Browser | http://localhost:7474 |
| MySQL | localhost:3306 (root/root) |

---

## All API Endpoints with curl

Set your base URL once:
```bash
BASE=http://localhost:8080
```

---

### PING

```bash
curl $BASE/api/lcm/ping
# → ping Successfully
```

---

### NS TRIGGER FLOW (Site Manager → CTLM)

#### Step 1 — Site Manager fires the NS create trigger

```bash
curl -s -X POST $BASE/api/lcm/ns/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "nsId"  : "ns-001",
    "name"  : "uhn1tky817500083",
    "nsType": "GnodeB Sub6",
    "characteristics": [
      {"name": "status",      "value": "INSTANTIATION_PENDING"},
      {"name": "technology",  "value": "T5G"},
      {"name": "vendor",      "value": "ALTIOSTAR"},
      {"name": "domain",      "value": "RAN"},
      {"name": "displayName", "value": "HELMSUB6JUN8"}
    ]
  }'
# → 202 Accepted
# {
#   "cnfId": "ns-001",
#   "currentState": "INSTANTIATION_IN_PROGRESS",
#   "message": "NS create trigger accepted — processing asynchronously"
# }
```

**What happens automatically in the background (no further calls needed):**
1. Neo4j creates: NS node + CUCP + CUUP + DU, all with state `INSTANTIATION_IN_PROGRESS`
2. NS_CONTAINS and PARENT_OF relationships set
3. For each CNF: `INSTANTIATE` + `INSTANTIATION_COMPLETE` published to Kafka (same key → ordered)
4. LcmCommandConsumer processes them: `UNINSTANTIATED → INSTANTIATION_IN_PROGRESS → INSTANTIATED`
5. When all 3 CNFs reach `INSTANTIATED`: NS state → `INSTANTIATED`, callback fired to `lcm-ns-callback`

Kafka message on `lcm-ns-callback`:
```json
{
  "nsId": "ns-001",
  "nsName": "uhn1tky817500083",
  "nsType": "GnodeB Sub6",
  "status": "INSTANTIATED",
  "message": "All CNFs instantiated successfully",
  "timestamp": "2026-07-21T10:30:00",
  "cnfStatuses": {
    "ns-001-macro-sub6-cu-cp": "INSTANTIATED",
    "ns-001-macro-sub6-cu-up": "INSTANTIATED",
    "ns-001-macro-sub6-du":    "INSTANTIATED"
  }
}
```

---

#### Step 2 — Check NS instantiation status

```bash
curl -s $BASE/api/lcm/ns/ns-001/status
# → {
#     "ns-001-macro-sub6-cu-cp": "INSTANTIATED",
#     "ns-001-macro-sub6-cu-up": "INSTANTIATED",
#     "ns-001-macro-sub6-du":    "INSTANTIATED"
#   }
```

---

#### Failure scenario — mark a CNF as failed

```bash
# If CUUP fails during instantiation
curl -s -X POST $BASE/api/lcm/ns-001-macro-sub6-cu-up/fail \
  -H "Content-Type: application/json" \
  -d '{"cnfName":"CUUP", "message":"Pod OOMKilled"}'

# Kafka "lcm-ns-callback" receives:
# { "status": "INSTANTIATION_FAILED", "message": "Instantiation failed for CNF: ns-001-macro-sub6-cu-up" }
```

---

### NGDU_SUB6 NS TRIGGER

Same 3-CNF flow as MacroSub6 — only the `nsType` and subType prefix differ.

```bash
curl -s -X POST $BASE/api/lcm/ns/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "nsId"  : "ns-ngdu-001",
    "name"  : "ngdu-node-001",
    "nsType": "GnodeB NGDU_Sub6",
    "characteristics": [
      {"name": "technology", "value": "T5G"},
      {"name": "vendor",     "value": "ALTIOSTAR"}
    ]
  }'
# Creates:
#   NS   (subType: GnodeB NGDU_Sub6)
#   CUCP (subType: NGDU_SUB6_CU_CP, neType: CU-CP)  ← parent
#   CUUP (subType: NGDU_SUB6_CU_UP, neType: CU-UP)  ← child of CUCP
#   DU   (subType: NGDU_SUB6_DU,    neType: DU)      ← child of CUCP
#
# CNF IDs:
#   ns-ngdu-001-ngdu-sub6-cu-cp
#   ns-ngdu-001-ngdu-sub6-cu-up
#   ns-ngdu-001-ngdu-sub6-du
#
# No further calls needed — INSTANTIATE + INSTANTIATION_COMPLETE auto-fired for each CNF.
# → callback on "lcm-ns-callback": { "status": "INSTANTIATED", cnfStatuses: { ...all INSTANTIATED } }
```

---

### INDIVIDUAL CNF LCM

#### Get current state

```bash
curl -s $BASE/api/lcm/ns-001-macro-sub6-cu-cp/state
# → { "cnfId": "ns-001-macro-sub6-cu-cp", "currentState": "INSTANTIATED" }
```

#### Instantiate manually

```bash
curl -s -X POST $BASE/api/lcm/cnf-001/instantiate \
  -H "Content-Type: application/json" \
  -d '{"cnfName":"MyNF", "message":"Manual deploy"}'
```

#### Scale

```bash
curl -s -X POST $BASE/api/lcm/cnf-001/scale \
  -H "Content-Type: application/json" \
  -d '{"message":"Scale for peak load"}'

curl -s -X POST $BASE/api/lcm/cnf-001/complete-scale
```

#### Terminate

```bash
curl -s -X POST $BASE/api/lcm/cnf-001/terminate
curl -s -X POST $BASE/api/lcm/cnf-001/complete-termination
```

#### Audit history

```bash
curl -s $BASE/api/lcm/ns-001-macro-sub6-cu-cp/history
# → [ { "fromState":"UNINSTANTIATED", "toState":"INSTANTIATION_IN_PROGRESS", "event":"INSTANTIATE", ... }, ... ]
```

---

### TOPOLOGY — NS Management

#### List all NS nodes

```bash
curl -s $BASE/api/topology/ns
```

#### Register an NS manually (no auto-CNF creation)

```bash
curl -s -X POST $BASE/api/topology/ns \
  -H "Content-Type: application/json" \
  -d '{"nsId":"ns-manual-001", "name":"Manual-NS-1"}'
```

#### Link a CNF under an NS manually

```bash
curl -s -X POST $BASE/api/topology/ns-manual-001/cnf/cucp-001
```

#### Set PARENT_OF manually (CUCP → CUUP)

```bash
curl -s -X POST $BASE/api/topology/cucp-001/child/cuup-001
curl -s -X POST $BASE/api/topology/cucp-001/child/du-001
```

---

### TOPOLOGY — CNF Management

#### Register a CNF manually

```bash
curl -s -X POST $BASE/api/topology/cnf \
  -H "Content-Type: application/json" \
  -d '{"cnfId":"cucp-001", "name":"CUCP-Test", "type":"MACRO_SUB6_CU_CP"}'
```

#### Get raw CNF node

```bash
curl -s $BASE/api/topology/ns-001-macro-sub6-cu-cp
```

#### Legacy DEPENDS_ON

```bash
curl -s -X POST $BASE/api/topology/cnf-b/dependencies/cnf-a
curl -s $BASE/api/topology/cnf-b/dependencies
```

---

### TOPOLOGY — connect.CNF.out / connect.CNF.in

#### connect.CNF.out — get parent

```bash
# CUUP → returns CUCP
curl -s $BASE/api/topology/ns-001-macro-sub6-cu-up/connect/out

# CUCP → returns NS
curl -s $BASE/api/topology/ns-001-macro-sub6-cu-cp/connect/out

# NS → 404 (no parent)
curl -s $BASE/api/topology/ns-001/connect/out
```

#### connect.CNF.in — get children

```bash
# NS → [CUCP, CUUP, DU]
curl -s $BASE/api/topology/ns-001/connect/in

# CUCP → [CUUP, DU]
curl -s $BASE/api/topology/ns-001-macro-sub6-cu-cp/connect/in

# CUUP → []
curl -s $BASE/api/topology/ns-001-macro-sub6-cu-up/connect/in
```

---

### TOPOLOGY — Unified Entity Lookup

Works for any ID — NS or CNF. Returns full details + connectOut + connectIn.

```bash
# Look up NS
curl -s $BASE/api/topology/entity/ns-001
# → {
#     "id": "ns-001",
#     "entityType": "NetworkService",
#     "subType": "GnodeB Sub6",
#     "currentState": "INSTANTIATED",
#     "technology": "T5G",
#     "vendor": "ALTIOSTAR",
#     "characteristics": {"status":"INSTANTIATED", ...},
#     "connectOut": null,
#     "connectIn": [
#       {"id":"ns-001-macro-sub6-cu-cp", "entityType":"CNF", "subType":"MACRO_SUB6_CU_CP", "neType":"CU-CP"},
#       {"id":"ns-001-macro-sub6-cu-up", "entityType":"CNF", "subType":"MACRO_SUB6_CU_UP", "neType":"CU-UP"},
#       {"id":"ns-001-macro-sub6-du",    "entityType":"CNF", "subType":"MACRO_SUB6_DU",    "neType":"DU"}
#     ]
#   }

# Look up CUUP
curl -s $BASE/api/topology/entity/ns-001-macro-sub6-cu-up
# → {
#     "id": "ns-001-macro-sub6-cu-up",
#     "entityType": "CNF",
#     "subType": "MACRO_SUB6_CU_UP",
#     "neType": "CU-UP",
#     "currentState": "INSTANTIATED",
#     "connectOut": {"id":"ns-001-macro-sub6-cu-cp", "subType":"MACRO_SUB6_CU_CP", "neType":"CU-CP"},
#     "connectIn": []
#   }
```

---

### NEO4J BROWSER QUERIES

Open http://localhost:7474 (no login, auth disabled in dev).

```cypher
// All nodes
MATCH (n:CnfNode) RETURN n

// Full NS topology with relationships
MATCH (ns:CnfNode {entityType:'NetworkService'})-[r]->(c:CnfNode)
RETURN ns, r, c

// PARENT_OF hierarchy
MATCH (cucp:CnfNode)-[r:PARENT_OF]->(child:CnfNode)
RETURN cucp, r, child

// Find all CNFs under a specific NS
MATCH (ns:CnfNode {cnfId:'ns-001'})-[:NS_CONTAINS]->(c:CnfNode)
RETURN c.cnfId, c.subType, c.neType, c.currentState

// connect.CNF.out — find parent of CUUP
MATCH (p:CnfNode)-[:PARENT_OF]->(c:CnfNode {cnfId:'ns-001-macro-sub6-cu-up'})
RETURN p

// All NS nodes and their states
MATCH (n:CnfNode {entityType:'NetworkService'})
RETURN n.cnfId, n.name, n.subType, n.currentState
```

---

## Invalid Transition Test

```bash
# Put cnf-001 in INSTANTIATED state first
curl -s -X POST $BASE/api/lcm/cnf-001/instantiate
curl -s -X POST $BASE/api/lcm/cnf-001/complete-instantiation

# Try to instantiate again — should be rejected (INSTANTIATED + INSTANTIATE is invalid)
curl -s -X POST $BASE/api/lcm/cnf-001/instantiate
# → Kafka consumer logs: "Invalid state transition for cnfId=cnf-001: state=INSTANTIATED, event=INSTANTIATE"
```

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `MYSQL_HOST` | localhost | MySQL hostname |
| `MYSQL_PORT` | 3306 | MySQL port |
| `MYSQL_DB` | ctlmdb | Database name |
| `MYSQL_USER` | root | MySQL username |
| `MYSQL_PASSWORD` | root | MySQL password |
| `NEO4J_HOST` | localhost | Neo4j hostname |
| `NEO4J_PORT` | 7687 | Neo4j Bolt port |
| `NEO4J_PASSWORD` | (empty) | Neo4j password |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka broker |

---

## Run Tests

```bash
mvn test
```

---

## K8s Deployment (Minikube)

```bash
minikube start
eval $(minikube docker-env)
docker build -t ctlm-app:latest .

kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-mysql.yaml
kubectl apply -f k8s/02-neo4j.yaml
kubectl apply -f k8s/03-kafka.yaml
kubectl apply -f k8s/04-app.yaml

# Watch pods come up
kubectl get pods -n ctlm -w

# Access the app
minikube service ctlm-app -n ctlm --url
```

---

## Interview Talking Points

**"Tell me about this project"**
> CTLM is a lifecycle manager for 5G CNFs. When Site Management triggers an NS create, CTLM auto-creates the NS and all child CNFs (CUCP, CUUP, DU) with correct topology relationships in Neo4j, then fires INSTANTIATE and INSTANTIATION_COMPLETE for each CNF via Kafka automatically. It tracks the state of every CNF independently, and when all three reach INSTANTIATED, it fires a callback to Site Management. The API returns 202 immediately — all processing is async via Kafka.

**"Why Kafka?"**
> Decoupling and durability. The REST call completes in milliseconds. If the consumer is down, the message stays in Kafka and processes when it recovers. Direct calls would block and lose the command on failure.

**"Why Neo4j?"**
> The NS/CNF hierarchy is a graph: NS contains CUCP, CUUP, DU; CUCP is parent of CUUP and DU. In MySQL this would require self-join tables and recursive queries. In Neo4j, traversals like "give me the parent of this CNF" or "give me all CNFs under this NS" are single Cypher queries.

**"How does NsStateService work?"**
> It keeps an in-memory ConcurrentHashMap of `nsId → {cnfId → LcmState}`. Every time LcmCommandConsumer transitions a CNF state, it calls `onCnfStateChange()`. When all CNFs reach INSTANTIATED, it atomically marks the callback as sent (preventing duplicates) and publishes the result to Kafka.

**Design patterns used:**

| Pattern | Where |
|---|---|
| State Machine | `LcmStateMachineService` |
| Command Pattern | `LcmCommand` over Kafka |
| Observer | `NsStateService` watching CNF state changes |
| Repository | `CnfNodeRepository`, `AuditLogRepository` |
| DTO | `NsTriggerRequest`, `EntityResponse`, `NsCallbackEvent` |
