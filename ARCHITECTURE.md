# Architecture — E-commerce EDA (Formation Kafka)

## 1. Vue d'ensemble

Application e-commerce composée de **6 microservices** communiquant principalement par
événements Kafka (pattern **Saga chorégraphiée**), avec quelques appels REST synchrones
là où c'est pertinent.

| Service | Rôle | Base de données | Format messages |
|---|---|---|---|
| `order-service` | Point d'entrée REST, cycle de vie des commandes (state machine) | PostgreSQL | Avro |
| `inventory-service` | Catalogue produits + réservation/libération de stock | PostgreSQL | Avro |
| `payment-service` | Traitement des paiements (simulé, avec échecs aléatoires) | PostgreSQL | Avro |
| `shipping-service` | Préparation et suivi des livraisons | PostgreSQL | JSON |
| `notification-service` | Emails/SMS simulés (virtual threads Java 21) | — (stateless) | consomme Avro + JSON |
| `analytics-service` | Agrégats temps réel (CA, commandes/min, taux d'échec paiement) | PostgreSQL | consomme tout |

## 2. Flux nominal (Saga chorégraphiée)

```
Client
  │ POST /api/orders (REST)
  ▼
order-service ──REST GET /products/{id}──▶ inventory-service   (validation prix, synchrone)
  │
  │ publie OrderCreated ──────────────▶ topic: order.events
  ▼
inventory-service (consomme order.events)
  │ réserve le stock
  │ publie StockReserved | StockRejected ─▶ topic: inventory.events
  ▼
payment-service (consomme inventory.events)
  │ traite le paiement
  │ publie PaymentSucceeded | PaymentFailed ─▶ topic: payment.events
  ▼
shipping-service (consomme payment.events, si succès)
  │ publie ShippingDispatched, ShippingDelivered ─▶ topic: shipping.events
  ▼
order-service (consomme inventory/payment/shipping.events)
  └─ met à jour le statut : CREATED → STOCK_RESERVED → PAID → SHIPPED → DELIVERED
                                    └→ REJECTED        └→ PAYMENT_FAILED
```

**Compensation** (rollback distribué) :
- `PaymentFailed` → `inventory-service` consomme et **libère le stock** (`StockReleased`).
- `StockRejected` → `order-service` passe la commande en `REJECTED`, notification envoyée.

**Consommateurs transverses** :
- `notification-service` consomme `order.events`, `payment.events`, `shipping.events` → envoie les notifications.
- `analytics-service` consomme **tous les topics** → agrégats temps réel.

## 3. Topics Kafka

| Topic | Partitions | RF | Clé | Format | Producteur |
|---|---|---|---|---|---|
| `order.events` | 6 | 2 | `orderId` | Avro | order-service |
| `inventory.events` | 6 | 2 | `orderId` | Avro | inventory-service |
| `payment.events` | 6 | 2 | `orderId` | Avro | payment-service |
| `shipping.events` | 3 | 2 | `orderId` | JSON | shipping-service |
| `*.dlt` (dead letter topics) | 1 | 2 | — | — | error handlers |

**Choix pédagogiques :**
- **Clé = `orderId`** partout → garantit l'ordre des événements d'une même commande
  (démo : que se passe-t-il sans clé ? événements désordonnés).
- **RF = 2** avec 2 brokers → démo leader/follower, ISR, panne d'un broker.
- `min.insync.replicas=2` sur `payment.events` + `acks=all` → démo du blocage producteur
  quand un broker tombe (trade-off durabilité/disponibilité).
- `shipping.events` en **JSON** vs les autres en **Avro** → comparaison taille des messages,
  évolution de schéma, erreurs de désérialisation.

## 4. Mapping notions Kafka → services

| Notion | Où c'est illustré |
|---|---|
| Producer / acks | `payment-service` : `acks=all` ; `analytics` (si produit des agrégats) : `acks=1` — comparaison latence/durabilité |
| Idempotence producteur | `enable.idempotence=true` partout ; démo des duplicats sans |
| Déduplication consommateur | `inventory-service` et `payment-service` : table `processed_events(event_id)` en DB (exactly-once applicatif) |
| Partitions & ordering | clé `orderId`, démo repartitionnement, ordre par partition uniquement |
| Replicas / leader / follower / ISR | cluster 2 brokers, `kill` d'un broker en live, élection de leader |
| Consumer groups & rebalancing | `analytics-service` scalé à 2-3 instances ; démo rebalancing, partition assignée |
| Offsets | commit manuel (`payment`), auto-commit (`analytics`), **replay** avec `seek` pour recalculer les agrégats |
| Schema Registry / Avro | order, inventory, payment ; démo évolution de schéma compatible/incompatible |
| Avro vs JSON | benchmark taille + débit entre `shipping.events` (JSON) et les autres (Avro) |
| Dead Letter Topic | poison pill dans `notification-service` → `*.dlt` après retries |
| Consumer lag / performance | injection de charge sur `order.events`, observation du lag dans Kafka UI |
| Problèmes classiques | rebalancing storm, poison pill, lag, duplicats, désérialisation, ordre inter-partitions |

## 5. REST vs Kafka

REST (synchrone, quand on a besoin d'une réponse immédiate) :
- `POST /api/orders`, `GET /api/orders/{id}` — order-service (API publique)
- `GET /api/products`, `GET /api/products/{id}` — inventory-service (catalogue, appelé par order-service pour valider les prix)
- `GET /api/analytics/dashboard` — analytics-service (consultation des agrégats)

Kafka (asynchrone) : tout le reste — la saga complète.

## 6. Java 21 & observabilité

- **Virtual threads** : `notification-service` — chaque notification (I/O simulé de 200-500 ms)
  est traitée sur un virtual thread ; démo du débit vs thread pool classique.
  Aussi : `spring.threads.virtual.enabled=true` sur les endpoints REST.
- **Correlation ID / Span ID** : Micrometer Tracing (Brave). Le correlation ID est généré
  à l'entrée (`POST /api/orders`), propagé dans les **headers HTTP** (appel REST vers inventory)
  et les **headers Kafka** à travers toute la saga. Use case : suivre une commande de bout
  en bout dans les logs des 6 services (`traceId` dans le pattern de log).
  Prépare l'arrivée de l'API Gateway (auth centralisée, rate limiting) sur Kubernetes.

## 7. Infrastructure

**Docker Compose (phase 1) :**
- 2 brokers Kafka en mode **KRaft** (pas de ZooKeeper)
- Schema Registry (Confluent)
- Kafka UI (provectus/kafka-ui) — visualisation topics, partitions, lag, consumer groups
- 1 PostgreSQL (une database par service)
- Les 6 services, chacun avec son **Dockerfile multi-stage** (build Maven → image JRE 21 minimale)

**Kubernetes (phase 2) :**
- Manifests (ou Helm) par service, Kafka via Strimzi ou manifests dédiés
- API Gateway (Spring Cloud Gateway) : authentification centralisée, rate limiting, propagation du correlation ID

## 8. Structure du projet (multi-module Maven)

```
kafka-training/
├── pom.xml                      (parent)
├── common/                      (schémas Avro partagés, DTOs, config tracing)
├── order-service/
├── inventory-service/
├── payment-service/
├── shipping-service/
├── notification-service/
├── analytics-service/
├── docker-compose.yml
├── k8s/
└── ARCHITECTURE.md
```
