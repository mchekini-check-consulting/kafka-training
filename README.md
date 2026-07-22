# E-commerce EDA — Formation Kafka

Application e-commerce à architecture événementielle (6 microservices, saga chorégraphiée)
conçue pour illustrer un maximum de notions Kafka. Voir [ARCHITECTURE.md](ARCHITECTURE.md)
pour la conception détaillée.

## Prérequis

- Java 21+, Maven 3.9+, Docker

## Démarrage rapide

```bash
# 1. Infra seule (2 brokers Kafka KRaft, Schema Registry, Kafka UI, Postgres)
docker compose up -d

# 2. Build du projet
mvn install -DskipTests

# 3a. Tout en Docker :
docker compose --profile apps up -d --build

# 3b. Ou en local pour développer (chaque service dans un terminal) :
mvn -pl inventory-service spring-boot:run
mvn -pl order-service spring-boot:run
mvn -pl payment-service spring-boot:run
mvn -pl shipping-service spring-boot:run
mvn -pl notification-service spring-boot:run
mvn -pl analytics-service spring-boot:run
```

## Tester le flux complet

```bash
# Créer une commande
curl -s -X POST http://localhost:8180/api/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "C-42",
        "customerEmail": "client@example.com",
        "items": [
          {"productId": "P-1001", "quantity": 2},
          {"productId": "P-1002", "quantity": 1}
        ]
      }'

# Suivre son statut (CREATED → STOCK_RESERVED → PAID → SHIPPED → DELIVERED)
curl -s http://localhost:8180/api/orders/<orderId>

# Catalogue
curl -s http://localhost:8181/api/products

# Dashboard analytics (et replay des offsets)
curl -s http://localhost:8185/api/analytics/dashboard
curl -s -X POST http://localhost:8185/api/analytics/replay
```

## URLs et ports

> Les ports « standards » (9092, 5432, 8080, 8081…) étant occupés par d'autres
> projets Docker sur la machine, la stack expose des ports décalés.

| Composant | URL / port hôte |
|---|---|
| Kafka UI | http://localhost:18080 |
| Schema Registry | http://localhost:18081 |
| Kafka broker 1 / 2 (depuis l'hôte) | `localhost:19092` / `localhost:19094` |
| PostgreSQL | `localhost:15432` (user/mdp : `training`) |
| order-service | http://localhost:8180 |
| inventory-service | http://localhost:8181 |
| payment-service | http://localhost:8182 |
| shipping-service | http://localhost:8183 |
| notification-service | http://localhost:8184 |
| analytics-service | http://localhost:8185 |

## Démos Kafka prévues

| Démo | Comment |
|---|---|
| Panne d'un broker, élection de leader | `docker stop kafka-2` puis `kafka-topics --describe` |
| Blocage producteur (acks=all + min.insync.replicas=2) | `docker stop kafka-2` puis créer une commande → payment-service bloque sur payment.events |
| Duplicats / idempotence consommateur | relivrer un événement, observer les logs « déjà traité » |
| Poison pill → Dead Letter Topic | commande avec un email contenant `poison` → 3 tentatives puis `notification.dlt` |
| Rebalancing des consumer groups | lancer une 2ᵉ instance d'analytics-service |
| Replay d'offsets | `POST /api/analytics/replay` |
| Avro vs JSON | comparer la taille des messages de `payment.events` (Avro) et `shipping.events` (JSON) dans Kafka UI |
| Évolution de schéma | modifier un `.avsc` et observer la compatibilité dans le Schema Registry |
| Virtual threads Java 21 | logs de notification-service (`thread virtuel : true`) |
| Correlation ID de bout en bout | suivre un `traceId` dans les logs des 6 services |
