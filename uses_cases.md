# Use cases — Scénarios fonctionnels de la formation Kafka

Chaque scénario part d'une situation métier concrète, donne les commandes pour le
reproduire, puis explique ce qui se produit techniquement dans Kafka.

**Base URL** : en local `http://localhost`, sur la VM `http://187.77.175.250`
(mêmes ports). Les commandes CLI Kafka s'exécutent dans un broker :
`docker exec kafka-1 <commande> --bootstrap-server localhost:29092 ...`
(sur la VM : `ssh root@187.77.175.250` d'abord).

Rappel des ports : order `8180`, inventory `8181`, payment `8182`, shipping `8183`,
notification `8184`, analytics `8185`, Kafka UI `18080`, Schema Registry `18081`.

---

## UC-01 — Commande nominale : la saga de bout en bout

### Fonctionnellement
Un client commande 2 claviers et 1 souris. Il paye, il est livré. Sept acteurs
travaillent sans jamais s'appeler directement : la commande, le stock, le paiement,
la livraison, les notifications et les statistiques avancent uniquement en réagissant
à des événements.

### Scénario
```bash
# Créer la commande
curl -s -X POST http://localhost:8180/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "C-42", "customerEmail": "client@example.com",
  "items": [{"productId": "P-1001", "quantity": 2}, {"productId": "P-1002", "quantity": 1}]
}'
# Récupérer <orderId> dans la réponse, puis suivre le statut :
watch -n 1 "curl -s http://localhost:8180/api/orders/<orderId> | jq .status"
```
Statuts attendus : `CREATED` → `STOCK_RESERVED` → `PAID` → `SHIPPED` → `DELIVERED`
(la livraison arrive ~15 s après l'expédition).

### Techniquement
1. `order-service` valide les produits et les prix par un **appel REST synchrone**
   vers `inventory-service` (seul moment où une réponse immédiate est nécessaire).
2. Il publie `OrderCreated` (Avro) sur `order.events` avec **clé = orderId**.
3. `inventory-service` consomme, décrémente le stock (verrou pessimiste en base),
   publie `StockReserved` sur `inventory.events`.
4. `payment-service` consomme, débite (simulation), publie `PaymentSucceeded`
   sur `payment.events`.
5. `shipping-service` consomme, publie `ShippingDispatched` puis `ShippingDelivered`
   (JSON) sur `shipping.events`.
6. `order-service` consomme **tous** ces topics et fait avancer sa state machine.
   `notification-service` et `analytics-service` consomment en parallèle : chaque
   topic est lu par **plusieurs consumer groups indépendants**, chacun avec ses
   propres offsets — c'est le pub/sub de Kafka.

### À observer
- Kafka UI → Topics : les messages sur les 4 topics, et pour un même `orderId`
  la **même partition** à chaque fois.
- Kafka UI → Consumers : 5 groups (`order-service`, `inventory-service`, `payment-service`,
  `shipping-service`, `notification-service`, `analytics-service`) avec des offsets qui avancent.
- Dashboard : `curl http://localhost:8185/api/analytics/dashboard`

---

## UC-02 — Stock insuffisant : rejet de commande

### Fonctionnellement
Un client commande 5 docks USB-C alors qu'il n'en reste que 3. La commande est
refusée et il est prévenu — mais la boutique reste disponible : le rejet est
lui-même un événement, pas une erreur HTTP.

### Scénario
```bash
curl -s http://localhost:8181/api/products/P-1006   # stock = 3
curl -s -X POST http://localhost:8180/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "C-42", "customerEmail": "client@example.com",
  "items": [{"productId": "P-1006", "quantity": 5}]
}'
```
La création répond **201** (la commande est acceptée en `CREATED`), puis passe en
`REJECTED` une seconde plus tard.

### Techniquement
La validation de stock est **asynchrone et éventuellement cohérente** : au moment
du POST, personne ne sait encore si le stock suffit. `inventory-service` consomme
`OrderCreated`, constate l'insuffisance, et publie `StockRejected` (avec la raison)
au lieu de `StockReserved`. `order-service` et `notification-service` consomment
cet événement. C'est le point clé de l'EDA : **accepter la commande d'abord,
converger ensuite** — à comparer avec une validation synchrone bloquante.

### À observer
- La raison du rejet dans la réponse de `GET /api/orders/<orderId>` (statut) et
  dans les logs de `notification-service` (« Commande annulée »).
- Sur `inventory.events` dans Kafka UI : un `StockRejected` au lieu d'un `StockReserved`.

---

## UC-03 — Échec de paiement : compensation (saga)

### Fonctionnellement
La carte du client est refusée. Problème : le stock a déjà été réservé pour lui.
Sans action corrective, 2 claviers resteraient bloqués pour une commande morte.
La réservation doit être annulée — c'est la **compensation** de la saga.

### Scénario
Le paiement échoue aléatoirement dans 15 % des cas (`payment.success-rate: 0.85`).
Pour le reproduire à coup sûr, passer le taux à 0 :
```bash
# En local : relancer payment-service avec la propriété surchargée
java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar --payment.success-rate=0
# (en Docker : ajouter PAYMENT_SUCCESSRATE=0 dans l'environnement du service et recréer)

curl -s http://localhost:8181/api/products/P-1001 | jq .stock     # noter le stock
# créer une commande de 2 x P-1001 (cf. UC-01) → statut final PAYMENT_FAILED
curl -s http://localhost:8181/api/products/P-1001 | jq .stock     # stock revenu à l'initial
```

### Techniquement
Il n'y a **pas de transaction distribuée** (pas de 2PC) entre les services : chacun
a sa base. La cohérence est rétablie par un flux d'événements inverse :
`PaymentFailed` → `inventory-service` le consomme, relit ses `reservations`
(écrites lors de la réservation précisément pour ça), ré-incrémente le stock et
publie `StockReleased`. Chaque étape est locale et transactionnelle ; c'est la
chorégraphie qui produit le rollback global. À noter : `order-service` consomme le
même `PaymentFailed` pour passer la commande en `PAYMENT_FAILED` — un même
événement déclenche plusieurs réactions indépendantes.

### À observer
- Logs `inventory-service` : « Stock libéré pour la commande … (compensation) ».
- Le `StockReleased` sur `inventory.events` dans Kafka UI.

---

## UC-04 — Panne d'un broker : réplicas, leader, follower, ISR

### Fonctionnellement
Un serveur Kafka tombe en pleine journée. Les clients continuent de commander
comme si de rien n'était : aucune perte, aucune interruption visible.

### Scénario
```bash
# Avant : chaque partition a un leader (1 ou 2) et 2 réplicas in-sync
docker exec kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic order.events

docker stop kafka-2                      # 💥 panne

docker exec kafka-1 kafka-topics --bootstrap-server localhost:29092 --describe --topic order.events
# → toutes les partitions ont maintenant Leader: 1, Isr: 1

# Créer une commande (UC-01) : la saga fonctionne toujours
docker start kafka-2                     # réparation
# Quelques secondes plus tard : Isr: 1,2 à nouveau (le follower a rattrapé)
```

### Techniquement
Chaque partition a **RF=2** : un leader qui sert lectures/écritures et un follower
qui réplique. À l'arrêt de `kafka-2`, le **contrôleur KRaft** (nœud dédié, c'est
pour ça qu'il existe dans notre compose) détecte la perte et **élit leader** le
réplica survivant pour chaque partition dont le leader était `kafka-2`. Les clients
(producteurs/consommateurs) se re-routent automatiquement via les métadonnées du
cluster. L'**ISR** (in-sync replicas) se réduit à `1` : le cluster fonctionne mais
sans filet. Au redémarrage, le follower **rattrape le log du leader** puis réintègre
l'ISR. Aucune donnée n'est perdue car tout ce qui était acquitté existait sur au
moins un réplica survivant.

### À observer
- La colonne `Isr` avant/pendant/après.
- Kafka UI → Brokers : passage de 2 à 1 broker et retour.

---

## UC-05 — acks=all + min.insync.replicas : durabilité contre disponibilité

### Fonctionnellement
Politique de l'entreprise : un encaissement ne doit **jamais** être perdu, même si
un serveur meurt dans la milliseconde qui suit. Prix à payer : si le cluster est
dégradé, on préfère **suspendre les encaissements** que risquer d'en perdre un.
Les commandes s'accumulent alors en attente, et repartent toutes seules quand le
cluster est réparé.

### Scénario
```bash
docker stop kafka-2
# Créer une commande (UC-01) → elle se fige en STOCK_RESERVED
docker logs payment-service --tail 20   # NotEnoughReplicasException
docker start kafka-2
# ~10 s plus tard la commande repart et finit DELIVERED
```

### Techniquement
Le topic `payment.events` est le seul créé avec **`min.insync.replicas=2`**, et
`payment-service` produit avec **`acks=all`** : le broker n'acquitte une écriture
que lorsqu'elle est répliquée sur 2 réplicas in-sync. Avec un seul broker vivant,
ISR=1 < 2 → le broker **refuse l'écriture** (`NotEnoughReplicasException`) ; le
producteur réessaie en boucle (retries). Le paiement est traité (ligne en base !)
mais l'événement ne part pas : la saga s'arrête là, en attente. Les autres topics
(`min.insync.replicas=1` par défaut) continuent d'accepter les écritures — d'où le
`StockReserved` passé sans problème. C'est le **triangle durabilité / disponibilité /
latence** : mêmes brokers, deux politiques différentes selon la criticité du topic.

### À observer
- Le contraste : `inventory.events` accepte, `payment.events` refuse.
- Le rattrapage automatique au redémarrage (aucune commande perdue).

---

## UC-06 — Message livré deux fois : idempotence et déduplication

### Fonctionnellement
Cauchemar du e-commerce : **débiter un client deux fois**. Kafka garantit
*at-least-once* : en cas d'incident (crash, rebalancing, timeout), un événement
déjà traité peut être relivré. Le double débit doit être impossible.

### Scénario — forcer la relivraison en rejouant les offsets
```bash
docker stop payment-service    # (ou arrêter le jar local)

# Remettre les offsets du group payment-service au début du topic
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group payment-service --topic inventory.events \
  --reset-offsets --to-earliest --execute

docker start payment-service
docker logs -f payment-service
# → "Événement ... déjà traité — le client ne sera PAS débité deux fois" pour chaque ancien message
```
Vérifier : `GET /api/orders` → aucun statut n'a changé, pas de nouveau paiement en base.

### Techniquement
Deux mécanismes distincts, souvent confondus :
1. **Idempotence du producteur** (`enable.idempotence=true`, activée partout) :
   élimine les duplicats créés par les *retries réseau* du producteur (numéro de
   séquence par partition, déduplication côté broker). Elle ne protège **pas**
   contre la relivraison côté consommateur.
2. **Déduplication applicative** (côté consommateur) : chaque événement porte un
   `eventId` UUID ; `payment-service` enregistre les `eventId` traités dans la table
   `processed_events`, **dans la même transaction** que l'écriture du paiement.
   À la relivraison, `existsById(eventId)` → l'événement est ignoré. Effet métier
   exactement-une-fois construit sur un transport au-moins-une-fois.

`payment-service` utilise en plus le **commit manuel d'offset** (`ack-mode:
manual_immediate`) : l'offset n'est commité qu'après traitement complet, donc un
crash au milieu ne perd jamais d'événement — il en relivre, et la déduplication absorbe.

---

## UC-07 — Poison pill : le message qui bloque tout, et le Dead Letter Topic

### Fonctionnellement
Une commande contient une donnée qui fait systématiquement planter l'envoi d'email.
Sans précaution, ce message serait relu et replanterait **en boucle infinie**,
bloquant toutes les notifications derrière lui (même partition). Règle métier :
après 3 échecs, on met le message de côté pour analyse humaine et on continue.

### Scénario
```bash
curl -s -X POST http://localhost:8180/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "C-666", "customerEmail": "poison@example.com",
  "items": [{"productId": "P-1002", "quantity": 1}]
}'
docker logs -f notification-service
# → 3 tentatives (IllegalStateException) espacées d'1 s, puis publication vers notification.dlt

# Vérifier ensuite qu'une commande normale (UC-01) est bien notifiée : la file n'est pas bloquée
```

### Techniquement
Le message est bien formé (il se désérialise), c'est son **traitement** qui échoue —
définition du *poison pill*. Sans stratégie d'erreur, le container Spring re-poll
le même offset indéfiniment : la partition est gelée. Le `DefaultErrorHandler` de
`notification-service` est configuré avec `FixedBackOff(1000ms, 2 retries)` + un
`DeadLetterPublishingRecoverer` : après le 3ᵉ échec, le message est **republié tel
quel sur `notification.dlt`**, l'offset est commité, et la consommation reprend au
message suivant. Le DLT conserve les headers d'origine (topic, partition, offset,
exception) pour le diagnostic. Variante du problème : le message **indésérialisable**
(ex. un JSON brut poussé sur un topic Avro), traité lui par `ErrorHandlingDeserializer`.

### À observer
- Kafka UI → topic `notification.dlt` : le message empoisonné et ses headers `kafka_dlt-*`.

---

## UC-08 — Rebalancing : ajouter un consommateur en cours de route

### Fonctionnellement
Le Black Friday approche : les statistiques prennent du retard. On ajoute une
2ᵉ instance d'`analytics-service` **sans rien arrêter** : le travail se répartit
automatiquement entre les deux.

### Scénario
```bash
# Lancer une 2e instance localement (même group-id, autre port HTTP)
java -jar analytics-service/target/analytics-service-1.0.0-SNAPSHOT.jar --server.port=8186

# Dans les logs des DEUX instances : "partitions revoked" puis "partitions assigned"
# Kafka UI → Consumers → analytics-service : les partitions sont réparties entre 2 membres

# L'arrêter (Ctrl+C) : nouveau rebalancing, l'instance restante récupère tout
```

### Techniquement
Toutes les instances partageant `group-id: analytics-service` forment un **consumer
group** ; le *group coordinator* (un broker) répartit les partitions entre membres.
À l'arrivée d'un membre : **rebalancing** — chaque partition n'est consommée que
par un seul membre du group. Avec `concurrency: 3` par instance et 6 partitions :
1 instance = 6 partitions sur 3 threads, 2 instances = ~3 partitions chacune.
**Limite structurante** : au-delà de 6 instances×threads, les surplus restent
**inactifs** — le nombre de partitions plafonne le parallélisme, d'où l'importance
de le dimensionner dès la création du topic. Problème classique associé : le
*rebalancing storm* — des consommateurs trop lents dépassent `max.poll.interval.ms`,
sont exclus du group, re-déclenchent un rebalancing, qui ralentit encore, etc.

---

## UC-09 — Replay : recalculer le passé

### Fonctionnellement
Un bug a faussé les statistiques depuis hier. Dans un système classique, les
données consommées sont perdues → il faudrait restaurer un backup. Avec Kafka :
on remet les compteurs à zéro et on **rejoue tout l'historique** — les topics
sont un journal, pas une file.

### Scénario
```bash
curl -s http://localhost:8185/api/analytics/dashboard          # état actuel
curl -s -X POST http://localhost:8185/api/analytics/replay     # purge + seek début
sleep 5
curl -s http://localhost:8185/api/analytics/dashboard          # mêmes chiffres, reconstruits
```

### Techniquement
Contrairement à une MQ traditionnelle, consommer ne détruit pas : les messages
restent dans le log (durée de rétention configurable), et chaque group ne fait
qu'avancer un **curseur (offset)** par partition. Le endpoint appelle
`seekToBeginning()` (via `AbstractConsumerSeekAware`) : au poll suivant, chaque
consumer du group repart de l'offset 0 de ses partitions et retraite tout.
C'est le fondement de l'*event sourcing* et du pattern « reconstruire une vue
matérialisée ». Conditions pour que ce soit sûr : un traitement **idempotent ou
réinitialisé** (ici : purge de la table `metrics` juste avant). À noter :
`analytics-service` est en `enable-auto-commit: true` — acceptable car des
métriques approximatives tolèrent un re-traitement, contrairement au paiement (UC-06).

---

## UC-10 — Avro vs JSON : format, taille, contrat

### Fonctionnellement
Deux équipes, deux choix : le paiement émet en Avro (contrat strict, compact),
la livraison en JSON (lisible, sans outillage). Comparons ce que ça donne sur
le même volume d'événements.

### Scénario
```bash
# Générer du trafic : une dizaine de commandes (boucle sur UC-01)
for i in $(seq 1 10); do curl -s -X POST http://localhost:8180/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C-'$i'","customerEmail":"c'$i'@ex.com","items":[{"productId":"P-1002","quantity":1}]}' > /dev/null; done

# Kafka UI → Topics → payment.events vs shipping.events → comparer la taille des messages
# Les schémas enregistrés :
curl -s http://localhost:18081/subjects | jq
curl -s "http://localhost:18081/subjects/payment.events-com.training.ecommerce.avro.PaymentSucceeded/versions/1" | jq
```

### Techniquement
- **JSON** (`shipping.events`) : chaque message embarque **les noms de champs et le
  typage implicite** — verbeux, et le « contrat » n'existe que dans le code des
  deux côtés. Un renommage de champ casse silencieusement les consommateurs.
- **Avro** (`payment.events`) : le message ne contient que les **valeurs binaires** +
  5 octets d'en-tête (magic byte + **ID du schéma** dans le Schema Registry).
  Le producteur enregistre le schéma une fois ; le consommateur le récupère par ID
  (mis en cache) pour désérialiser. Gains : taille (souvent 2-5×), vitesse, et un
  **contrat central versionné**. Coûts : dépendance d'infra (le Registry devient
  critique), outillage de build (génération de classes), débogage moins direct.
- Nos topics multi-types utilisent la stratégie `TopicRecordNameStrategy` : un
  subject par type d'événement (`payment.events-...PaymentSucceeded`, etc.) au lieu
  d'un seul subject par topic.

---

## UC-11 — Évolution de schéma : compatible ou refusée

### Fonctionnellement
Le marketing veut ajouter un libellé de devise dans l'événement de paiement.
Les 3 consommateurs de `payment.events` ne doivent **pas** avoir à être livrés en
même temps que le producteur — sinon c'est un déploiement big-bang, l'anti-pattern
que l'EDA cherche à éviter.

### Scénario
```bash
# 1. Évolution COMPATIBLE : champ avec valeur par défaut
#    Dans common/src/main/avro/PaymentSucceeded.avsc, ajouter au tableau fields :
#    { "name": "currency", "type": "string", "default": "EUR" }
mvn install -DskipTests && java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar
# → démarre, publie en v2 ; les consommateurs en v1 continuent de fonctionner

# 2. Évolution INCOMPATIBLE : retirer le champ "amount" (sans défaut) du .avsc
# → à l'enregistrement du schéma, le Schema Registry répond 409 Conflict
#   et le producteur échoue avec SerializationException. Rien n'est publié.
```

### Techniquement
Le Schema Registry applique un mode de compatibilité (défaut : `BACKWARD`) : un
nouveau schéma n'est accepté que si **les consommateurs utilisant l'ancien peuvent
lire les nouvelles données**. Ajouter un champ *avec défaut* est BACKWARD-compatible :
un lecteur v1 ignore le champ inconnu ; un lecteur v2 lisant un vieux message
comble avec le défaut. Supprimer un champ obligatoire ne l'est pas → **refus à la
source**, à la publication, plutôt qu'une explosion à la consommation des semaines
plus tard. C'est le rôle de « gardien du contrat » du Registry : l'erreur est
détectée chez celui qui casse, pas chez ceux qui subissent.

### À observer
- `curl http://localhost:18081/subjects/.../versions` : les versions successives.
- Le 409 dans les logs du producteur lors de l'évolution incompatible.

---

## UC-12 — Consumer lag : le retard qui s'accumule

### Fonctionnellement
Une opération flash génère un pic de commandes. Le paiement, qui prend ~200 ms par
opération (appel bancaire), n'absorbe plus le débit entrant : les clients voient
leur commande rester « en attente de paiement » de plus en plus longtemps.

### Scénario
```bash
# Injecter 100 commandes rapidement
for i in $(seq 1 100); do curl -s -X POST http://localhost:8180/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C-'$i'","customerEmail":"c'$i'@ex.com","items":[{"productId":"P-1001","quantity":1}]}' > /dev/null & done; wait

# Kafka UI → Consumers → payment-service : colonne "Consumer lag" qui monte puis redescend
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group payment-service
```

### Techniquement
Le **lag** = (dernier offset produit) − (dernier offset commité), par partition :
c'est LA métrique de santé d'un consommateur, celle qu'on met sous alerte en
production. Ici le producteur (`inventory-service`) écrit plus vite que
`payment-service` ne lit (200 ms/message). Le lag monte, puis se résorbe une fois
le pic passé. Leviers, dans l'ordre : paralléliser le traitement, ajouter des
instances (UC-08, plafonné par les 6 partitions), et seulement ensuite
repartitionner. Danger associé : si un traitement dépasse `max.poll.interval.ms`,
le membre est exclu du group → rebalancing → le lag empire (cf. UC-08).

---

## UC-13 — Correlation ID : suivre une commande dans 6 services

### Fonctionnellement
Un client appelle le support : « ma commande 46cc68fc… est bloquée ». Le support
doit reconstituer son parcours à travers 6 services et 4 topics — sans identifiant
commun, c'est une chasse au trésor dans 6 fichiers de logs.

### Scénario
```bash
# Créer une commande (UC-01), puis relever dans les logs d'order-service le traceId
# (3e crochet du log, ex : [6a611d47edbad548...-7af7da4d84e2c9a0])
docker logs order-service 2>&1 | grep "créée"

# Retrouver TOUT le parcours avec ce seul traceId :
for s in order inventory payment shipping notification analytics; do
  echo "=== $s ==="; docker logs ${s}-service 2>&1 | grep "6a611d47edbad548"
done
```

### Techniquement
Micrometer Tracing (pont Brave) génère un **traceId** à l'entrée HTTP (`POST
/api/orders`) et le propage automatiquement : dans les **headers HTTP** de l'appel
REST vers inventory (format W3C `traceparent`), et dans les **headers Kafka** de
chaque message produit (l'observabilité est activée sur les templates et les
listeners : `observation-enabled: true`). Chaque service consommateur restaure le
contexte et crée un **spanId** enfant ; le pattern de log Spring Boot affiche
`[traceId-spanId]` sur chaque ligne. Un seul identifiant relie donc appel REST,
4 topics et 6 services. C'est la brique sur laquelle l'API Gateway (phase 2)
s'appuiera, et il suffirait de brancher un backend (Zipkin/Tempo) pour visualiser
la cascade complète.

---

## UC-14 — Virtual threads (Java 21) : absorber 100 emails simultanés

### Fonctionnellement
Les 100 commandes du UC-12 déclenchent ~400 notifications (création, paiement,
expédition, livraison). Chaque « envoi SMTP » prend 300 ms. Avec un pool de 10
threads classiques : 400 × 300 ms / 10 = ~12 s de file d'attente. Avec un virtual
thread par envoi : tout part quasi immédiatement.

### Scénario
```bash
# Après l'injection du UC-12 :
docker logs notification-service 2>&1 | grep "thread virtuel : true" | wc -l
# → un virtual thread par email, débit non plafonné par un pool
```

### Techniquement
`NotificationSender` utilise `Executors.newVirtualThreadPerTaskExecutor()` : chaque
envoi bloquant (simulation d'I/O SMTP) s'exécute sur un **virtual thread**, démonté
de son thread porteur pendant le `sleep` — des milliers de tâches bloquantes
coûtent quelques Mo, là où 1000 threads plateforme coûteraient ~1 Go de stacks.
`spring.threads.virtual.enabled: true` fait de même pour les requêtes HTTP (Tomcat).
**Nuance importante côté Kafka** : le *listener* reste sur les threads du container
(1 par partition assignée) — c'est voulu, car l'ordre par partition et le commit
d'offset imposent un traitement séquentiel ; les virtual threads ne servent que
pour le travail I/O *décorrélé de l'offset* (ici l'envoi d'email, dont l'échec ne
doit d'ailleurs pas re-livrer le message — trade-off à discuter avec UC-07).

---

## Récapitulatif : problématique → use case

| Problématique Kafka | Use case |
|---|---|
| Producer, consumer, pub/sub multi-groups | UC-01 |
| Partitions, clé, garantie d'ordre | UC-01, UC-08 |
| Replicas, leader, follower, ISR, élection | UC-04 |
| acks, min.insync.replicas, durabilité vs dispo | UC-05 |
| Idempotence producteur | UC-06 |
| Duplication de messages, exactly-once applicatif | UC-06 |
| Commit manuel vs auto-commit des offsets | UC-06, UC-09 |
| Poison pill, Dead Letter Topic | UC-07 |
| Consumer groups, rebalancing, scaling | UC-08 |
| Offsets, replay, rétention (log vs file) | UC-09 |
| Avro vs JSON, Schema Registry | UC-10 |
| Évolution et compatibilité de schéma | UC-11 |
| Consumer lag, performances | UC-12 |
| Correlation ID / span ID | UC-13 |
| Virtual threads Java 21 | UC-14 |
| Saga, compensation, cohérence éventuelle | UC-02, UC-03 |
