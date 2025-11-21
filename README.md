# VisitorFlow-SparkScala — Pipeline de capture d’événements web (Spark) avec UI locale

Ce repo fournit un pipeline Spark Structured Streaming (Scala) pour analyser des événements web (clics, vues, scrolls): heatmap de clics en console, agrégation d’erreurs et export des parcours utilisateurs en Parquet. Il inclut une UI locale (web/sandbox.html) et un pont HTTP→Kafka pour tester sans dépendances externes côté navigateur.

## TL;DR — Démarrer en 5 étapes (navigateur → Kafka → Spark)
- Pré‑requis: Docker Desktop démarré. Première utilisation: `docker compose pull`.
1) Démarrer Kafka: `docker compose up -d`
2) Option A (un seul terminal): `sbt devUp` → démarre le pont + Spark ensemble, puis passez à l’étape 4 directement.
   Option B (deux terminaux):
   - Terminal 1: `sbt bridge` (laisse tourner)
   - Terminal 2: `sbt streamKafka`
3) Ouvrir l’UI: http://localhost:8080/ et interagir (view/click/scroll)
4) Observer: heatmap/erreurs + chemins
   - Sous Windows (mode fallback par défaut):
     - Heatmap: snapshots JSONL sous `./out/heatmap/heatmap-*.jsonl` (par bucket: pageUrl, element, xBucket, yBucket, count)
     - Chemins: snapshots JSONL sous `./out/paths/paths-*.jsonl`
   - Sous Linux/macOS ou Windows avec Spark forcé:
     - Heatmap en Parquet sous `./out/heatmap`
     - Chemins en Parquet sous `./out/paths`

Arrêt: `Ctrl+C` dans les terminaux sbt, puis `docker compose down`.

## Aliases sbt (pour éviter les invites interactives)
- `sbt genData` → génère des JSON d’exemple sous `data/events`
- `sbt streamFiles` → lance le pipeline en lecture de fichiers JSON
- `sbt bridge` → démarre le pont HTTP→Kafka (POST /event)
- `sbt streamKafka` → lance le pipeline en lecture Kafka
- `sbt devUp` → démarre le pont + UI et Spark (lecture Kafka) dans le même process (idéal « un seul terminal »)

## Notes et dépannage (UI locale & pont)
- Santé Kafka: `docker compose ps` doit montrer `kafka` en `healthy` (patientez 10–20 s).
- UI: l’UI est désormais servie par le pont sur http://localhost:8080/ (GET /). Plus besoin d’ouvrir un fichier local `web/sandbox.html`.
- CORS: le pont renvoie `Access-Control-Allow-Origin: *` et gère OPTIONS/GET/POST.
- Ports: si 8080 est pris, lancez `sbt "runMain ...HttpKafkaBridge -- --port=8090"` et ouvrez http://localhost:8090/.
- Topic: `webevents` par défaut (auto‑création activée). Gardez le même côté pont et Spark.
- Schéma: respectez les champs obligatoires `userId`, `timestamp`, `eventType` (voir ci‑dessous).
- Windows: autorisez le pare‑feu pour localhost au premier lancement.
 - Windows: si vous ne souhaitez pas installer `winutils.exe`, `sbt streamKafka` bascule automatiquement en « fallback » sans Spark (lecteur Kafka léger) qui affiche les agrégations en console. Pour forcer Spark sur Windows: ajoutez `--forceSparkOnWindows=true` à la commande.

Astuce « un seul terminal »: préférez `sbt devUp`. Sinon, si vous lancez `sbt bridge`, laissez‑le tourner dans un terminal et ouvrez un second terminal pour `sbt streamKafka`. Interrompre `sbt bridge` avant `sbt streamKafka` peut engendrer des erreurs si l’UI ne peut plus publier vers Kafka.

### Architecture amont/aval (pourquoi Kafka → Spark en aval ?)
- Amont (capture): la page web envoie des événements au pont HTTP qui les publie immédiatement dans Kafka. Cette étape est « amont » car elle réagit aux interactions utilisateur en temps réel et alimente la file d’événements.
- Aval (traitement): le job Spark Structured Streaming lit le topic Kafka et réalise les agrégations (heatmap, erreurs, parcours). C’est « en aval » par rapport à la capture — Spark ne doit pas bloquer l’UI.
- Avantage: découplage robuste via Kafka, tolérance aux pannes, rejouabilité, montée en charge et possibilité d’ajouter d’autres consommateurs.

## Format d’événement attendu (1 JSON par ligne)
{
  "userId": "u123",
  "timestamp": 1732000000000,
  "eventType": "click|view|scroll|mousemove|error",
  "pageUrl": "https://exemple.com/page",
  "referrer": "https://exemple.com/",
  "element": "#css-selector",
  "x": 123,
  "y": 456,
  "screenWidth": 1920,
  "screenHeight": 1080,
  "meta": "{}"
}

## Autres scénarios (optionnels)
- Local pur (sans Kafka): `sbt genData` puis `sbt streamFiles` (résultats identiques côté console/Parquet).

## Mode fallback Windows (sans Spark)
Sur Windows, Spark/Hadoop peut exiger `winutils.exe` pour gérer des métadonnées/checkpoints. Ce repo inclut un mode fallback automatique pour `sbt streamKafka`:
- Un petit consommateur Kafka lit le topic `webevents` et imprime toutes les 5s:
  - Top heatmap des clics (par page/élément/buckets x,y)
  - Erreurs par page
  - Un aperçu des parcours par utilisateur
- Export JSONL (sans Spark):
  - Chemins: `./out/paths/paths-YYYYMMDD-HHmmss.jsonl`
  - Heatmap: `./out/heatmap/heatmap-YYYYMMDD-HHmmss.jsonl`
- Pour forcer l’usage de Spark (si vous avez installé `winutils.exe` ou utilisez WSL2):
  `sbt "runMain test.apachesparkscala.visitorflow.Main -- --source=kafka --kafkaBrokers=localhost:9092 --kafkaTopic=webevents --outputPath=./out --checkpointPath=./chk --sessionTimeoutMinutes=2 --forceSparkOnWindows=true"`

## Conformité (rappel synthétique)
- Environnement de test fermé sans PII pour les essais locaux. Pour tout site client: cadre contractuel (DPA), CMP/consentement, minimisation des données et sécurité appropriée.

## Tests
- Un seul test unitaire est fourni (`ParseArgsSpec`) pour vérifier le parsing d’arguments; lancez `sbt test`.
