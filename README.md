# Stone – Tier 2: Kafka Consumer -> Elasticsearch

Consumidor que lê eventos do Kafka (tópico `transactions`) e indexa no Elasticsearch no índice/alias `transactions`.

## Requisitos
- Docker e Docker Compose
- Portas livres no host (ou ajuste no docker-compose):
  - 2181 (zookeeper), 9092 (kafka), 9200 (elasticsearch)

## Subir o ambiente

```bash
docker compose up -d --build
docker compose ps
