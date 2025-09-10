import json
import os
import time
from typing import List, Dict

from kafka import KafkaConsumer, TopicPartition
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ESConnectionError

TOPIC = os.getenv("TOPIC", "transactions")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "stone-tier2-consumer")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
BULK_SIZE = int(os.getenv("BULK_SIZE", "1000"))

INDEX_NAME = "transactions"

MAPPING = {
    "mappings": {
        "properties": {
            "id":         {"type": "keyword"},
            "type":       {"type": "keyword"},
            "created_at": {"type": "date"},
            "client_id":  {"type": "keyword"},
            "payer_id":   {"type": "keyword"},
            "amount":     {"type": "double"}
        }
    }
}

def wait_for_es(es: Elasticsearch, timeout: int = 120):
    start = time.time()
    while True:
        try:
            if es.ping():
                print("[consumer] Elasticsearch disponível.")
                return
        except ESConnectionError:
            pass
        if time.time() - start > timeout:
            raise RuntimeError("Timeout esperando Elasticsearch.")
        print("[consumer] Aguardando Elasticsearch...")
        time.sleep(2)

def ensure_index(es: Elasticsearch):
    if not es.indices.exists(index=INDEX_NAME):
        print(f"[consumer] Criando índice '{INDEX_NAME}'...")
        es.indices.create(index=INDEX_NAME, **MAPPING)
    else:
        print(f"[consumer] Índice '{INDEX_NAME}' já existe.")

def make_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                group_id=GROUP_ID,
                auto_offset_reset=AUTO_OFFSET_RESET,
                enable_auto_commit=False,  # commit manual após bulk
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda v: v.decode("utf-8") if v else None,
                consumer_timeout_ms=10_000  # encerra o poll após 10s sem mensagens
            )
            print("[consumer] Conectado ao Kafka.")
            return consumer
        except Exception as e:
            print(f"[consumer] Erro conectando ao Kafka: {e}. Tentando novamente em 2s...")
            time.sleep(2)

def to_bulk_actions(messages: List[Dict]) -> List[Dict]:
    actions = []
    for msg in messages:
        doc = msg.value
        # Validação mínima de campos esperados
        if not isinstance(doc, dict):
            continue
        actions.append({
            "_index": INDEX_NAME,
            "_id": doc.get("id"),
            "_op_type": "index",
            "_source": doc
        })
    return actions

def main():
    es = Elasticsearch(ES_URL)
    wait_for_es(es)
    ensure_index(es)

    consumer = make_consumer()

    print(f"[consumer] Consumindo de '{TOPIC}' e indexando em '{INDEX_NAME}'...")
    buffer = []
    last_commit = time.time()

    try:
        while True:
            for msg in consumer.poll(timeout_ms=1000).values():
                for record in msg:
                    buffer.append(record)

                if len(buffer) >= BULK_SIZE:
                    actions = to_bulk_actions(buffer)
                    if actions:
                        helpers.bulk(es, actions, request_timeout=120)
                        consumer.commit()  # commit offsets
                        print(f"[consumer] Bulk OK ({len(actions)} docs).")
                    buffer.clear()
                    last_commit = time.time()

            # flush periódico mesmo com poucos registros
            if buffer and (time.time() - last_commit > 5):
                actions = to_bulk_actions(buffer)
                if actions:
                    helpers.bulk(es, actions, request_timeout=120)
                    consumer.commit()
                    print(f"[consumer] Bulk OK ({len(actions)} docs - flush).")
                buffer.clear()
                last_commit = time.time()

    except KeyboardInterrupt:
        print("[consumer] Encerrando por CTRL+C...")
    finally:
        try:
            if buffer:
                actions = to_bulk_actions(buffer)
                if actions:
                    helpers.bulk(es, actions, request_timeout=120)
                    consumer.commit()
                    print(f"[consumer] Bulk final OK ({len(actions)} docs).")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()
