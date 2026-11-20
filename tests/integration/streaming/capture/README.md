# Captura Kafka durável — STREAM-004

O teste cria um tópico isolado, publica dez eventos sintéticos e captura os
offsets 0–10 para um diretório exclusivo. Depois repete o intervalo com outro
`capture_id` e compara os bytes, coordenadas e metadados.

```bash
docker compose -p pixguard-stream004 \
  -f docker-compose.yml \
  -f tests/integration/streaming/publish/compose.yaml \
  up -d --wait kafka

docker compose -p pixguard-stream004 \
  -f docker-compose.yml \
  -f tests/integration/streaming/publish/compose.yaml \
  run --rm --no-deps app pytest -q \
  tests/integration/streaming/capture/test_capture.py

docker compose -p pixguard-stream004 \
  -f docker-compose.yml \
  -f tests/integration/streaming/publish/compose.yaml \
  down
```

O namespace padrão de uma captura é `/data/pixguard/captures/<capture-id>`.
O `records.jsonl` contém key, value, headers, timestamp, coordenadas e checksum
SHA-256 de cada registro; `manifest.json` registra a identidade congelada,
intervalo, contagem, segmento e estado concluído.
