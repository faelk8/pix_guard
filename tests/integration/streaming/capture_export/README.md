# Exportação de captura Kafka — STREAM-005

A exportação usa uma captura concluída e cria um snapshot fechado em
`/data/pixguard/captures/<capture-id>/snapshots/<snapshot-id>/`.

```bash
docker compose build app
docker compose run --rm --no-deps app \
  pixguard export-capture \
  --capture-id capture-001 \
  --snapshot-id snapshot-001

docker compose run --rm --no-deps app \
  pytest -q tests/unit/test_capture_export.py \
  tests/integration/streaming/capture_export/test_export.py
```

Os arquivos publicados são `events.jsonl`, `quarantine.jsonl` e `manifest.json`.
Cada exportação rejeita sobrescrever um snapshot existente e verifica o checksum
da captura antes de produzir conteúdo.
