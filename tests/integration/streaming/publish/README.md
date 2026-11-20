# Publicação incremental Kafka — STREAM-003 (#29)

`pixguard publish` valida o manifesto concluído do gerador e todo o contrato
`PaymentEvent`, SHA-256 e contagem antes do primeiro envio. Copia a origem para
`publications/<batch-id>/source.jsonl`; alterações posteriores no arquivo original
não mudam os bytes enviados. Cada execução consulta a configuração efetiva do
Kafka: uma partição, RF=1, `cleanup.policy=delete`, `retention.ms=86400000`.
O publicador nunca cria tópicos. A CLI retorna código não zero em qualquer falha.

Execute a partir da raiz do repositório, com recursos de teste próprios:

```bash
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml build app
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml up -d --wait kafka
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml run --rm --no-deps app pytest -q tests/unit/test_publish_events.py tests/integration/streaming/publish
```

As fixtures criam tópicos de nomes únicos e removem somente esses tópicos ao
terminar. Os testes incluem dez eventos reais com bytes, chaves, headers e recibos
conferidos; nova sessão/ID; política divergente; ausência de tópico; broker
inacessível; acks não observados; crash de processo; contagem/checksum/contrato
inválidos; disco indisponível simulado e origem mutável. A fixture aguarda o líder
e a metadata após criar cada tópico. O lote de 100.000 fica em STREAM-007.

Para guardar uma execução de dez eventos no volume isolado e inspecioná-la:

```bash
docker compose -p pixguard-stream003 run --rm kafka-topic-init
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml run --rm --no-deps app pixguard generate --count 10 --seed 42
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml run --rm --no-deps app pixguard publish --batch-id smoke-001 --timeout 60
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml run --rm --no-deps app python -c 'import json,pathlib; p=pathlib.Path("/data/pixguard/publications/smoke-001"); print((p/"manifest.json").read_text()); print((p/"receipts-00000000.json").read_text())'
docker compose -p pixguard-stream003 -f docker-compose.yml -f tests/integration/streaming/publish/compose.yaml down
```

O último comando preserva volumes. Use um novo batch ID para outra publicação;
IDs existentes falham sem sobrescrever artefatos. Não apague diretórios de execuções
incertas para contornar a reserva. `--input` seleciona o JSONL, `--output-root` o
namespace de publicações; mantenha o mesmo namespace para garantir exclusividade.

## Interpretação e recuperação

O manifesto registra identidade de cluster/tópico, origem, versão, timestamps,
total, `confirmed`, `uncertain` e `not_sent`. Recibos possuem ordinal zero-based,
event_id, hash dos bytes do valor e coordenada `(cluster_id, topic, partition,
offset)`. Headers são `source_batch_id`, `source_sha256`, `source_ordinal` e
`transport_version=1`. O valor conserva o JSON original sem o delimitador JSONL.

Antes de enviar cada janela de até 100 registros, o manifesto registra a janela
como incerta e passa por fsync + rename + fsync de diretório. Acks são persistidos
por janela, e só depois avançam as contagens. `completed` exige todos os recibos e
verificação final do hash da cópia privada. Crash durante uma janela deixa estado
`publishing`, nunca sucesso; a janela pode incluir registros ainda não enviados.
Um diretório reservado sem manifesto também é incompleto. Recibos à frente do
manifesto podem existir após crash, mas não autorizam considerar a execução
concluída automaticamente. Não há retomada automática nem reconciliação por consumo
nesta etapa; preserve os artefatos para investigação.

Uma falha tratada mantém acks observados como confirmados, ofertas sem ack como
incertas e registros ainda não tentados como não enviados. Uma exceção no próprio
`produce` é conservadoramente incerta. Se o armazenamento falhar, use o último
checkpoint durável; ele pode superestimar incerteza. Um ack posterior não promove
um manifesto de falha a sucesso. `acks=all` e idempotência são por sessão; uma
republicação com outro ID pode criar duplicatas. Não há exactly-once entre Kafka e
filesystem. O prazo `--timeout` limita inspeção/envio Kafka (até 3600s), não a cópia
local, validação ou fsync. Broker único PLAINTEXT é limite do laboratório.

A reserva sincroniza também a cadeia de diretórios ancestrais, do topo do
filesystem até o namespace do lote: um ancestral existente pode ter sido criado
por outro processo que ainda não fez fsync do pai. Para `--output-root` customizado,
o usuário do container precisa conseguir abrir esses diretórios para leitura e
o filesystem precisa suportar fsync de diretórios. Qualquer recusa interrompe a
reserva antes do envio; não há fallback que omita sincronização.

O namespace é resolvido para seu caminho canônico na construção do adaptador;
aliases/symlinks existentes são resolvidos antes de criar diretórios. Criação,
sincronização e reserva usam essa mesma cadeia física.
