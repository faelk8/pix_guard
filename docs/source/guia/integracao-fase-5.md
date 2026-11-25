# Integrar Kafka, Spark e dashboard — FASE 5

A FASE 5 verifica se um lote sintético preserva seus eventos e agregados ao passar pelo Kafka antes do
processamento Spark. O mesmo lote também é processado diretamente, e duas avaliações usam a mesma Silver.
Este guia explica as etapas e como interpretar suas evidências. O
{download}`runbook operacional da FASE 5 <../../runbooks/fase-5.md>` é a fonte canônica dos comandos completos,
incluindo a consulta PySpark que compara os Parquet Gold por grupo.

As decisões que orientam esta integração estão registradas nas
{download}`ADR 0006 — Kafka na FASE 2 <../../adr/0006-fase2-kafka-streaming.md>` e
{download}`ADR 0007 — cliente Kafka Python <../../adr/0007-cliente-kafka-python.md>`. O
{doc}`índice de decisões de arquitetura <../arquitetura/decisoes>` aponta para o histórico completo.

Todos os eventos e rótulos são inventados. O ensaio usa **snapshots finitos de offsets Kafka**, exportados para
**JSONL processado em lote pelo Spark**. O broker local tem uma partição e uma réplica. Os resultados não
estabelecem processamento contínuo, alta disponibilidade, semântica *exactly-once* de ponta a ponta nem eficácia
contra fraude real.

## Entender o percurso

```{mermaid}
flowchart LR
  GEN[Gerador: 100.000 eventos] --> ORIG[JSONL original]
  ORIG --> PUB[Publicação Kafka]
  PUB --> CAP[Captura: offsets de 0 a 99.999]
  CAP --> SNAP[Snapshot JSONL canônico]
  SNAP --> SPARK[Spark SDP em lote]
  ORIG --> OFF[Spark em lote: comparação offline]
  SPARK --> LAY[Bronze / Silver / Gold]
  LAY --> EVAL[Regra fixa sobre Silver]
  CAP --> DASH[Dashboard: manifestos e resumos]
  SNAP --> DASH
  LAY --> DASH
  EVAL --> DASH
```

A publicação confirma o envio ao broker; a captura registra posições de uma partição, preservando metadados de
transporte. A exportação valida os registros e produz um arquivo canônico ordenado por identificador de evento.
O Spark lê esse arquivo, congela sua entrada e materializa Bronze, Silver, quarentena e Gold. A avaliação lê a
Silver de uma execução concluída. O dashboard lê manifestos e resumos persistidos no volume, sem executar Spark.

## Preparar um ensaio isolado

Use Docker Engine e Docker Compose com perfis e suporte a `up --wait`. Execute os blocos em ordem, na raiz do
repositório e no mesmo terminal Bash. Reserve disco para o broker, captura, snapshots e três runs Spark; o
serviço Spark tem limite de 4 GiB e duas CPUs. Escolha um nome Compose **inédito**, pois este exemplo requer
volumes vazios e offsets começando em zero. Para usar outra porta local do painel, exporte
`PIXGUARD_DASHBOARD_PORT` antes de iniciá-lo.

```bash
export COMPOSE_PROJECT_NAME=pixguard-fase5-guia-01
export PIXGUARD_KAFKA_TOPIC=pixguard.payment_event_v1
export PIXGUARD_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
batch=phase5-001
capture=phase5-001
snapshot=phase5-001
offline_run=phase5-offline-001
kafka_run=phase5-kafka-001
kafka_repeat_run=phase5-kafka-002
analysis_a=phase5-analysis-001
analysis_b=phase5-analysis-002
data=/data/pixguard
original=$data/events.jsonl
exported=$data/captures/$capture/snapshots/$snapshot/events.jsonl

docker compose config --quiet
docker compose --profile pipeline build app pipeline
docker compose run --rm --no-deps app pixguard check-storage
docker compose up -d --wait kafka
docker compose run --rm --no-deps kafka-topic-init
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic "$PIXGUARD_KAFKA_TOPIC" --time latest
```

O provisionador confere uma partição, replicação 1 e retenção de 24 h. Só prossiga se o último comando mostrar
`pixguard.payment_event_v1:0:0`. Caso contrário, escolha outro projeto vazio. App, pipeline e painel compartilham
`pixguard_data`; Kafka usa `kafka_data`. Todos os caminhos `/data/pixguard` abaixo pertencem aos containers.

## Publicar o lote e congelar a faixa

```bash
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output "$original"
docker compose run --rm --no-deps app sha256sum "$original"
docker compose run --rm --no-deps app pixguard publish --input "$original" --batch-id "$batch" --timeout 1800
docker compose run --rm --no-deps app pixguard capture --capture-id "$capture" \
  --topic "$PIXGUARD_KAFKA_TOPIC" --partition 0 --start 0 --end 100000
docker compose run --rm --no-deps app pixguard export-capture --capture-id "$capture" --snapshot-id "$snapshot"
docker compose run --rm --no-deps app sha256sum "$data/captures/$capture/records.jsonl" "$exported"
```

Antes de processar, confira os manifestos: publicação `completed`, `confirmed=100000`, `uncertain=0` e
`not_sent=0`; captura `completed` e `count=100000`; snapshot com `physical=exported=100000`, reconciliação
aprovada e `duplicates=transport_quarantine=contract_quarantine=0`. O intervalo **[0, 100000)** tem fim exclusivo:
contém 100.000 posições, de 0 a 99.999.

| Artefato de referência | SHA-256 registrado no runbook |
|---|---|
| JSONL gerado com seed 42 | `274d6a0b1543ddd9adfb881428527c0d34ba8870cf541b4254954f90caba110e` |
| Segmento de transporte `phase5-001` | `ca1b27e0931f7fcc6588b284ef3a39a829984e343afff0b81920c0ed3294670e` |
| JSONL exportado `phase5-001` | `e3121462bdece487e8baec73d8ee034e2ca0a9ec0226c60496390390690948fb` |

Confira o hash de cada arquivo contra seu manifesto. O digest do segmento pode variar, pois inclui metadados
Kafka. Original e exportado podem conter os mesmos eventos com hashes diferentes: a exportação muda a ordem e
a serialização. Igualdade de bytes e equivalência dos eventos são verificações diferentes.

## Processar, repetir e comparar

```bash
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process \
  --input "$original" --run-id "$offline_run"
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process \
  --input "$exported" --run-id "$kafka_run"
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process \
  --input "$exported" --run-id "$kafka_repeat_run"
```

Cada run publica `runs/<run_id>/manifest.json` e mantém a entrada congelada. No run Kafka, `input.sha256` deve
ser igual ao `events_sha256` do snapshot; no offline, ao `source_sha256` da publicação. IDs de runs são exclusivos.

| Evidência da referência | Offline | Kafka | Reexecução Kafka |
|---|---:|---:|---:|
| Bronze / Silver | 100.000 / 100.000 | 100.000 / 100.000 | 100.000 / 100.000 |
| Duplicatas removidas / quarentena | 0 / 0 | 0 / 0 | 0 / 0 |
| Grupos Gold | 3.269 | 3.269 | 3.269 |
| Soma BRL em Decimal | 245449133.57 | 245449133.57 | 245449133.57 |
| Reconciliação | aprovada | aprovada | aprovada |

Execute também a consulta **Processar e comparar Kafka e offline** do runbook em um novo container do profile
`pipeline`, mantendo as variáveis deste terminal. Ela lê os manifestos e os Parquet completos, valida contagens
e a soma Decimal e aplica `exceptAll` nos dois sentidos. O resultado registrado foi zero diferenças por grupo.
Comparar apenas totais ou o resumo de até vinte grupos do dashboard não comprova essa equivalência completa.

## Avaliar a mesma Silver e conferir o painel

```bash
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis evaluate \
  --source-run-id "$kafka_run" --analysis-run-id "$analysis_a"
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis evaluate \
  --source-run-id "$kafka_run" --analysis-run-id "$analysis_b"
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis compare \
  --left "$analysis_a" --right "$analysis_b"
docker compose up -d --wait dashboard
docker compose ps dashboard
docker compose exec -T dashboard python -c \
  'from urllib.request import urlopen; print(urlopen("http://127.0.0.1:8501/_stcore/health", timeout=5).read().decode())'
```

O healthcheck deve responder `ok`. Abra a porta local publicada, por padrão
[127.0.0.1:8501](http://127.0.0.1:8501), e confira os IDs nas seções **Transporte Kafka — FASE 2**,
**Processamento offline — FASE 3** e **Avaliação da regra — FASE 4**.

As duas avaliações de referência tiveram 100.000 eventos, TP=5.008, FP=0, FN=0 e TN=94.992; a comparação
retornou `equivalent=true`. A regra reproduz as faixas de valores associadas pelo gerador aos rótulos sintéticos.
Consulte o [guia da FASE 4](analise-fase-4.md) para interpretar métricas e a limitação de checksum histórico da
Silver. Repetir a avaliação verifica reprodução da mesma Silver e protocolo, sem medir generalização.

## Distinguir replay e recuperação

**Replay** é ler novamente a faixa ainda retida no Kafka, usando um novo ID de captura e snapshot. Não publique
o lote outra vez: isso acrescentaria posições no tópico.

```bash
replay_capture=phase5-replay-001
replay_snapshot=phase5-replay-001
docker compose run --rm --no-deps app pixguard capture --capture-id "$replay_capture" \
  --topic "$PIXGUARD_KAFKA_TOPIC" --partition 0 --start 0 --end 100000
docker compose run --rm --no-deps app pixguard export-capture \
  --capture-id "$replay_capture" --snapshot-id "$replay_snapshot"
docker compose run --rm --no-deps app python tests/integration/streaming/acceptance/verify_batch.py \
  --source "$original" --capture "$data/captures/$replay_capture" \
  --snapshot "$data/captures/$replay_capture/snapshots/$replay_snapshot" \
  --expected-count 100000 --expected-physical 100000
```

O replay registrado retornou `status=verified`, 100.000 posições físicas, 100.000 eventos, zero duplicatas ou
quarentenas e o mesmo `events_sha256` do snapshot original (`e3121462…`). A reexecução Spark acima usa outro run
sobre o mesmo snapshot; ela verifica processamento reproduzível sem voltar a consumir Kafka.

**Recuperação** retoma uma captura interrompida com o mesmo ID, tópico, partição e faixa, a partir da posição
durável. O aceite integrado usa outro projeto Compose, broker temporário e volumes separados:

```bash
export PIXGUARD_ACCEPTANCE_PROJECT=pixguard-phase5-guia-acceptance
bash tests/integration/streaming/acceptance/run.sh
docker compose -p "$PIXGUARD_ACCEPTANCE_PROJECT" \
  -f docker-compose.yml -f tests/integration/streaming/acceptance/compose.yaml down
unset PIXGUARD_ACCEPTANCE_PROJECT
```

Na referência, passaram **8 testes**. As falhas injetadas foram antes de `fsync`, depois de `fsync`, depois do
commit Kafka e falta de espaço. Cada captura retomada terminou com 15 posições, 10 eventos exportados,
1 duplicata equivalente, 3 quarentenas de contrato e 1 de transporte. Offset expirado e broker indisponível
não produziram captura concluída. Essa evidência cobre as falhas controladas enumeradas; não simula desligamento
abrupto da máquina nem garante recuperação diante de corrupção externa. O `down` preserva os volumes do aceite;
para um ensaio novo, escolha outro nome de projeto.

## Resolver problemas e encerrar

| Sintoma | Ação |
|---|---|
| Offset inicial diferente de zero | Comece em um projeto Compose inédito, com volumes vazios. |
| Broker indisponível ou tópico incompatível | Consulte `docker compose ps kafka` e `docker compose logs kafka`; recupere o broker e repita `kafka-topic-init`. |
| Publicação interrompida | Confira `confirmed`, `uncertain` e `not_sent` em `publications/<batch-id>/manifest.json`. Um novo batch pode republicar eventos. |
| Captura interrompida | Repita os mesmos parâmetros. Se a retenção eliminou offsets necessários, preserve os manifestos e refaça o ensaio em volumes novos. |
| Exportação falhou ou ID já usado | Confira captura e checksum; corrija a causa e use novo ID de snapshot. Spark e avaliação também exigem novos IDs. |
| Painel sem resultados | Confira projeto, volume, IDs e manifestos `completed`; saúde HTTP não atesta equivalência dos dados. |
| Falta de espaço ou permissão | Use `pixguard check-storage` via app e `docker compose run --rm --no-deps app df -h /data/pixguard`; app e pipeline usam UID 10001. |

No mesmo projeto Compose, encerre os serviços conservando os dados:

```bash
docker compose down
```

Para descartar todos os artefatos do ensaio, confira primeiro o projeto com `docker compose ls -a`.
`docker compose down -v` apaga os volumes Kafka e de dados, incluindo snapshots, manifestos, Parquet e análises.
Não há limpeza automática dos runs; acompanhe o espaço disponível.

Para revisar: por que hashes diferentes podem representar o mesmo lote? Por que um replay usa nova captura,
mas uma retomada usa o mesmo ID? Que comparação detectaria diferenças Gold escondidas por totais iguais?
