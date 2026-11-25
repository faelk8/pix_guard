# FASE 5 — Do lote Kafka ao dashboard

Este roteiro envia um lote sintético ao Kafka, captura uma faixa de offsets e exporta os eventos como JSONL.
O Spark processa esse snapshot e o arquivo original em duas execuções. A comparação mostra se o transporte
preservou as contagens e os agregados monetários. Duas avaliações posteriores verificam se a regra produz
os mesmos resultados quando recebe a mesma Silver.

## Preparar o ambiente

Use Docker Engine e Docker Compose com suporte a perfis e a `up --wait`. A partir da raiz do repositório,
execute os blocos em ordem, no mesmo terminal Bash. Reserve disco para os logs do broker, a captura, duas
cópias da entrada e as camadas Spark. O serviço Spark usa até 4 GiB e duas CPUs. Use um **nome de projeto
Compose inédito** para começar com volumes vazios. Se a porta 8501 estiver ocupada, escolha outra porta
local livre antes de iniciar o dashboard, por exemplo `export PIXGUARD_DASHBOARD_PORT=8502`.

```bash
export COMPOSE_PROJECT_NAME=pixguard-fase5-demo-01
export PIXGUARD_KAFKA_TOPIC=pixguard.payment_event_v1
export PIXGUARD_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
batch=phase5-001
capture=phase5-001
snapshot=phase5-001
offline_run=phase5-offline-001
kafka_run=phase5-kafka-001
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

O provisionador verifica uma partição, fator de replicação 1 e retenção de 24 h. O último comando deve mostrar
`pixguard.payment_event_v1:0:0`. Se o próximo offset for diferente de zero, o intervalo deste exemplo já tem
histórico. Use outro nome de projeto com volumes novos. O volume `pixguard_data` é compartilhado entre app,
pipeline e painel. O broker mantém seu próprio volume `kafka_data`.

## Gerar, publicar e capturar

```bash
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output "$original"
docker compose run --rm --no-deps app sha256sum "$original"
docker compose run --rm --no-deps app pixguard publish --input "$original" --batch-id "$batch" --timeout 1800
docker compose run --rm --no-deps app pixguard capture --capture-id "$capture" \
  --topic "$PIXGUARD_KAFKA_TOPIC" --partition 0 --start 0 --end 100000
docker compose run --rm --no-deps app pixguard export-capture --capture-id "$capture" --snapshot-id "$snapshot"
docker compose run --rm --no-deps app sha256sum "$data/captures/$capture/records.jsonl" "$exported"
```

Confira o manifesto de cada comando antes de seguir. A publicação deve ficar `completed`, com
`confirmed=100000`, `uncertain=0` e `not_sent=0`. A captura deve ficar `completed`, com
`count=100000`. O fim é exclusivo. Os registros desta partição ocupam os offsets **0 a 99.999**.
O manifesto do snapshot deve registrar `physical=exported=100000`, reconciliação aprovada e zero
em `duplicates`, `transport_quarantine` e `contract_quarantine`.

| Artefato da execução de referência | SHA-256 observado |
|---|---|
| JSONL gerado, seed 42 | `274d6a0b1543ddd9adfb881428527c0d34ba8870cf541b4254954f90caba110e` |
| Segmento de transporte da captura `phase5-001` | `ca1b27e0931f7fcc6588b284ef3a39a829984e343afff0b81920c0ed3294670e` |
| JSONL exportado do snapshot `phase5-001` | `e3121462bdece487e8baec73d8ee034e2ca0a9ec0226c60496390390690948fb` |

Compare cada hash com o campo correspondente do seu manifesto. O segmento inclui metadados Kafka
da publicação e seu digest pode variar em outra execução. O exportador usa serialização canônica e
ordena por identificador de evento. O arquivo exportado e o original, portanto, podem ter os mesmos
eventos e SHA-256 diferentes.

## Processar e comparar Kafka e offline

```bash
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process \
  --input "$original" --run-id "$offline_run"
docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process \
  --input "$exported" --run-id "$kafka_run"
```

Cada run congela a entrada, calcula o SHA-256 e publica seu manifesto em `runs/<run_id>/manifest.json`.
O `input.sha256` do run Kafka deve corresponder a `events_sha256` do snapshot. O do run offline
deve corresponder a `source_sha256` da publicação. Na execução de referência o run Kafka levou 67,416 s.

| Resultado | Via Kafka | Offline |
|---|---:|---:|
| Bronze / Silver | 100.000 / 100.000 | 100.000 / 100.000 |
| Duplicatas removidas / quarentena | 0 / 0 | 0 / 0 |
| Grupos Gold | 3.269 | 3.269 |
| Soma BRL (Decimal) | 245449133.57 | 245449133.57 |
| Reconciliação | aprovada | aprovada |

Em um terceiro container, leia os manifestos e os Parquet Gold completos. A consulta abaixo verifica os
totais de eventos e de BRL de cada execução, conta os grupos e procura diferenças por grupo nos dois sentidos.

```bash
docker compose --profile pipeline run --rm -T --no-deps pipeline python - "$offline_run" "$kafka_run" <<'PY'
from decimal import Decimal
import json
from pathlib import Path
import sys
from pyspark.sql import SparkSession, functions as F

root = Path("/data/pixguard")
spark = SparkSession.builder.master("local[2]").appName("comparar-gold").getOrCreate()
expected = {"bronze": 100000, "silver_unique": 100000, "duplicates_removed": 0, "quarantine": 0, "gold": 3269}
tables = []
try:
    for run_id in sys.argv[1:]:
        manifest = json.loads((root / "runs" / run_id / "manifest.json").read_text())
        assert manifest["status"] == "completed" and manifest["reconciliation"]["passed"]
        assert manifest["counts"] == expected, manifest["counts"]
        gold = spark.read.parquet(str(root / manifest["outputs"]["gold"]))
        groups, events, amount = gold.agg(F.count("*"), F.sum("event_count"), F.sum("amount_total_brl")).first()
        assert (groups, events, amount) == (3269, 100000, Decimal("245449133.57"))
        print(run_id, groups, events, amount)
        tables.append(gold)
    left, right = tables
    assert not left.exceptAll(right).take(1) and not right.exceptAll(left).take(1)
    print("Gold equivalente em todos os grupos")
finally:
    spark.stop()
PY
```

## Repetir a avaliação e conferir o painel

Use o mesmo `--source-run-id` nas duas avaliações. Assim a comparação verifica a mesma Silver, o manifesto
de origem, o protocolo, as métricas e as tabelas de predições e avaliação.

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

Ambas as análises registraram 100.000 eventos, TP=5.008, FP=0, FN=0 e TN=94.992.
A comparação retornou `equivalent=true`. A regra apenas reproduziu as faixas de valores que o gerador
associou aos rótulos sintéticos. Estes resultados não medem eficácia antifraude.

O endpoint de saúde deve responder `ok`. Abra a porta local publicada pelo Compose. Nas seções
**Transporte Kafka — FASE 2**, **Processamento offline — FASE 3** e **Avaliação da regra — FASE 4**,
confira os IDs exibidos e as contagens acima. Na validação de referência o serviço estava saudável em
[127.0.0.1:8501](http://127.0.0.1:8501).

## Recuperar uma execução

| Sintoma | Verificação e ação |
|---|---|
| Tópico incompatível ou broker indisponível | Consulte `docker compose ps kafka` e `docker compose logs kafka`. Repita o provisionamento após a recuperação. A verificação sinaliza divergências de topologia e retenção. |
| Publicação interrompida | Inspecione `publications/<batch-id>/manifest.json`, sobretudo `confirmed`, `uncertain` e `not_sent`. Um novo ID representa uma republicação e pode duplicar registros no tópico. Para repetir o aceite desde o offset zero, use um projeto vazio. |
| Captura interrompida | Repita o comando de captura com o mesmo ID, tópico, partição e faixa. A retomada parte da posição durável. Se a retenção do broker já eliminou o início necessário, preserve os manifestos e refaça o ensaio em volumes novos. |
| Falha de exportação ou ID já existente | Inspecione o manifesto da captura e o checksum do segmento. Depois de corrigir a causa, escolha um novo ID de snapshot. Spark e análise também exigem novos IDs para novas execuções. |
| Dashboard sem resultados | Confirme o projeto Compose, o volume, o estado `completed` dos manifestos e os IDs exibidos. O painel usa resumos persistidos por execução. |
| Volume sem espaço ou permissão | Confira `docker compose run --rm --no-deps app pixguard check-storage` e `docker compose run --rm --no-deps app df -h /data/pixguard`. O usuário das imagens app e pipeline tem UID 10001. |

O broker local tem uma réplica. O Spark recebe um lote finito em JSONL. O teste cobre essa configuração
e esse intervalo de offsets. Garantias de alta disponibilidade, streaming contínuo e semântica
*exactly-once* de ponta a ponta ficam fora do alcance do experimento.

## Encerrar

Com o mesmo `COMPOSE_PROJECT_NAME`, execute `docker compose down` para parar os serviços e conservar os
volumes. Para repetir a demonstração do zero, escolha outro nome de projeto. Se **quiser descartar todos
os dados deste projeto de teste**, confira o nome com `docker compose ls -a` e use `docker compose down -v`.
Essa opção remove tanto os registros do Kafka quanto os snapshots, manifestos, análises e Parquet.
