# Comandos e configuração

Execute estes comandos a partir da raiz do repositório. O host chama o Docker Compose; os processos da
aplicação, Kafka, dashboard e documentação executam dentro de containers.

| Ação | Comando |
|---|---|
| Validar arquivos Compose | `docker compose config --quiet` |
| Subir Kafka, aplicação e painel | `docker compose up -d --wait dashboard` |
| Subir Kafka e Redpanda Console | `docker compose up -d --wait redpanda-console` |
| Gerar lote padrão de 100 mil | `docker compose run --rm --no-deps app pixguard generate` |
| Rodar os testes | `docker compose run --rm --no-deps app pytest` |
| Construir imagem Spark SDP | `docker compose --profile pipeline build pipeline` |
| Validar o grafo SDP | `docker compose --profile pipeline run --rm pipeline pixguard-pipeline dry-run` |
| Processar com ID próprio | `docker compose --profile pipeline run --rm pipeline pixguard-pipeline process --run-id fase3-001` |
| Testar pipeline e contratos | `docker compose --profile pipeline run --rm pipeline pytest tests/integration/pipeline tests/unit/test_pipeline_contract.py -q` |
| Ver estado dos containers | `docker compose ps` |
| Consultar logs | `docker compose logs --tail=100 kafka app dashboard redpanda-console` |
| Encerrar containers, preservando volumes | `docker compose down` |

O comando `docker compose down -v` remove os volumes nomeados e os arquivos que eles contêm; não o use se
pretender preservar a geração local.

## Portas

| Serviço | Padrão | Configuração |
|---|---:|---|
| Dashboard Streamlit | 8501 | `PIXGUARD_DASHBOARD_PORT` |
| Redpanda Console | 8080 | `PIXGUARD_KAFKA_CONSOLE_PORT` |
| Kafka (rede interna) | 9092 | `PIXGUARD_KAFKA_BOOTSTRAP_SERVERS` |

Para ajustar as portas, copie `.env.example` para `.env` na raiz e altere os valores. `.env` é ignorado pelo
Git; não armazene credenciais reais nele.

## Testes no container

```bash
docker compose run --rm --no-deps app pytest
```

A suíte cobre contrato, geração determinística, armazenamento JSONL, estados do painel e encerramento dos
containers. Os testes e a aplicação usam Python 3.12 na imagem do projeto.

## Serviço da pipeline

O serviço `pipeline` está no profile Compose `pipeline`; construa-o antes da primeira execução. A CLI aceita
`dry-run` e `process`. Ambos aceitam `--input`; somente `process` aceita `--run-id`. O input
padrão é `/data/pixguard/events.jsonl`. Sem `--run-id`, o comando cria um UUID aleatório. Prefira um ID curto e
exclusivo para facilitar a inspeção, por exemplo `fase3-001`. Reutilizar um ID existente é recusado.

As camadas consultáveis ficam sob `runs/<run_id>/spark-warehouse/`; `manifest.json` e `dashboard-summary.json`
ficam no diretório da execução. O dashboard escolhe o sucesso mais recente a partir do horário UTC do manifesto;
não há alias `latest`. Cada run retém também `input-snapshot.jsonl`; todas as views usam essa cópia e
substituir a origem antes da publicação gera falha. Reexecuções e snapshots ocupam espaço adicional e não são
limpos automaticamente.

## Saída esperada da FASE 3

Com os 100.000 eventos sintéticos da seed padrão, o teste registrado resultou em Bronze 100.000, Silver 100.000,
duplicatas removidas 0, quarentena 0 e Gold 3.269. A soma BRL observada foi `245449133.57`. Esses são resultados
do simulador e da configuração do teste, não uma meta para outras seeds nem uma métrica de efetividade
antifraude.

Para encerrar os serviços sem apagar dados, execute `docker compose down`. `docker compose down -v` remove também
`pixguard_data` e `kafka_data`.

## Avaliar a regra fixa da FASE 4

Gere um lote em um arquivo novo e use identificadores exclusivos para Silver e runs de análise. A sequência não
substitui `/data/pixguard/events.jsonl`; se algum exemplo abaixo já existir no volume, use nomes livres.

```bash
docker compose --profile pipeline build pipeline
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output /data/pixguard/events-docs-fase4.jsonl
docker compose --profile pipeline run --rm pipeline pixguard-pipeline process --input /data/pixguard/events-docs-fase4.jsonl --run-id docs-fase4-silver
docker compose --profile pipeline run --rm pipeline pixguard-analysis evaluate --source-run-id docs-fase4-silver --analysis-run-id docs-fase4-eval-001
docker compose --profile pipeline run --rm pipeline pixguard-analysis evaluate --source-run-id docs-fase4-silver --analysis-run-id docs-fase4-eval-002
docker compose --profile pipeline run --rm pipeline pixguard-analysis compare --left docs-fase4-eval-001 --right docs-fase4-eval-002
```

O comando `evaluate` grava predictions sem rótulo, associa o alvo depois e publica artefatos em
`/data/pixguard/analysis-runs/<analysis_run_id>/`. A comparação lê os conjuntos completos em container novo.
No lote verificado da FASE 4, ambos continham 100.000 eventos e a comparação retornou `equivalent: true`.
Consulte o [guia da análise](../guia/analise-fase-4.md) para gerar e processar um lote, explicar a matriz e
as métricas e conhecer a limitação de checksum histórico Silver.

## Integração final — FASE 5

O [guia da FASE 5](../guia/integracao-fase-5.md) prepara um projeto Compose inédito e define todas as variáveis
dos comandos abaixo. Execute-os na raiz, no mesmo terminal e depois de preparar imagens, storage e tópico
conforme o guia. O {download}`runbook canônico <../../runbooks/fase-5.md>` contém a consulta PySpark de comparação
dos Parquet completos. A sequência usa um lote de 100.000 eventos e uma faixa finita; Spark recebe JSONL em lote.

| Etapa | Comando após a preparação do guia |
|---|---|
| Gerar lote | `docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output "$original"` |
| Publicar | `docker compose run --rm --no-deps app pixguard publish --input "$original" --batch-id "$batch" --timeout 1800` |
| Capturar offsets [0, 100000) | `docker compose run --rm --no-deps app pixguard capture --capture-id "$capture" --topic "$PIXGUARD_KAFKA_TOPIC" --partition 0 --start 0 --end 100000` |
| Exportar JSONL | `docker compose run --rm --no-deps app pixguard export-capture --capture-id "$capture" --snapshot-id "$snapshot"` |
| Processar snapshot | `docker compose --profile pipeline run --rm --no-deps pipeline pixguard-pipeline process --input "$exported" --run-id "$kafka_run"` |
| Avaliar Silver | `docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis evaluate --source-run-id "$kafka_run" --analysis-run-id "$analysis_a"` |
| Repetir avaliação da mesma Silver | `docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis evaluate --source-run-id "$kafka_run" --analysis-run-id "$analysis_b"` |
| Comparar avaliações da mesma Silver | `docker compose --profile pipeline run --rm --no-deps pipeline pixguard-analysis compare --left "$analysis_a" --right "$analysis_b"` |
| Construir portal com warnings como erros | `docker compose --profile docs run --rm --build docs` |

Publicação, captura e snapshot devem estar concluídos e reconciliados antes do Spark. Confira os hashes de
manifestos: `input.sha256` Kafka corresponde a `events_sha256` do snapshot; o offline corresponde a
`source_sha256` da publicação. Não espere hashes iguais entre original e exportado, cuja serialização e ordem
são canônicas. O segmento de transporte ainda inclui metadados Kafka que podem variar.

No ensaio registrado, offline, Kafka e reexecução tiveram Bronze/Silver 100.000, duplicatas/quarentena zero,
Gold 3.269 e soma Decimal BRL `245449133.57`. Replay exige novos IDs de captura/snapshot sem republicação;
retomada de captura interrompida exige os mesmos ID, tópico, partição e faixa. Novos runs Spark e análises
sempre usam IDs exclusivos. O guia descreve os 8 testes de recuperação controlada e seus limites.
