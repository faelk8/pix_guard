# Processamento offline: da Bronze à Gold

Este guia explica como executar e conferir a FASE 3 mesmo que você nunca tenha usado Spark. Nesta etapa, Spark lê
o arquivo de eventos da FASE 1 e grava resultados locais em Parquet. A FASE 3 **não consome Kafka**, ainda não
usa dados reais e não é um serviço antifraude.

## Antes de começar

Tenha Docker Engine e Docker Compose funcionando e execute os comandos na raiz do repositório. O volume chamado
`pixguard_data` mantém arquivos entre containers. São criadas duas imagens: `app`, com o gerador e dashboard, e
`pipeline`, uma imagem separada com Spark SDP, Java e Python.

Valide a configuração e construa as imagens dentro do Docker:

```bash
docker compose config --quiet
docker compose build app
docker compose --profile pipeline build pipeline
```

O profile `pipeline` mantém Spark fora dos serviços padrão e fora da imagem leve do dashboard.

## O caminho dos dados

Uma linha JSONL é um evento JSON independente; o lote tem 100.000 linhas por padrão. A pipeline lê as linhas
com Spark, produz tabelas Parquet no volume e publica um manifesto quando a execução termina.

```{mermaid}
flowchart LR
  JSONL[events.jsonl · volume Docker] --> SDP[Spark Declarative Pipelines]
  SDP --> B[Bronze · entrada bruta]
  B --> V[Validação do contrato]
  V --> S[Silver · válidos e únicos]
  V --> Q[Quarentena · inválidos ou conflito]
  S --> G[Gold · agregados por dia e dimensões]
  G --> P[Parquet]
  SDP --> M[manifest.json + resumo top-20]
  P --> M
  M --> D[Dashboard Streamlit]
```

### Termos rápidos

* **Spark** é uma ferramenta de processamento de dados que divide operações em tarefas; aqui roda em modo local
  dentro de um container e lê o JSONL sem montar o arquivo inteiro na memória do processo controlador.
* **Spark Declarative Pipelines (SDP)** descreve os datasets e a ordem de dependência entre eles. `dry-run`
  verifica o plano; `run` o executa.
* **Parquet** é um formato de arquivo organizado por colunas, usado para consultar e analisar dados tabulares com
  eficiência.
* **Manifesto** é um pequeno arquivo JSON com o resultado e os caminhos da execução: contagens, status,
  checksum e horários.
* **`run_id`** é o identificador único de uma tentativa de processamento. Ele mantém cada saída em diretório
  próprio para permitir comparação e impedir que uma tentativa reutilize a pasta da outra.

Os nomes ajudam a entender a limpeza progressiva:

* **Bronze** conserva o JSON original e proveniência para que a entrada possa ser auditada.
* **Silver** verifica o contrato `payment_event_v1`, guarda o horário em UTC e o valor BRL como decimal exato.
  Cópias equivalentes do mesmo `event_id` viram uma linha; se um ID tiver payloads diferentes, todos os registros
  desse grupo vão para quarentena. Registros inválidos também são postos em quarentena com motivo.
* **Gold** soma BRL e conta eventos por dia UTC, comerciante sintético, canal e status. Não usa a resposta
  `ground_truth` como atributo analítico.

Essas camadas não representam três serviços: são três níveis de organização dos arquivos de dados. O pipeline
usa Spark Declarative Pipelines (SDP), uma API que declara datasets e suas dependências; `spark-pipelines
dry-run` valida o plano declarado, enquanto `spark-pipelines run` materializa as saídas.

## Executar um lote

Gere ou regenere o lote padrão de forma determinística. O arquivo fica no volume compartilhado em
`/data/pixguard/events.jsonl`:

```bash
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output /data/pixguard/events.jsonl
```

Antes de gravar as camadas, confira o plano. O `dry-run` não materializa o resultado final:

```bash
docker compose --profile pipeline run --rm pipeline pixguard-pipeline dry-run --input /data/pixguard/events.jsonl
```

Depois execute o processamento real. Use um identificador único para cada execução:

```bash
docker compose --profile pipeline run --rm pipeline pixguard-pipeline process --input /data/pixguard/events.jsonl --run-id fase3-001
```

A saída JSON da execução informa `status: completed`, as contagens, a reconciliação e os caminhos dos Parquet.
Um lote validado com seed 42 produziu:

| Camada/medida | Resultado verificado |
|---|---:|
| Linhas de entrada e Bronze | 100.000 |
| Silver único | 100.000 |
| Duplicatas equivalentes removidas | 0 |
| Linhas em quarentena | 0 |
| Grupos Gold | 3.269 |
| Reconciliação | `100000 = 100000 + 0 + 0` |

Dois `run_id` distintos produziram o mesmo Gold; uma leitura em um container novo encontrou 3.269 grupos em cada
saída e zero diferenças. O valor BRL agregado observado foi `245449133.57`. Essa soma pertence somente ao lote
sintético daquele teste; não descreve pagamentos nem fraude real.

## Conferir os arquivos e o manifesto

Cada execução ocupa um diretório próprio no volume `pixguard_data`:

```text
/data/pixguard/
├── events.jsonl
└── runs/
    └── fase3-001/
        ├── manifest.json
        ├── dashboard-summary.json
        ├── checkpoints/
        └── spark-warehouse/    # tabelas Parquet Bronze, Silver, Gold e apoio
```

O manifesto registra versão do esquema e pipeline, caminho e SHA-256 da entrada, quantidades de cada camada,
status, horários UTC, duração, caminho de cada conjunto e reconciliação. É gravado atomicamente no fim da
execução. O manifesto concluído só afirma sucesso depois das saídas e das contagens terem sido lidas e conferidas.

O `dashboard-summary.json` guarda até 20 grupos Gold para exibir uma tabela e um gráfico sem carregar o Parquet
inteiro no app. O dashboard escolhe o manifesto válido concluído com `finished_at` UTC mais recente. Uma falha
posterior não substitui esse resultado; o painel mostra um aviso e mantém o último sucesso. Execute o dashboard:

```bash
docker compose up -d --wait dashboard
```

Abra [http://localhost:8501](http://localhost:8501) e vá à seção **Processamento offline — FASE 3**. A tela
mostra `run_id`, timestamps, entrada/Bronze/Silver/duplicatas/quarentena/Gold e a amostra Gold. BRL permanece como
texto decimal; o gráfico usa contagens inteiras. O banner sintético permanece visível.

## Testar e validar

Execute os testes da pipeline na imagem Spark e os testes gerais no container leve da aplicação:

```bash
docker compose --profile pipeline run --rm pipeline pytest tests/integration/pipeline tests/unit/test_pipeline_contract.py -q
docker compose run --rm --no-deps app pytest -q
```

A fixture de integração cobre JSON válido, payload semanticamente igual com whitespace diferente, payload
divergente com o mesmo ID, JSON inválido e um evento atrasado. Os testes também exercitam caminhos e locks. A
validação executada passou com 12 testes Spark e 111 testes da app, com um teste de integração Spark ignorado na
imagem leve (onde Spark não é instalado). O lote de 100.000 linhas levou cerca de 61 segundos por execução com
limites Compose de 2 CPUs e 4 GiB para o pipeline; duração varia conforme a máquina.

## Entender fases e limitações

Kafka e Redpanda Console ainda aparecem no Compose para as experiências de mensageria da FASE 2, mas o gerador
não publica neles automaticamente. A FASE 3 abre diretamente `events.jsonl`; a FASE 2, streaming, checkpoints
de Kafka e garantias de processamento ponta a ponta permanecem pendentes. A FASE 4 sobre regras ou ML também não
está incluída aqui.

Para rastrear o trabalho: pipeline e contrato de saída foram implementados na Issue #10; a leitura visual está
na Issue #11; a proposta de warehouse por execução e o registro da regra de conflito estão na ADR 0004 / Issue #14.

Cada execução é imutável e isolada; não existe alias `latest` nem política automática de retenção. Runs ocupam
espaço no volume e devem ser removidos somente após decidir que não são mais necessários. `docker compose down`
encerra serviços, mas preserva os dados. `docker compose down -v` apaga os volumes nomeados e os arquivos neles.

## Perguntas para revisar

1. Por que Silver distingue duplicatas equivalentes de um grupo com payloads conflitantes?
2. O que o manifesto permite conferir depois que o container Spark terminou?
3. Por que os grupos Gold desta simulação não comprovam o desempenho de um sistema antifraude?
