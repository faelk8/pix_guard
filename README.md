<h1 align="center">PixGuard AI</h1>

<div align="center">
  <img alt="Apache Kafka" height="72" width="80" src="https://cdn.simpleicons.org/apachekafka/231F20">
  <img alt="Docker" height="72" width="80" src="https://cdn.simpleicons.org/docker/2496ED">
  <img alt="Python" height="72" width="80" src="https://cdn.simpleicons.org/python/3776AB">
</div>

O PixGuard AI é um laboratório de engenharia de dados com pagamentos e rótulos fictícios. Com a seed 42,
o gerador produz 100.000 eventos. O publicador envia o lote ao Kafka e a captura congela uma faixa de
offsets. O exportador transforma essa faixa em um arquivo JSONL para o Spark Declarative Pipelines (SDP).
Também é possível processar diretamente o JSONL original para conferir os resultados.

## Do Kafka ao dashboard

O Spark processa cada arquivo em lote. A Bronze preserva as linhas de entrada, a Silver reúne eventos válidos
e únicos e a Gold agrega contagens e valores. Uma regra didática avalia a Silver. O dashboard permite
inspecionar a captura, o snapshot, o processamento e a avaliação usando os manifestos no volume Docker.

Na FASE 5, tanto a entrada via Kafka quanto a offline produziram 100.000 eventos Silver, 3.269 grupos Gold e
a soma de R$ 245.449.133,57. Duas avaliações sobre a mesma Silver reproduziram a classificação do simulador.
O experimento é local, trabalha com snapshots finitos e não estabelece processamento contínuo nem garantia
de entrega exatamente uma vez. As métricas descrevem exclusivamente dados sintéticos.
[Reproduzir a FASE 5 em Docker](docs/runbooks/fase-5.md).

## Processar o lote da FASE 3

Os comandos abaixo são executados a partir da raiz do repositório. Primeiro gere o lote sintético padrão e
valide o plano do Spark; depois execute o processamento real:

```bash
docker compose run --rm --no-deps app pixguard generate
docker compose --profile pipeline run --rm pipeline pixguard-pipeline dry-run
docker compose --profile pipeline run --rm pipeline pixguard-pipeline process --run-id fase3-exemplo-001
docker compose up -d --wait dashboard
```

Abra [http://localhost:8501](http://localhost:8501) e procure a seção **Processamento offline — FASE 3**.
Ela exibe as contagens Bronze/Silver/Gold, a reconciliação, os grupos Gold resumidos e um aviso de dados
sintéticos. Um lote verificado produziu 100.000 linhas Bronze, 100.000 Silver, nenhuma quarentena e 3.269
grupos Gold; duas execuções com `run_id` diferentes produziram os mesmos agregados. O valor observado descreve
somente a seed e o simulador usados no teste.

Cada `run_id` tem seu próprio diretório em `/data/pixguard/runs/` no volume Docker `pixguard_data`. O manifesto
e os Parquet ficam no volume, não no repositório. `docker compose down` mantém o volume; `docker compose down -v`
apaga o volume e os dados locais.

## Visualizar o Kafka

O Redpanda Console é uma interface web para explorar um cluster Kafka. Neste projeto, ela roda em container e
se conecta ao broker pela rede interna do Compose. Com o Docker ativo, inicie a interface:

```bash
docker compose up -d --wait redpanda-console
```

Abra [http://localhost:8080](http://localhost:8080). Em **Topics**, você pode consultar os tópicos e abrir as
mensagens de cada partição. A interface é publicada somente no endereço local da máquina.

Após publicar o lote, abra o tópico `pixguard.payment_event_v1` e selecione **Messages** para inspecionar as
mensagens da partição 0. O Console deve usar o mesmo projeto Compose da publicação.

## Testes em containers

Use um projeto Compose de testes separado da demonstração. A suíte da aplicação usa um broker Kafka local.

```bash
export COMPOSE_PROJECT_NAME=pixguard-testes
docker compose up -d --wait kafka
docker compose run --rm --no-deps app pytest -q
docker compose --profile pipeline run --rm --no-deps pipeline pytest -q tests/integration/pipeline
docker compose --profile pipeline run --rm --no-deps pipeline pytest -q tests/integration/analysis
docker compose down
unset COMPOSE_PROJECT_NAME
```

## Fases

- **FASE 0 — Arquitetura:** objetivos, limites, decisões e critérios de validação.
- **FASE 1 — Fluxo inicial:** serviços Docker, contrato de eventos, gerador sintético, painel de progresso e Redpanda Console para inspecionar Kafka.
- **FASE 2 — Streaming:** integrada. Publica o lote, captura offsets e exporta um snapshot JSONL verificável.
- **FASE 3 — Processamento offline:** integrada; processa JSONL com Spark SDP e camadas Bronze,
  Silver e Gold. Não consome Kafka.
- **FASE 4 — Análise:** integrada; baseline fixa, métricas, comparação reproduzível e painel. ML
  não foi necessário para esta demonstração e não está implementado.
- **FASE 5 — Operação:** execução ponta a ponta concluída e comparada ao caminho offline.

O planejamento e o histórico de validação estão em [docs/plan.md](docs/plan.md). As decisões de arquitetura ficam
em [docs/adr/](docs/adr/), e as notas de estudo em [docs/learning-notes.md](docs/learning-notes.md). O código é
publicado por entregas vinculadas a Issues e Pull Requests; consulte o estado de cada fase antes de usar os
serviços descritos no plano.

Para entender ou executar a análise, consulte o [guia da FASE 4](docs/source/guia/analise-fase-4.md). Os números
perfeitos observados vêm de faixas de valores separadas de propósito no gerador: eles demonstram conformidade
com o simulador, não capacidade de detectar fraude real.

## Portal de documentação

A documentação didática da arquitetura, dos comandos e dos limites de cada fase está em `docs/source/`. Construa
o portal Sphinx dentro de um container com `make docs-build`; os arquivos HTML são gerados em `docs/_build/html/`.
