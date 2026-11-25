Arquitetura
===========

PixGuard AI é um laboratório para estudar dados, mensageria e processamento. A FASE 1 gera dados sintéticos em
JSONL, acompanha o manifesto do gerador e disponibiliza uma interface para explorar Kafka. A FASE 3 acrescenta
um pipeline offline que lê o JSONL com Spark Declarative Pipelines (SDP), materializa Bronze/Silver/Gold em
Parquet no volume Docker. A FASE 4 avalia uma regra fixa sobre a Silver e alimenta outra seção do dashboard.
A **FASE 2 está implementada**: comandos separados publicam o lote JSONL no Kafka, capturam uma faixa finita de
offsets e exportam um snapshot JSONL. Gerar o lote não inicia a publicação automaticamente.

A FASE 5 integrou esse transporte ao Spark em lote e ao dashboard, comparando o snapshot vindo do Kafka com o
JSONL original. O caminho offline continua disponível. O :doc:`guia da FASE 5 <../guia/integracao-fase-5>`
explica a execução validada com 100.000 eventos e seus limites: não demonstra streaming contínuo, semântica
*exactly-once* de ponta a ponta nem eficácia antifraude real.

Fluxo principal
---------------

.. mermaid::

   flowchart LR
     subgraph compose[Docker Compose]
       GEN[Gerador FASE 1] --> JSONL[events.jsonl · volume]
       JSONL -->|caminho offline| SDP[Spark SDP · FASE 3 em lote]
       JSONL --> PUB[Publicador · FASE 2]
       PUB --> K[Kafka local]
       K --> CAP[Captura · faixa finita de offsets]
       CAP --> EXP[Exportador]
       EXP --> SNAP[Snapshot JSONL · volume]
       SNAP -->|integração FASE 5| SDP
       K --> CONSOLE[Redpanda Console]
       PUB --> TM[Manifestos de transporte]
       CAP --> TM
       EXP --> TM
       TM --> DASH[Dashboard]
       SDP --> B[Bronze · JSON bruto]
       B --> S[Silver · validado e único]
       S --> G[Gold · agregados Decimal]
       S --> Q[Quarentena · inválidos/conflitos]
       SDP --> M[Manifesto e resumo top-20]
       G --> PARQ[Parquet no volume]
       M --> DASH
       PARQ -. consultável .-> DASH
       S --> SCORE[Regra fixa FASE 4]
       SCORE --> PRED[Predições sem rótulo]
       PRED --> JOIN[Junção posterior por event_id]
       GT[ground_truth · avaliação] --> JOIN
       JOIN --> EVAL[Métricas e manifesto FASE 4]
       EVAL --> DASH
     end

Docker é o padrão de container: cada serviço fica isolado e tem suas dependências. Docker Compose descreve os
serviços, rede, portas e volumes. Kafka é um sistema de mensagens que permite a serviços diferentes trocarem
eventos. Redpanda Console é uma aplicação web compatível com o protocolo Kafka e torna os tópicos e mensagens
visíveis no navegador.

A publicação confirma entregas ao broker; a captura persiste registros de uma faixa de offsets com fim
exclusivo. O exportador valida essa captura e produz o snapshot JSONL para processamento. O Spark não consome
Kafka diretamente: ele recebe um arquivo finito, seja o original do gerador, seja o snapshot exportado.
Na FASE 5, os dois caminhos produziram 100.000 eventos Silver, 3.269 grupos Gold e a mesma soma Decimal de
R$ 245.449.133,57, conforme a execução documentada no :doc:`guia da integração <../guia/integracao-fase-5>`.
Esses valores são evidência do lote sintético observado, não uma garantia para outros dados.

O arquivo ``events.jsonl`` é JSON Lines: cada linha contém um evento JSON independente. Isso permite ao Spark
ler um conjunto grande sem carregar o arquivo inteiro para a memória do processo driver. O perfil Compose
``pipeline`` executa a CLI real ``spark-pipelines``; um ``dry-run`` prepara/valida o grafo declarado e ``run``
materializa os datasets. Todas as views consomem a mesma cópia da entrada, retida no run; uma alteração da
origem antes da publicação faz o processamento falhar. Cada execução usa um ``run_id`` e warehouse próprios. O dashboard lê o manifesto
concluído mais recente e um resumo limitado, sem iniciar Spark.

Bronze conserva os registros brutos; Silver valida o contrato `payment_event_v1`, guarda horários em UTC e valores
como Decimal, deduplica payloads equivalentes e envia inválidos/conflitos à quarentena; Gold agrega quantidade e
valor por dia UTC, comerciante, canal e status. Um grupo com o mesmo ``event_id`` e payload divergente vai inteiro
à quarentena, segundo decisão de Rafael. Na FASE 4, a projeção do scorer usa apenas ``amount_decimal`` como
feature; salva previsões sem rótulos e só então faz junção 1:1 por ``event_id``. O dashboard valida manifesto e
hashes sem importar Spark. Os resultados permanecem sintéticos e locais.

.. toctree::
   :maxdepth: 1

   roadmap
   decisoes
