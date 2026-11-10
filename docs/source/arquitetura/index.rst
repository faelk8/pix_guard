Arquitetura
===========

PixGuard AI é um laboratório para estudar dados, mensageria e processamento. A FASE 1 gera dados sintéticos em
JSONL, acompanha o manifesto do gerador e disponibiliza uma interface para explorar Kafka. A FASE 3 acrescenta
um pipeline offline que lê o JSONL com Spark Declarative Pipelines (SDP), materializa Bronze/Silver/Gold em
Parquet no volume Docker. A FASE 4 avalia uma regra fixa sobre a Silver e alimenta outra seção do dashboard.
O gerador e o broker continuam sem ligação automática; streaming Kafka é a FASE 2 planejada.

Fluxo principal
---------------

.. mermaid::

   flowchart LR
     subgraph compose[Docker Compose]
       GEN[Gerador FASE 1] --> JSONL[events.jsonl · volume]
       JSONL --> SDP[Spark SDP · FASE 3 offline]
       SDP --> B[Bronze · JSON bruto]
       B --> S[Silver · validado e único]
       S --> G[Gold · agregados Decimal]
       S --> Q[Quarentena · inválidos/conflitos]
       SDP --> M[Manifesto e resumo top-20]
       G --> PARQ[Parquet no volume]
       M --> DASH[Dashboard]
       PARQ -. consultável .-> DASH
       S --> SCORE[Regra fixa FASE 4]
       SCORE --> PRED[Predições sem rótulo]
       PRED --> JOIN[Junção posterior por event_id]
       GT[ground_truth · avaliação] --> JOIN
       JOIN --> EVAL[Métricas e manifesto FASE 4]
       EVAL --> DASH
       K[Kafka · FASE 2 planejada] --> CONSOLE[Redpanda Console]
       DEMO[Mensagem manual de demonstração] --> K
     end
       GEN -. publicação automática futura .-> K

Docker é o padrão de container: cada serviço fica isolado e tem suas dependências. Docker Compose descreve os
serviços, rede, portas e volumes. Kafka é um sistema de mensagens que permite a serviços diferentes trocarem
eventos. Redpanda Console é uma aplicação web compatível com o protocolo Kafka e torna os tópicos e mensagens
visíveis no navegador.

O arquivo ``events.jsonl`` é JSON Lines: cada linha contém um evento JSON independente. Isso permite ao Spark
ler um conjunto grande sem carregar o arquivo inteiro para a memória do processo driver. O perfil Compose
``pipeline`` executa a CLI real ``spark-pipelines``; um ``dry-run`` prepara/valida o grafo declarado e ``run``
materializa os datasets. Cada execução usa um ``run_id`` e warehouse próprios. O dashboard lê o manifesto
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
