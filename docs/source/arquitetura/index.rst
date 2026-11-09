Arquitetura
===========

PixGuard AI é um laboratório para estudar dados, mensageria e processamento. Na FASE 1, o que está implementado
é a geração sintética em arquivo JSONL, um painel que lê o manifesto do gerador e uma interface para explorar o
Kafka. O gerador e o broker ainda não estão ligados por um produtor Kafka.

Fluxo principal
---------------

.. mermaid::

   flowchart LR
     GEN[Gerador sintético] --> JSONL[JSONL no volume Docker]
     JSONL --> DASH[Painel de progresso]
     K[Kafka no Compose] --> CONSOLE[Redpanda Console]
     DEMO[Mensagem de demonstração] --> K
     GEN -. publicação automática prevista na FASE 2 .-> K

Docker é o padrão de container: cada serviço fica isolado e tem suas dependências. Docker Compose descreve os
serviços, rede, portas e volumes. Kafka é um sistema de mensagens que permite a serviços diferentes trocarem
eventos. Redpanda Console é uma aplicação web compatível com o protocolo Kafka e torna os tópicos e mensagens
visíveis no navegador.

O arquivo ``events.jsonl`` é JSON Lines: cada linha contém um evento JSON independente. Isso permite gravar e
ler um conjunto grande progressivamente. O painel acompanha um manifesto pequeno com estado e contagens; não
carrega o arquivo inteiro para mostrar progresso.

.. toctree::
   :maxdepth: 1

   roadmap
   decisoes
