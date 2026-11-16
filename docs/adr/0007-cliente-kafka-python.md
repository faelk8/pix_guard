# ADR 0007 — Cliente Python para Kafka

- Status: **cliente aceito por Rafael em 2026-11-12 para a PoC**; uso produtivo sujeito às validações seguintes.
- Data: 2026-11-16
- Issue: #25
- Responsável: engenharia de streaming

## Contexto

A imagem da aplicação usa Python 3.12 em Debian Bookworm e precisa testar produção, consumo, administração de tópicos, headers e replay Kafka antes de introduzir adaptadores produtivos. O broker local é Apache Kafka 4.4.0 em KRaft single-node. A PoC roda dentro de um projeto Compose isolado.

## Alternativas consideradas

| Cliente | Características verificadas | Resultado nesta PoC |
|---|---|---|
| `confluent-kafka` | Cliente Python mantido pela Confluent, baseado em `librdkafka`; oferece Producer, Consumer e AdminClient. PyPI 2.12.2 publica wheel CPython 3.12 para manylinux x86-64 e a imagem alvo instalou a versão sem compilação local. | Instalado e executado na imagem app. Publicou e leu dez eventos `payment_event_v1`, preservou chave, headers, payload e offsets após reinício do broker; timeout de entrega foi observado sem sucesso falso. |
| `kafka-python` | Cliente comunitário em Python puro, com APIs produtor, consumidor e administração; PyPI descreve ausência de núcleo C/Cython/Rust e instalação sem dependências externas. | Não instalado nem testado nesta tarefa. |

A comparação serve para esta prova local, não é benchmark nem análise completa de manutenção, desempenho ou suporte comercial. Fontes: [visão geral oficial do cliente Python Confluent](https://docs.confluent.io/kafka-clients/python/current/overview.html), [release confluent-kafka 2.12.2 no PyPI](https://pypi.org/project/confluent-kafka/2.12.2/) e [página do kafka-python no PyPI](https://pypi.org/project/kafka-python/).

## Decisão

Fixar `confluent-kafka==2.12.2` como cliente da imagem app para a PoC e para os próximos adaptadores Kafka, sujeito à validação de compatibilidade em cada nova capacidade. A decisão favorece as APIs de administração, produtor e consumidor no mesmo pacote e a instalação comprovada com wheel CPython 3.12; não afirma superioridade geral.

## Evidências e limites

- `docker compose build app` concluiu com Python 3.12.12 e `confluent-kafka 2.12.2`; a biblioteca reportou `librdkafka 2.12.1`.
- PoC publicada na PR #26: dez eventos, tópico isolado, partição 0, offsets 0–9; replay após restart preservou conteúdo e coordenadas.
- Endpoint indisponível gerou `_MSG_TIMED_OUT` em aproximadamente dois segundos e zero entregas bem-sucedidas.
- Não foram testados lote de 100.000, ingestão produtiva, consumer groups com commits duráveis, captura de snapshots, Spark connector, throughput ou recuperação de crash entre filesystem e offset.
- A versão 2.12.2 é fixada no lockfile; upgrades requerem repetição das validações de integração.

## Consequências

A imagem app ganha uma dependência com extensão nativa e `librdkafka` empacotada. O build validado usa wheel manylinux no ambiente de referência; outras arquiteturas devem validar wheel e runtime. A API fica atrás de adaptadores quando o publicador e o capturador produtivos forem implementados.

Esta decisão não altera o contrato `payment_event_v1`, tópicos, retenção, semântica de captura ou garantias de entrega da ADR 0006.
