# ADR 0004 — Execuções locais de Spark SDP isoladas por `run_id`

- Status: proposta operacional pendente de revisão de Rafael; não aprovada
- Data: 2026-11-09
- Issue: #14
- Responsáveis: agente de arquitetura de dados

## Contexto

A FASE 1 fornece eventos sintéticos `payment_event_v1` em JSONL. A FASE 2, integração com Kafka, ainda não foi
implementada. A FASE 3 foi delimitada para processar esse JSONL offline; esta decisão não habilita Kafka,
streaming contínuo nem entrega exactly-once.

A PoC descartável da Issue #9, documentada em `specs/architecture/data-platform.md` e nos relatórios PLAN-002,
confirmou a instalação de `pyspark[pipelines]==4.3.0` com Python 3.12 e Java 17 na imagem
`python:3.12-slim-bookworm`, além de `dry-run`, execução SDP e escrita Parquet em container. A PoC comunicada
usou uma fixture sintética de duas linhas idênticas: Bronze=2, Silver=1 e Gold=1, com soma 12.34. O Gold foi
consultado em um container novo.

A repetição no mesmo warehouse local falhou com `LOCATION_ALREADY_EXISTS`, inclusive com
`--full-refresh-all`. Dois warehouses distintos, isolados por `run_id`, permitiram duas materializações e
consulta dos resultados. A evidência é limitada a essa PoC; não demonstra escala, retenção, payload conflitante,
quarentena nem lote de 100.000 eventos. Os resultados da execução foram comunicados pelo agente raiz nos
relatórios PLAN-002; não são uma nova execução feita para esta ADR.

## Decisão

Propõe-se, para implementação e revisão, que cada execução offline da FASE 3 receba um `run_id` e use um
warehouse próprio em `runs/<run_id>/warehouse`. As saídas Parquet Bronze, Silver e Gold e a quarentena ficam
sob o diretório da execução. Após sucesso, o entrypoint/orquestrador publica um manifesto com estado concluído,
versão do contrato e pipeline, referência e checksums da entrada, contagens e caminhos das saídas. Consumidores
leem somente execuções concluídas pelo caminho registrado no manifesto. Reexecuções usam outro `run_id`; não
reutilizam nem limpam o warehouse anterior como parte normal do processamento.

Como regra operacional ainda sujeita à revisão de Rafael, propõe-se colapsar duplicatas idênticas por
`event_id` e encaminhar integralmente à quarentena qualquer grupo que contenha o mesmo `event_id` com payloads
divergentes. Esta regra não altera o contrato v1 e não deve ser considerada aprovada ou implementada até revisão
e especificação contratual próprias.

O runtime candidato é Python 3.12, Java 17 e `pyspark[pipelines]==4.3.0` sobre Debian Bookworm, conforme a
PoC. Isso registra a configuração testada, não fixa imagem ou lockfile produtivo.

## Alternativas consideradas

1. **Reutilizar o mesmo warehouse e executar full refresh.** Evitaria novos diretórios por execução, mas a PoC
   falhou no mesmo local com `LOCATION_ALREADY_EXISTS`, inclusive com `--full-refresh-all`. Não é a proposta
   enquanto esse comportamento não for resolvido e reproduzivelmente validado.
2. **Um warehouse novo por `run_id`.** Isola artefatos e corresponde ao caminho que funcionou na PoC. Custa
   armazenamento adicional e exige uma política posterior de retenção e limpeza; não resolve por si só a
   concorrência na criação de identificadores nem o consumo de execuções incompletas.
3. **Catálogo persistente externo.** Poderia preservar metadados entre sessões, mas não foi testado na PoC e
   acrescentaria um serviço e operação fora do escopo local demonstrado. Fica como alternativa futura, sujeita
   a nova validação e decisão.

## Consequências

- A proposta favorece isolamento e consulta de Parquet por caminho mesmo em container novo, sem depender da
  persistência do catálogo local.
- O manifesto publicado somente após sucesso oferece aos consumidores uma referência explícita para os
  artefatos concluídos. Seu formato, escrita atômica e comportamento em falhas ainda precisam ser especificados
  na implementação.
- Cada execução ocupa espaço próprio. Retenção, remoção segura e recuperação após falha ficam pendentes e não
  devem apagar ou misturar artefatos de outra execução.
- A deduplicação idêntica e a quarentena integral de conflitos são propostas, não resultados observados. A
  fixture da PoC só verificou a primeira regra em duas linhas idênticas.
- A FASE 3 permanece processamento offline de JSONL. Kafka, streaming e garantias exactly-once pertencem a
  escopo futuro e não são consequências desta ADR.

## Validação

Antes de tratar a proposta como validada para uso, a implementação deve demonstrar em containers:

- dry-run e run com entrada JSONL e saída Parquet, incluindo consulta das camadas em container novo;
- duas execuções com `run_id` distintos, sem sobrescrever ou misturar os artefatos;
- reconciliação e manifesto após sucesso, além de comportamento verificável em falha;
- duplicatas idênticas e conflito de payload para um mesmo `event_id`, com evidência da política contratual de
  quarentena;
- lote sintético padrão de 100.000 eventos e checagens de contagem, soma decimal e saídas;
- testes de retenção/limpeza antes de automatizar remoção de execuções antigas.

Até esses testes, somente os resultados da fixture mínima descritos no contexto são evidência da PoC. Os demais
itens são validações pendentes, não fatos executados.

## Relações

- Issue #9 / PLAN-002: PoC e arquitetura de dados da plataforma.
- Issue #10: trabalho relacionado à FASE 3; consultar seu escopo na implementação.
- Issue #14 / ADR-001: registro desta proposta.
- `specs/architecture/data-platform.md`: arquitetura e limites da FASE 3.
- `specs/tasks/backlog/SPARK-001.md`: implementação dependente desta ADR.
