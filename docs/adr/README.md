<h1 align="center">Registros de decisão arquitetural</h1>

<div align="center">
  <img
    alt="Arquitetura"
    height="80"
    width="90"
    src="https://cdn.simpleicons.org/diagramsdotnet/F08705"
  >
</div>

As decisões arquiteturais do projeto são registradas aqui para preservar contexto, alternativas e consequências.

## Convenção

- Nome: `NNNN-descricao-breve.md`, com quatro dígitos e descrição em kebab-case.
- Numeração sequencial; `0000-modelo.md` é somente o modelo.
- Uma decisão aceita não é reescrita: crie uma nova ADR que a substitua.
- Relacione a Issue e os commits responsáveis pela decisão.

Copie `0000-modelo.md`, escolha o próximo número e preencha todas as seções.

## Decisões

- [0001 — Armazenamento em volume Docker](0001-armazenamento-em-volume-docker.md)
- [0002 — Execução em containers](0002-execucao-em-containers.md)
- [0003 — Dados sintéticos reproduzíveis](0003-dados-sinteticos-reproduziveis.md)
- [0004 — Execuções locais de Spark SDP isoladas por `run_id`](0004-spark-sdp-local-runs.md) — proposta pendente de revisão.
- [0005 — Baseline explicável e avaliação sintética da FASE 4](0005-fase4-analysis-baseline.md) — `proposed`, aguardando decisão de Rafael (Issue #15).
