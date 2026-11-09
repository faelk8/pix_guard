# ADR 0003 — Usar dados sintéticos reproduzíveis

- Status: aceita
- Data: 2026-11-09
- Issue: #4
- Responsáveis: projeto PixGuard AI

## Contexto

O PixGuard AI é um laboratório educacional. Não deve processar pagamentos reais nem expor dados pessoais, e os
exercícios precisam poder ser repetidos para comparar resultados.

## Decisão

Gerar localmente 100.000 eventos fictícios por padrão, usando a seed 42. Manter atributos de entrada separados
do rótulo `ground_truth`, que só existe porque os cenários são simulados. Não depender do Banco Central para
gerar ou testar o conjunto padrão.

## Alternativas consideradas

- **Usar transações reais:** exigiria acesso e controles para dados financeiros e pessoais, além de não ser
  necessário para validar a infraestrutura do laboratório.
- **Baixar um conjunto externo de fraude:** adicionaria dependências de disponibilidade, licença, formato e
  qualidade antes da arquitetura básica estar validada.

## Consequências

- Qualquer pessoa pode repetir a geração sem credenciais ou acesso externo.
- O conjunto ajuda a aprender contratos, processamento e qualidade, mas não representa padrões reais de fraude.
- Métricas obtidas com esses dados descrevem apenas o simulador e não podem ser apresentadas como eficácia em
  produção.

## Validação

Na FASE 1, a geração padrão produziu 100.000 eventos. Duas execuções com a mesma seed tiveram o mesmo SHA-256;
os testes confirmaram que rótulos ficam separados dos atributos e que dados pessoais não fazem parte do contrato.

## Relações

Issue #4; contrato `payment_event_v1`; Issue #2, gerador sintético da FASE 1.
