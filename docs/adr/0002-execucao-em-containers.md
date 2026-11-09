# ADR 0002 — Executar código e testes em containers

- Status: aceita
- Data: 2026-11-09
- Issue: #4
- Responsáveis: projeto PixGuard AI

## Contexto

O ambiente deve ser reproduzível e evitar que versões de Python, dependências ou ferramentas instaladas no host
alterem os resultados. A interface gráfica pode ser acessada pelo navegador do host, mas sua aplicação deve
executar dentro do ambiente do projeto.

## Decisão

Usar Docker Compose para iniciar Kafka, aplicação, dashboard e o serviço reservado ao pipeline. O código da
aplicação, o processamento e os testes executam dentro de containers. O navegador do host atua apenas como
cliente do dashboard local.

## Alternativas consideradas

- **Executar Python e testes diretamente no host:** simples para começar, mas depende das versões e instalações
  particulares de cada máquina.
- **Instalar todas as ferramentas manualmente no host:** aumenta o acoplamento ao ambiente e dificulta repetir
  a configuração.

## Consequências

- A configuração dos serviços, limites e conexões fica descrita no Compose e pode ser repetida.
- A execução precisa do Docker e dos recursos definidos para cada container.
- O serviço `pipeline` fica reservado para fases posteriores; sua presença no Compose não significa que Spark
  já esteja instalado ou processando dados.

## Validação

Na FASE 1, `docker compose config --quiet` validou a configuração, 100 testes passaram no container da
aplicação e o dashboard respondeu localmente.

## Relações

Issue #4; `docker-compose.yml`; Issues #1, #2 e #3 da FASE 1.
