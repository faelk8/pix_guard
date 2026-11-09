# ADR 0001 — Armazenar dados em volume Docker local

- Status: aceita
- Data: 2026-11-09
- Issue: #4
- Responsáveis: projeto PixGuard AI

## Contexto

O laboratório precisa persistir os eventos e compartilhar arquivos entre a aplicação, o pipeline e o dashboard.
Nesta etapa, o objetivo é aprender e validar a arquitetura em uma única máquina, sem adicionar um serviço
separado de armazenamento de objetos.

## Decisão

Usar um volume nomeado do Docker, montado em `/data/pixguard` nos serviços que precisam acessar os dados. Não
usar MinIO nem depender de um bucket S3 nesta versão.

## Alternativas consideradas

- **MinIO/S3 local:** oferece uma API de objetos, mas adiciona serviço e operação que não são necessários para o
  fluxo inicial baseado em arquivos.
- **Diretório montado do host:** facilita inspecionar arquivos pelo host, mas acopla a execução a caminhos e
  permissões específicos da máquina.

## Consequências

- Os containers compartilham os mesmos dados e o volume sobrevive à recriação dos containers.
- O armazenamento permanece local ao host Docker; não é distribuído nem oferece alta disponibilidade.
- Backups e remoção de volumes precisam ser tratados explicitamente.

## Validação

Na FASE 1, a aplicação gravou eventos no volume e o dashboard leu o manifesto. Testes confirmaram persistência
após reinício dos serviços. O volume só é removido quando solicitado explicitamente.

## Relações

Issue #4; `docker-compose.yml`; contrato e gerador da FASE 1.
