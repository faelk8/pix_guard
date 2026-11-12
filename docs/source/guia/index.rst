Guia de uso
===========

Este guia começa pelos conceitos e comandos da FASE 1 e inclui um passo a passo para o processamento offline da
FASE 3. O **PixGuard AI não processa pagamentos nem é um sistema antifraude**: todos os registros e rótulos são
inventados para estudo.

Primeiros passos
----------------

Docker executa os serviços em containers. Docker Compose é a ferramenta que inicia esse conjunto; o volume
nomeado mantém os arquivos entre execuções. O requisito para acompanhar esta fase é ter Docker Engine com
Docker Compose instalado.

No terminal, na pasta raiz do projeto:

.. code-block:: bash

   docker compose config --quiet
   docker compose up -d --wait dashboard redpanda-console
   docker compose run --rm --no-deps app pixguard generate

O comando do gerador cria 100.000 eventos no padrão e os grava em ``/data/pixguard/events.jsonl`` dentro do
volume. Abra http://localhost:8501 para acompanhar o manifesto de progresso e http://localhost:8080 para explorar
o broker Kafka. O gerador atual escreve JSONL; ele não envia automaticamente esse lote ao Kafka.

.. note::

   Se quiser abrir somente o serviço de inspeção Kafka, rode ``docker compose up -d --wait redpanda-console``.

As páginas seguintes explicam o contrato, o painel, o processamento offline e a análise sintética.

.. toctree::
   :maxdepth: 1

   contrato
   dashboard
   processamento-fase-3
   analise-fase-4
   kafka-console

Solução de problemas
--------------------

* Se a porta ``8501`` ou ``8080`` estiver ocupada, ajuste ``PIXGUARD_DASHBOARD_PORT`` ou
  ``PIXGUARD_KAFKA_CONSOLE_PORT`` no arquivo ``.env`` (crie-o copiando ``.env.example``).
* Se o Console não encontrar o broker, confira ``docker compose ps`` e ``docker compose logs kafka
  redpanda-console``. Os containers conversam pelo nome de serviço ``kafka:9092``.
* Para encerrar os containers sem apagar os dados do volume, use ``docker compose down``.

Os comandos de execução e os limites da fase estão em :doc:`../referencia/comandos`.
