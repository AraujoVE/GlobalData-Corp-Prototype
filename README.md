# Guia de Execução

Este guia descreve os passos para que qualquer um possa localmente testar a tarefa desenvolvida.

## Justificativas Arquiteturais (Decisões de Design)

Antes de iniciar os passos práticos, vale destacar o racional por trás de algumas decisões técnicas tomadas neste projeto:

* **Arquitetura Medallion (Bronze, Silver, Gold) em Streaming e Batch:**
  * **Por que foi escolhido:** Separar os dados em camadas (`Raw/Bronze` para auditoria e histórico bruto, `Clean/Silver` para validações de formato/esquema e regras de integridade, e `Aggregated/Gold` para regras de negócio e detecção de fraude) garante rastreabilidade, reprocessamento seguro e isolamento de falhas.
  * **Em detrimento de:** Manter uma única tabela monolítica (antipattern de *Data Swamp*), o que dificultaria a governança de dados, auditoria de falhas e aplicação de regras de mascaramento (PII).

* **Cloud Workflows + Cloud Scheduler (para o Batch) vs. Ferramentas pesadas (ex: Airflow):**
  * **Por que foi escolhido:** O Google Cloud Workflows combinado com o Cloud Scheduler é totalmente serverless, possui baixo custo, alta resiliência nativa e dispensa a manutenção de servidores ou clusters de orquestração (como o Airflow/Cloud Composer) para um pipeline orientado a arquivos parquet de menor escala.
  * **Em detrimento de:** Cloud Composer / Apache Airflow, que introduz custos fixos elevados e sobrecarga operacional desnecessária para fluxos orientados a eventos/arquivos simples baseados em etapas sequenciais no BigQuery.

* **Apache Beam / DirectRunner (para o Streaming):**
  * **Por que foi escolhido:** O Apache Beam desacopla a lógica de processamento do motor de execução (`DirectRunner` para desenvolvimento/local e `DataflowRunner` para escala em nuvem), permitindo consistência no código de streaming e resiliência com Dead Letter Queue (DLQ) para mensagens corrompidas.
  * **Em detrimento de:** Scripts Python customizados com loops infinitos consumindo Pub/Sub (`while True`), que carecem de paralelismo nativo, tratamento estruturado de janela de tempo e tolerância a falhas distribuídas.

* **Governança de Dados com Policy Tags (Dataplex / Data Catalog):**
  * **Por que foi escolhido:** Proteção nativa de colunas sensíveis (PII como `client_name_pii` e `client_document_pii`) direto no BigQuery, garantindo conformidade com LGPD/GDPR sem duplicar dados para mascaramento.

---

## Atributos e Estrutura das Tabelas no BigQuery

As tabelas do pipeline seguem o padrão da Arquitetura Medallion, divididas entre os fluxos de Streaming e Batch, além do tratamento de erros (DLQ):

1. **Camada Bronze (Dados Brutos)**
   * **Tabelas:** `streaming_transaction_raw`, `batch_transaction_raw`, `streaming_transaction_dlq`
   * **Atributos principais:** Contêm todos os campos originais da transação sem tratamento de filtragem severa, preservando dados cadastrais, informações de dispositivo, valores e metadados de ingestão.
   * **Atributos de controle:** `processed_at` (Timestamp que registra o momento da ingestão na camada).
   * **Segurança:** As colunas sensíveis de PII (`client_name_pii`, `client_document_pii`) possuem Policy Tags aplicadas via Data Catalog para mascaramento e restrição de acesso granular.

2. **Camada Silver (Dados Limpos e Validados)**
   * **Tabelas:** `streaming_transaction`, `batch_transaction`
   * **Atributos principais:** Mantém o esquema estrutural completo da transação (IDs, valores, datas, tipos de produtos, canais e dados geográficos/dispositivos).
   * **Regras aplicadas:** Somente registros que passam pelas validações de formato (regex para IDs padronizados), status válidos (`APPROVED`, `DECLINED`, `PENDING`, `FRAUD_SUSPECT`) e tipos permitidos avançam para esta camada.

3. **Camada Gold (Agregações e Regras de Risco)**
   * **Tabelas:** `streaming_transaction_suspicious`, `batch_transaction_suspicious`
   * **Atributos principais:** Focada em analítica de risco e fraude. Armazena um subconjunto enxuto e otimizado contendo `transaction_id`, `amount`, `status`, `risk_classification` (ex: `HIGH_PURCHASE`, `FRAUD_SUSPECT`) e o `processed_at`.

---

## Pré-requisitos e Passos para Execução

Os passos abaixo devem ser seguidos em ordem:

1. **Ter Docker**
   * **Motivação:** O Docker foi escolhido pois com ele é possível executar todo o processo em um ambiente controlado, sem que o avaliador ou qualquer outra pessoa precise instalar certas dependências em seu espaço pessoal do computador.

2. **`chmod +x start-dev.sh`**
   * **O porquê:** Esse comando permite que o script seja rodado e com isso todo o processo seja desencadeado.

3. **`./start-dev.sh`**
   * **O que faz:** Esse script é responsável por iniciar o Docker e dar a permissão para os outros scripts.

4. **`./setup.sh`**
   * **O processo principal:** Com esse script a lógica de definição principal do sistema ocorre. Primeiro é checado se o usuário está autenticado, caso contrário ocorre a autenticação, que requer copiar um link do terminal e, após validar, colar o link gerado pelo autenticador. Após isso, o Terraform é iniciado, definindo todas as regras necessárias. Por fim o streaming_processor.py é executado no terminal de fundo, assim é possível continuar fazendo as outras operações sem que sejam atrapalhadas.

5. **`python3 transaction_publisher.py`**
   * **Testando o streaming:** Esse script simula e publica transações de teste em tempo real para validar o fluxo de streaming do pipeline.

6. **`python3 upload_batch.py`**
   * **Testando o batch:** A lógica de batch é controlada pelo terraform, contudo para que os dados possam ser lidos é preciso que eles sejam adicionados ao local de acesso do terraform. O upload_batch.py move o arquivo batch_data.parquet, que foi gerado pelo create_batch_data.py, para o caminho ideal, e quando isso é feito ele passa a ser acessível e quando o cron do google_cloud_scheduler_job for executado, a cada 5 minutos, os dados serão movidos para as tabelas no BQ.