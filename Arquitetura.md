```mermaid
graph TD
    %% Estilos e Cores por Camadas
    classDef source fill:#f9f,stroke:#333,stroke-width:2px;
    classDef storage fill:#bbf,stroke:#333,stroke-width:2px;
    classDef compute fill:#ff9,stroke:#333,stroke-width:2px;
    classDef bq fill:#9f9,stroke:#333,stroke-width:2px;
    classDef gov fill:#f9c,stroke:#333,stroke-width:2px;
    classDef iam fill:#ccc,stroke:#333,stroke-width:2px;

    %% Fontes de Dados (Entradas)
    subgraph Sources ["Fontes de Dados & Triggers"]
        TP[Transaction Publisher / App]:::source -->|Publica JSON| PT[Pub/Sub Topic: globaldata-corp-transactions-topic]:::storage
        CS[Cloud Scheduler]:::compute -->|A cada 5 min cron| WF[Cloud Workflows: globaldata-batch-pipeline]:::compute
        UPL[Upload Batch Script]:::source -->|Envia Parquet| GCS[GCS Bucket <br/> /batch-incoming/]:::storage
    end

    %% IAM Service Accounts
    subgraph IAM ["IAM & Seguranca"]
        SA_DEF[Compute Engine]:::iam
        SA_WF[Workflows & Scheduler Execution]:::iam
        SA_DEF -.->|Utilizada por| WF
        SA_DEF -.->|Utilizada por| SP[Apache Beam / DirectRunner]
    end

    %% Governanca (Dataplex / Data Catalog)
    subgraph Governance ["Governanca & Linhagem (Dataplex / Data Catalog)"]
        TAX[Taxonomia PII: GlobalData PII Governance Taxonomy]:::gov
        TAG[Policy Tag: Confidential PII <br/> Restringe campos sensiveis]:::gov
        TAX --- TAG
        TAG -.->|Protege colunas PII| BQ_BRONZE_TB
    end

    %% Fluxo de Streaming (Pub/Sub -> Dataflow/Direct -> BigQuery + DLQ)
    subgraph StreamingPipeline ["Fluxo de Streaming (Apache Beam / Python)"]
        PT -->|Subscription principal| SUB[Pub/Sub Subscription: <br/> globaldata-corp-transactions-subscription]:::storage
        SUB -->|Consome mensagens| SP[Streaming Processor <br/> streaming_processor.py]:::compute
        
        %% Roteamento e DLQ no Streaming
        SP -->|Falha Estrutural ou Excecao| DLQ_TOPIC[Pub/Sub Topic DLQ: <br/> globaldata-corp-transactions-dlq-topic]:::storage
        DLQ_TOPIC -->|Max delivery attempts = 5| DLQ_SUB[Pub/Sub Sub DLQ: <br/> globaldata-corp-transactions-dlq-subscription]:::storage
        DLQ_SUB -->|Consumidor DLQ| SP
    end

    %% Fluxo Batch (Workflows + GCS + BigQuery)
    subgraph BatchPipeline ["Fluxo Batch (Google Cloud Workflows)"]
        WF -->|Step 1: Carrega Parquet| BQ_STG[(BigQuery Staging: <br/> batch_transaction_raw_staging)]:::bq
        GCS -->|Le o arquivo parquet| BQ_STG
        WF -->|Step 2: Processa Lote| BQ_TRANS[Deduplica e Aplica Regras]:::compute
        WF -->|Step 4: Remove arquivo processado| GCS
    end

    %% BigQuery - Medallion Architecture (Bronze, Silver, Gold)
    subgraph BigQueryMedallion ["BigQuery - Medallion Architecture"]
        
        subgraph BronzeLayer ["Camada Bronze (Dados Brutos)"]
            BQ_BRONZE_TB[(streaming_transaction_raw)]:::bq
            BQ_BATCH_TB[(batch_transaction_raw)]:::bq
            BQ_DLQ_TB[(streaming_transaction_dlq)]:::bq
        end

        subgraph SilverLayer ["Camada Silver (Dados Limpos e Validados)"]
            BQ_SILVER_STR[(streaming_transaction)]:::bq
            BQ_SILVER_BAT[(batch_transaction)]:::bq
        end

        subgraph GoldLayer ["Camada Gold (Agregacoes e Regras de Risco)"]
            BQ_GOLD_STR[(streaming_transaction_suspicious)]:::bq
            BQ_GOLD_BAT[(batch_transaction_suspicious)]:::bq
        end

    end

    %% Conexoes do Streaming com o BigQuery
    SP -->|Writes Bronze| BQ_BRONZE_TB
    SP -->|Writes Silver| BQ_SILVER_STR
    SP -->|Writes Gold Risco| BQ_GOLD_STR
    SP -->|Writes DLQ Erros| BQ_DLQ_TB

    %% Conexoes do Batch com o BigQuery (Partindo da Staging/Processamento)
    BQ_STG -->|Alimenta com deduplicacao| BQ_BATCH_TB
    BQ_STG -->|Valida e Insere Prata| BQ_SILVER_BAT
    BQ_STG -->|Classifica Risco e Insere Ouro| BQ_GOLD_BAT
