terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# 0. Habilitacao de APIs essenciais para Governanca e Linhagem
resource "google_project_service" "datalineage_api" {
  service            = "datalineage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "dataplex_api" {
  service            = "dataplex.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "workflows_api" {
  service            = "workflows.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudscheduler_api" {
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute_api" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "pubsub_api" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "datacatalog_api" {
  service            = "datacatalog.googleapis.com"
  disable_on_destroy = false
}

# 1. Google Cloud Storage (Bucket GCS para camadas/raw e batch)
resource "google_storage_bucket" "data_bucket" {
  name                        = "${var.gcp_project_id}-storage-bucket"
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# 2. Google Cloud Pub/Sub (Topico principal, Subscription e DLQ para Resiliencia)
resource "google_pubsub_topic" "transactions_topic" {
  name = "globaldata-corp-transactions-topic"
  depends_on = [google_project_service.pubsub_api]
}

# Topico e Subscription de Dead Letter Queue (DLQ) para eventos malformados
resource "google_pubsub_topic" "transactions_dlq_topic" {
  name = "globaldata-corp-transactions-dlq-topic"
  depends_on = [google_project_service.pubsub_api]
}

resource "google_pubsub_subscription" "transactions_dlq_sub" {
  name  = "globaldata-corp-transactions-dlq-subscription"
  topic = google_pubsub_topic.transactions_dlq_topic.name
}

resource "google_pubsub_subscription" "transactions_sub" {
  name  = "globaldata-corp-transactions-subscription"
  topic = google_pubsub_topic.transactions_topic.name

  ack_deadline_seconds = 20

  # Configuracao de Dead Letter Policy para resiliencia do pipeline de streaming
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.transactions_dlq_topic.id
    max_delivery_attempts = 5
  }
}

# 3. BigQuery Datasets (Medallion Architecture)
resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id                 = "globaldata_bronze"
  friendly_name              = "Bronze Layer - Raw Data"
  description                = "Receives all raw JSON messages from streaming and batch."
  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id                 = "globaldata_silver"
  friendly_name              = "Silver Layer - Cleaned Data"
  description                = "Receives validated, clean records matching correct schemas and rules."
  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id                 = "globaldata_gold"
  friendly_name              = "Gold Layer - Aggregated Data"
  description                = "Receives domain and business specific data."
  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

# 4. Governanca: Taxonomia e Policy Tags para Protecao de Dados PII
resource "google_data_catalog_taxonomy" "pii_taxonomy" {
  region                 = var.gcp_region
  display_name           = "GlobalData PII Governance Taxonomy"
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
  depends_on = [google_project_service.datacatalog_api]
}

resource "google_data_catalog_policy_tag" "confidential_pii_tag" {
  taxonomy     = google_data_catalog_taxonomy.pii_taxonomy.id
  display_name = "Confidential PII"
  description  = "Restricts access to sensitive client PII fields"
}

resource "google_data_catalog_policy_tag_iam_member" "examiner_pii_access" {
  count      = var.examiner_email != "" ? 1 : 0
  policy_tag = google_data_catalog_policy_tag.confidential_pii_tag.name
  role       = "roles/datacatalog.categoryViewer"
  member     = "user:${var.examiner_email}"
}

resource "google_data_catalog_policy_tag_iam_member" "pii_reader" {
  policy_tag = google_data_catalog_policy_tag.confidential_pii_tag.id
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# 5. BigQuery Tables Schema Definition (Com Particionamento, Clustering e Policy Tags)
resource "google_bigquery_table" "batch_bronze_table" {
  dataset_id          = google_bigquery_dataset.bronze_dataset.dataset_id
  table_id            = "batch_transaction_raw"
  deletion_protection = false

  # Otimizacao: Particionamento por data de processamento
  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  # Otimizacao: Clustering por ID de cliente e tipo de transacao
  clustering = ["client_id", "transaction_type"]

  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED" },
    { name = "transaction_timestamp", type = "STRING", mode = "NULLABLE" },
    { name = "transaction_type", type = "STRING", mode = "NULLABLE" },
    { name = "transaction_partner_id", type = "STRING", mode = "NULLABLE" },
    { name = "client_company_id", type = "STRING", mode = "NULLABLE" },
    { name = "client_id", type = "STRING", mode = "NULLABLE" },
    { name = "client_company_account_id", type = "STRING", mode = "NULLABLE" },
    { name = "client_account_id", type = "STRING", mode = "NULLABLE" },
    { 
      name = "client_name_pii", 
      type = "STRING", 
      mode = "NULLABLE",
      "policyTags": {
        "names": [google_data_catalog_policy_tag.confidential_pii_tag.id]
      }
    },
    { name = "client_country_pii", type = "STRING", mode = "NULLABLE" },
    { 
      name = "client_document_pii", 
      type = "STRING", 
      mode = "NULLABLE",
      "policyTags": {
        "names": [google_data_catalog_policy_tag.confidential_pii_tag.id]
      }
    },
    { name = "company_id", type = "STRING", mode = "NULLABLE" },
    { name = "company_name", type = "STRING", mode = "NULLABLE" },
    { name = "company_country", type = "STRING", mode = "NULLABLE" },
    { name = "company_type", type = "STRING", mode = "NULLABLE" },
    { name = "product_type", type = "STRING", mode = "NULLABLE" },
    { name = "amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "currency", type = "STRING", mode = "NULLABLE" },
    { name = "device_id", type = "STRING", mode = "NULLABLE" },
    { name = "device_os", type = "STRING", mode = "NULLABLE" },
    { name = "device_ip", type = "STRING", mode = "NULLABLE" },
    { name = "status", type = "STRING", mode = "NULLABLE" },
    { name = "processed_at", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "streaming_bronze_table" {
  dataset_id          = google_bigquery_dataset.bronze_dataset.dataset_id
  table_id            = "streaming_transaction_raw"
  deletion_protection = false

  schema = google_bigquery_table.batch_bronze_table.schema
}

resource "google_bigquery_table" "batch_staging_table" {
  dataset_id          = google_bigquery_dataset.bronze_dataset.dataset_id
  table_id            = "batch_transaction_raw_staging"
  deletion_protection = false

  schema = google_bigquery_table.batch_bronze_table.schema
}

resource "google_bigquery_table" "streaming_dlq_table" {
  dataset_id          = google_bigquery_dataset.bronze_dataset.dataset_id
  table_id            = "streaming_transaction_dlq"
  deletion_protection = false

  schema = jsonencode([
    { name = "raw_message", type = "STRING", mode = "NULLABLE" },
    { name = "error_reason", type = "STRING", mode = "NULLABLE" },
    { name = "failed_at", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "batch_silver_table" {
  dataset_id          = google_bigquery_dataset.silver_dataset.dataset_id
  table_id            = "batch_transaction"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["client_id", "status"]

  schema = google_bigquery_table.batch_bronze_table.schema
}

resource "google_bigquery_table" "streaming_silver_table" {
  dataset_id          = google_bigquery_dataset.silver_dataset.dataset_id
  table_id            = "streaming_transaction"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["client_id", "status"]

  schema = google_bigquery_table.streaming_bronze_table.schema
}

resource "google_bigquery_table" "batch_gold_table" {
  dataset_id          = google_bigquery_dataset.gold_dataset.dataset_id
  table_id            = "batch_transaction_suspicious"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["risk_classification"]

  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED" },
    { name = "amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "status", type = "STRING", mode = "NULLABLE" },
    { name = "risk_classification", type = "STRING", mode = "NULLABLE" },
    { name = "processed_at", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "streaming_gold_table" {
  dataset_id          = google_bigquery_dataset.gold_dataset.dataset_id
  table_id            = "streaming_transaction_suspicious"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["risk_classification"]

  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED" },
    { name = "amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "status", type = "STRING", mode = "NULLABLE" },
    { name = "risk_classification", type = "STRING", mode = "NULLABLE" },
    { name = "processed_at", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

# 6. Orquestracao Batch (Cloud Workflows + Cloud Scheduler)
resource "google_workflows_workflow" "batch_orchestration" {
  name        = "globaldata-batch-pipeline"
  region      = var.gcp_region
  description = "Orquestra a carga batch usando tabelas temporarias efemeras do BigQuery"
  depends_on = [google_project_service.workflows_api]

  source_contents = <<-EOF
    main:
      steps:
        - logStart:
            call: sys.log
            args:
              text: "Iniciando pipeline batch com tabelas temporarias efemeras..."

        - step1_loadGcsToTempStaging:
            try:
              call: googleapis.bigquery.v2.jobs.insert
              args:
                projectId: ${var.gcp_project_id}
                body:
                  configuration:
                    load:
                      sourceUris: ["gs://${var.gcp_project_id}-storage-bucket/batch-incoming/transactions_batch.parquet"]
                      destinationTable:
                        projectId: ${var.gcp_project_id}
                        datasetId: "globaldata_bronze"
                        tableId: "batch_transaction_raw_staging"
                      sourceFormat: "PARQUET"
                      writeDisposition: "WRITE_TRUNCATE"
            except:
              as: e
              steps:
                - checkLoadError:
                    switch:
                      - condition: '$${e.code == 404}'
                        call: sys.log
                        args:
                          text: "Nenhum arquivo batch encontrado no GCS. Prosseguindo ciclo..."
                      - condition: '$${true}'
                        raise: $${e}

        - step2_processAndAppendBronze:
            call: googleapis.bigquery.v2.jobs.insert
            args:
              projectId: ${var.gcp_project_id}
              body:
                configuration:
                  jobType: "QUERY"
                  query:
                    query: |
                      INSERT INTO `${var.gcp_project_id}.globaldata_bronze.batch_transaction_raw`
                      SELECT * EXCEPT(rn, processed_at_temp), CURRENT_TIMESTAMP() AS processed_at FROM (
                        SELECT *, 
                            CURRENT_TIMESTAMP() AS processed_at_temp,
                            ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_timestamp DESC) as rn
                        FROM `${var.gcp_project_id}.globaldata_bronze.batch_transaction_raw_staging`
                      ) WHERE rn = 1;
                    useLegacySql: false

        - step2_processAndAppendSilver:
            call: googleapis.bigquery.v2.jobs.insert
            args:
              projectId: ${var.gcp_project_id}
              body:
                configuration:
                  jobType: "QUERY"
                  query:
                    query: |
                      INSERT INTO `${var.gcp_project_id}.globaldata_silver.batch_transaction`
                      SELECT 
                        transaction_id, transaction_timestamp, transaction_type, transaction_partner_id,
                        client_company_id, client_id, client_company_account_id, client_account_id,
                        client_name_pii, client_country_pii, client_document_pii,
                        company_id, company_name, company_country, company_type,
                        product_type, amount, currency, device_id, device_os, device_ip, status,
                        CURRENT_TIMESTAMP() AS processed_at
                      FROM (
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_timestamp DESC) as rn
                        FROM `${var.gcp_project_id}.globaldata_bronze.batch_transaction_raw_staging`
                      )
                      WHERE rn = 1
                        AND status IN ('APPROVED', 'DECLINED', 'PENDING', 'FRAUD_SUSPECT')
                        AND transaction_type IN ('PURCHASE', 'REFUND', 'TRANSFER', 'SUBSCRIPTION')
                        AND product_type IN ('DIGITAL_GOODS', 'PHYSICAL_GOODS', 'SERVICE', 'SOFTWARE', 'OTHERS')
                        AND device_os IN ('Android', 'iOS', 'Windows', 'MacOS', 'Linux', 'Other')
                        AND REGEXP_CONTAINS(transaction_partner_id, r'^cli_\\d{10}$')
                        AND REGEXP_CONTAINS(client_id, r'^cli_\\d{10}$')
                        AND REGEXP_CONTAINS(client_account_id, r'^acc_cli_\\d{10}$')
                        AND REGEXP_CONTAINS(company_id, r'^comp_\\d{5}$')
                        AND REGEXP_CONTAINS(device_id, r'^dev_\\d+$');
                    useLegacySql: false

        - step2_processAndAppendGold:
            call: googleapis.bigquery.v2.jobs.insert
            args:
              projectId: ${var.gcp_project_id}
              body:
                configuration:
                  jobType: "QUERY"
                  query:
                    query: |
                      INSERT INTO `${var.gcp_project_id}.globaldata_gold.batch_transaction_suspicious`
                      SELECT 
                        transaction_id, 
                        amount, 
                        status, 
                        CASE 
                          WHEN amount > 4000.0 THEN 'HIGH_PURCHASE'
                          WHEN status = 'FRAUD_SUSPECT' THEN 'FRAUD_SUSPECT'
                          ELSE 'NORMAL'
                        END AS risk_classification,
                        CURRENT_TIMESTAMP() AS processed_at
                      FROM `${var.gcp_project_id}.globaldata_silver.batch_transaction`
                      WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
                        AND (amount > 4000.0 OR status = 'FRAUD_SUSPECT');
                    useLegacySql: false

        - step3_clearMainStaging:
            call: googleapis.bigquery.v2.jobs.insert
            args:
              projectId: ${var.gcp_project_id}
              body:
                configuration:
                  query:
                    query: |
                      TRUNCATE TABLE `${var.gcp_project_id}.globaldata_bronze.batch_transaction_raw_staging`;
                    useLegacySql: false

        - step4_deleteProcessedFile:
            try:
              call: googleapis.storage.v1.objects.delete
              args:
                bucket: "${var.gcp_project_id}-storage-bucket"
                object: "batch-incoming/transactions_batch.parquet"
            except:
              as: e
              steps:
                - checkNotFound:
                    switch:
                      - condition: '$${e.code == 404}'
                        call: sys.log
                        args:
                          text: "Arquivo já havia sido removido ou não encontrado no GCS. Prosseguindo..."
                      - condition: '$${true}'
                        raise: $${e}

        - logSuccess:
            call: sys.log
            args:
              text: "Pipeline batch concluido com sucesso usando tabelas temporarias!"
  EOF
}

resource "google_cloud_scheduler_job" "daily_batch_trigger" {
  name        = "globaldata-daily-batch-trigger"
  region      = var.gcp_region
  description = "Aciona o workflow de batch a cada cinco minutos"
  schedule    = "*/5 * * * *"
  time_zone   = "UTC"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.batch_orchestration.id}/executions"
    oauth_token {
      service_account_email = data.google_compute_default_service_account.default.email
    }
  }
}

data "google_compute_default_service_account" "default" {
  depends_on = [google_project_service.compute_api]
}