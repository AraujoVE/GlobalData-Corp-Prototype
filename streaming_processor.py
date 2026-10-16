import re
import json
import logging
import threading
from utils import getProjectId
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam import pvalue


projectId = getProjectId()

# ---------------------------------------------------------
# CONFIGURACOES DO PIPELINE PRINCIPAL (MEDALLION)
# ---------------------------------------------------------
mainSubscriptionPath = f"projects/{projectId}/subscriptions/globaldata-corp-transactions-subscription"

bronzeTableSpec = f"{projectId}:globaldata_bronze.streaming_transaction_raw"
silverTableSpec = f"{projectId}:globaldata_silver.streaming_transaction"
goldTableSpec = f"{projectId}:globaldata_gold.streaming_transaction_suspicious"
mainDlqTableSpec = f"{projectId}:globaldata_bronze.streaming_transaction_dlq"

dlqSchema = {
    "fields": [
        {"name": "raw_message", "type": "STRING", "mode": "NULLABLE"},
        {"name": "error_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "failed_at", "type": "STRING", "mode": "NULLABLE"}
    ]
}

bronzeSilverSchema = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_timestamp", "type": "STRING", "mode": "NULLABLE"},
        {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "transaction_partner_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_company_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_company_account_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_account_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_name_pii", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_country_pii", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_document_pii", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_os", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_ip", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "risk_classification", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processed_at", "type": "STRING", "mode": "NULLABLE"}
    ]
}

goldSchema = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processed_at", "type": "STRING", "mode": "NULLABLE"}
    ]
}

class ProcessTransactionPipeline(beam.DoFn):
    def process(self, element):
        try:
            rawData = element.decode("utf-8")
            data = json.loads(rawData)
            
            transactionId = data.get("transaction_id")
            amount = data.get("amount")
            
            if not transactionId or amount is None:
                raise ValueError("Missing essential structural fields (transaction_id or amount)")
                
            status = data.get("status")
            txType = data.get("transaction_type")
            productType = data.get("product_type")
            deviceOs = data.get("device_os")
            
            txPartnerId = data.get("transaction_partner_id", "")
            clientId = data.get("client_id", "")
            clientAccountId = data.get("client_account_id", "")
            companyId = data.get("company_id", "")
            deviceId = data.get("device_id", "")
            
            timestampNow = beam.utils.timestamp.Timestamp.now().to_utc_datetime().isoformat()
            
            isRisky = (float(amount) > 4000.0 or status == "FRAUD_SUSPECT")
            riskClassification = "HIGH_PURCHASE" if float(amount) > 4000.0 else "FRAUD_SUSPECT"
            
            basePayload = {
                "transaction_id": transactionId,
                "transaction_timestamp": data.get("transaction_timestamp"),
                "transaction_type": txType,
                "transaction_partner_id": txPartnerId,
                "client_company_id": data.get("client_company_id"),
                "client_id": clientId,
                "client_company_account_id": data.get("client_company_account_id"),
                "client_account_id": clientAccountId,
                "client_name_pii": data.get("client_name_pii"),
                "client_country_pii": data.get("client_country_pii"),
                "client_document_pii": data.get("client_document_pii"),
                "company_id": companyId,
                "company_name": data.get("company_name"),
                "company_country": data.get("company_country"),
                "company_type": data.get("company_type"),
                "product_type": productType,
                "amount": float(amount),
                "currency": data.get("currency", "BRL"),
                "device_id": deviceId,
                "device_os": deviceOs,
                "device_ip": data.get("device_ip"),
                "status": status,
                "processed_at": timestampNow
            }
            
            yield pvalue.TaggedOutput('bronze', basePayload)
            
            validStatuses = ["APPROVED", "DECLINED", "PENDING", "FRAUD_SUSPECT"]
            validTxTypes = ["PURCHASE", "REFUND", "TRANSFER", "SUBSCRIPTION"]
            validProducts = ["DIGITAL_GOODS", "PHYSICAL_GOODS", "SERVICE", "SOFTWARE", "OTHERS"]
            validOs = ["Android", "iOS", "Windows", "MacOS", "Linux", "Other"]
            
            isPartnerValid = bool(re.match(r"^cli_\d{10}$", str(txPartnerId)))
            isClientValid = bool(re.match(r"^cli_\d{10}$", str(clientId)))
            isClientAccValid = bool(re.match(r"^acc_cli_\d{10}$", str(clientAccountId)))
            isCompanyValid = bool(re.match(r"^comp_\d{5}$", str(companyId)))
            isDeviceValid = bool(re.match(r"^dev_\d+$", str(deviceId)))
            
            isCleanData = (
                status in validStatuses and 
                txType in validTxTypes and 
                productType in validProducts and 
                deviceOs in validOs and
                isPartnerValid and
                isClientValid and
                isClientAccValid and
                isCompanyValid and
                isDeviceValid
            )
            
            if isCleanData:
                yield pvalue.TaggedOutput('silver', basePayload)
                
                if isRisky:
                    goldPayload = {
                        "transaction_id": transactionId,
                        "amount": float(amount),
                        "status": status,
                        "risk_classification": riskClassification,
                        "processed_at": timestampNow
                    }
                    yield pvalue.TaggedOutput('gold', goldPayload)
                    
        except Exception as e:
            errorPayload = {
                "raw_message": element.decode("utf-8", errors="ignore"),
                "error_reason": str(e),
                "failed_at": beam.utils.timestamp.Timestamp.now().to_utc_datetime().isoformat()
            }
            logging.warning(f"->> [DLQ ALERT] Structural failure captured: {e}")
            yield pvalue.TaggedOutput('dlq', errorPayload)

def run_main_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    print(f"->> Starting Complete Medallion Architecture Pipeline on subscription:\n{mainSubscriptionPath}")

    with beam.Pipeline(options=options) as p:
        rawStream = p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=mainSubscriptionPath)
        
        processedStreams = (
            rawStream 
            | "Process and Route Streams" >> beam.ParDo(ProcessTransactionPipeline()).with_outputs('bronze', 'silver', 'gold', 'dlq')
        )

        (
            processedStreams.bronze
            | "Window Bronze" >> beam.WindowInto(FixedWindows(60))
            | "Extract Bronze Key" >> beam.Map(lambda x: (x['transaction_id'], x))
            | "Deduplicate Bronze" >> beam.CombinePerKey(lambda elements: list(elements)[0])
            | "Extract Bronze Value" >> beam.Map(lambda kv: kv[1])
            | "Write to Bronze BQ" >> beam.io.WriteToBigQuery(
                bronzeTableSpec,
                schema=bronzeSilverSchema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        (
            processedStreams.silver
            | "Window Silver" >> beam.WindowInto(FixedWindows(60))
            | "Extract Silver Key" >> beam.Map(lambda x: (x['transaction_id'], x))
            | "Deduplicate Silver" >> beam.CombinePerKey(lambda elements: list(elements)[0])
            | "Extract Silver Value" >> beam.Map(lambda kv: kv[1])
            | "Write to Silver BQ" >> beam.io.WriteToBigQuery(
                silverTableSpec,
                schema=bronzeSilverSchema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        (
            processedStreams.gold
            | "Window Gold" >> beam.WindowInto(FixedWindows(60))
            | "Extract Gold Key" >> beam.Map(lambda x: (x['transaction_id'], x))
            | "Deduplicate Gold" >> beam.CombinePerKey(lambda elements: list(elements)[0])
            | "Extract Gold Value" >> beam.Map(lambda kv: kv[1])
            | "Write to Gold BQ" >> beam.io.WriteToBigQuery(
                goldTableSpec,
                schema=goldSchema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        
        (
            processedStreams.dlq
            | "Write to DLQ BigQuery" >> beam.io.WriteToBigQuery(
                mainDlqTableSpec,
                schema=dlqSchema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

dlqSubscriptionPath = f"projects/{projectId}/subscriptions/globaldata-corp-transactions-dlq-subscription"

class FormatDlqMessage(beam.DoFn):
    def process(self, element):
        yield {
            "raw_message": element.decode("utf-8", errors="ignore"),
            "error_reason": "Max delivery attempts exceeded in Pub/Sub subscription",
            "failed_at": beam.utils.timestamp.Timestamp.now().to_utc_datetime().isoformat()
        }

def run_dlq_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    print(f"->> Starting DLQ Consumer Pipeline on subscription:\n{dlqSubscriptionPath}")

    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Read from DLQ Pub/Sub" >> beam.io.ReadFromPubSub(subscription=dlqSubscriptionPath)
            | "Format DLQ Payload" >> beam.ParDo(FormatDlqMessage())
            | "Write DLQ to BigQuery" >> beam.io.WriteToBigQuery(
                mainDlqTableSpec,
                schema=dlqSchema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Cria as threads para rodar os dois pipelines de streaming simultaneamente
    t_main = threading.Thread(target=run_main_pipeline, name="MainPipelineThread")
    t_dlq = threading.Thread(target=run_dlq_pipeline, name="DlqPipelineThread")

    # Inicia as execucoes
    t_main.start()
    t_dlq.start()

    # Mantem o processo rodando enquanto as threads estiverem ativas
    t_main.join()
    t_dlq.join()