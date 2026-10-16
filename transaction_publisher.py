import os
import json
import time
import random
from datetime import datetime, timezone
from faker import Faker
from google.cloud import pubsub_v1
import re
from utils import getProjectId

fake = Faker("pt_BR")

PROJECT_ID = getProjectId()
TOPIC_ID = "globaldata-corp-transactions-topic"
publisher = pubsub_v1.PublisherClient()
topicPath = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def randomId(elemsNo):
    randomOrder = "".join(random.choice(["?", "#"]) for _ in range(elemsNo))
    return fake.bothify(text=randomOrder)

def generateTransactionEvent(): 
    statusOptions = ["APPROVED", "DECLINED", "PENDING", "FRAUD_SUSPECT", "#Invalid#"] 
    transactionOpions = ["PURCHASE", "REFUND", "TRANSFER", "SUBSCRIPTION", "#Invalid#"] 
    productOptions = ["DIGITAL_GOODS", "PHYSICAL_GOODS", "SERVICE", "SOFTWARE", "OTHERS", "#Invalid#"] 
    osOptions = ["Android", "iOS", "Windows", "MacOS", "Linux", "Other", "#Invalid#"] 
     
    transactionEvent = { 
        "transaction_id": f"tx_{fake.uuid4()}", 
        "transaction_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), 
        "transaction_type": random.choice(transactionOpions), 
        "transaction_partner_id": f"cli_{fake.random_number(digits=10, fix_len=True)}", 
        "client_company_id": str(fake.random_number(digits=10, fix_len=True)), 
        "client_id": f"cli_{fake.random_number(digits=10, fix_len=True)}", 
        "client_company_account_id": str(fake.random_number(digits=10, fix_len=True)), 
        "client_account_id": f"acc_cli_{fake.random_number(digits=10, fix_len=True)}", 
        "client_name_pii": fake.name(), 
        "client_country_pii": "BR", 
        "client_document_pii": fake.cpf(), 
        "company_id": f"comp_{fake.random_number(digits=5, fix_len=True)}", 
        "company_name": fake.company(), 
        "company_country": "BR", 
        "company_type": "E-commerce", 
        "product_type": random.choice(productOptions), 
        "amount": round(random.uniform(3998.0, 4001.0), 2), 
        "currency": "BRL", 
        "device_id": f"dev_{fake.random_number(digits=10, fix_len=True)}", 
        "device_os": random.choice(osOptions), 
        "device_ip": fake.ipv4(), 
        "status": random.choice(statusOptions) 
    } 
     
    return transactionEvent

def callback(future):
    try:
        messageId = future.result()
        print(f"\Message Successfully sent. Message ID: {messageId}")
    except Exception as e:
        print(f"Failure in publishing the message: {e}")

def streamMessages(minDelaySeconds=0.1, maxDelaySeconds=1, totalMessages=100):
    for i in range(10):
        print(f"{10 - i} segundos ate o inicio do envio")
        time.sleep(1)
    print(f"Initializing Cloud Publishing of Mocked Transaction Events: {topicPath}")
    
    for i in range(totalMessages):
        transactionEvent = generateTransactionEvent()
        
        isValidMessage = (i % 10 != 0)

        if isValidMessage:
            jsonFormatData = json.dumps(transactionEvent)
        else:
            jsonFormatData = '{"transaction_id": "INVALID_CORRUPT_TX", "amount": "INVALID_AMOUNT_STRING"'

        byteFormatData = jsonFormatData.encode("utf-8")

        print(f"[{i+1}] Sending Transaction: {transactionEvent['transaction_id'] if isValidMessage else 'Invalid Message (DLQ Test)'}")

        future = publisher.publish(topicPath, data=byteFormatData)
        future.add_done_callback(callback)
        
        
        time.sleep(random.uniform(minDelaySeconds, maxDelaySeconds))

if __name__ == "__main__":
    streamMessages()