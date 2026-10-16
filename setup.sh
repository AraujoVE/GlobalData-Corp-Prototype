#!/bin/bash
set -e

echo "=== 1. Verificando autenticacao e projeto GCP ==="

if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo "Nenhuma conta autenticada encontrada. Realizando login na Google Cloud..."
    gcloud auth login
fi

PROJECT_ID=$(grep -A 3 'variable "gcp_project_id"' terraform/variables.tf | grep 'default' | cut -d'"' -f2)

echo "Projeto extraido do Terraform: $PROJECT_ID"

gcloud config set project "$PROJECT_ID"

if [ ! -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
    gcloud auth application-default login
fi

cd terraform
terraform init
terraform plan -out=tfplan
terraform apply tfplan

cd ..
nohup python3 streaming_processor.py > processor.log 2>&1 &
echo "=== Ambiente configurado com sucesso! ==="