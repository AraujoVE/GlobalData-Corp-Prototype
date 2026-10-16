#!/bin/bash

# Nome da imagem do projeto
IMAGE_NAME="globaldata-env-vea"

# Garante permissao de execucao nos scripts locais antes de subir
chmod +x *.sh 2>/dev/null

# Verifica se a imagem ja existe localmente. Se nao existir, faz o build automaticamente.
if [[ "$(docker images -q $IMAGE_NAME 2> /dev/null)" == "" ]]; then
    echo "Imagem '$IMAGE_NAME' nao encontrada. Construindo pela primeira vez (isso pode levar alguns minutos)..."
    docker build --no-cache -t $IMAGE_NAME .
fi

echo "Iniciando o container Docker..."

# Detecta o sistema operacional para ajustar os volumes corretamente
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    docker run --rm -it -v "$HOME/.config/gcloud:/root/.config/gcloud" -v "${PWD}:/app" $IMAGE_NAME
else
    docker run --rm -it -v "$HOME/.config/gcloud:/root/.config/gcloud" -v "$(pwd):/app" $IMAGE_NAME
fi