FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://releases.hashicorp.com/terraform/1.16.6/terraform_1.16.6_linux_amd64.zip -o terraform.zip \
    && unzip terraform.zip \
    && mv terraform /usr/local/bin/ \
    && rm terraform.zip

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    apache-beam[gcp]==2.77.0 \
    google-cloud-bigquery==3.46.1 \
    google-cloud-pubsub==2.42.0 \
    google-cloud-storage==3.17.0 \
    faker==40.43.0 \
    pandas==3.0.6 \
    pyarrow==25.0.1

COPY . /app

CMD ["bash"]