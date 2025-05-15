FROM mcr.microsoft.com/devcontainers/python:3.11

# Install system dependencies and Terraform
RUN apt-get update && apt-get install -y \
    postgresql-client \
    git \
    curl \
    wget \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Install Terraform
    && wget https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip \
    && unzip terraform_1.7.5_linux_amd64.zip \
    && mv terraform /usr/local/bin/ \
    && rm terraform_1.7.5_linux_amd64.zip


# Install Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
 && apt-get update && apt-get install -y google-cloud-sdk

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Prefect and tools
RUN pip install prefect psycopg2-binary

# Set working directory to match compose file
WORKDIR /workspaces/CamOnDagster