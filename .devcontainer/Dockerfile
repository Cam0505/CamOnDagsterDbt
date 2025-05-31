FROM mcr.microsoft.com/devcontainers/python:3.11

# Install essential system dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy only dbt dependency files to leverage Docker cache
COPY dbt/packages.yml dbt/dbt_project.yml ./dbt/

# Run dbt deps to install dbt dependencies
RUN cd dbt && dbt deps

# Copy the rest of your application code
COPY . .

# Set working directory to match compose file
WORKDIR /workspaces/CamOnDagster

USER vscode