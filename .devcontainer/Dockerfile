FROM mcr.microsoft.com/devcontainers/python:3.11

# Install essential system dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Set working directory to match compose file
WORKDIR /workspaces/CamOnDagster

USER vscode