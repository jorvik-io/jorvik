# Base image with Ubuntu latest
FROM ubuntu:latest

# Install system dependencies and Python
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    python3-venv \
    wget \
    curl \
    gnupg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install OpenJDK 11 (required for PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment to avoid PEP 668 error https://peps.python.org/pep-0668/
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
WORKDIR /workspace
COPY . .
RUN pip install -e '.[tests]'
