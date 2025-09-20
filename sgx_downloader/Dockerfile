FROM python:3.11-slim

# Environment setup
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends unzip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (better cache reuse)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Create data directories (aligned with docker-compose volumes)
RUN mkdir -p /data/raw /data/reference

VOLUME ["/data/raw", "/data/reference"]

EXPOSE 5022

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5022"]
