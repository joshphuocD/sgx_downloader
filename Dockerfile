FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends unzip curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sgx_downloader.py .
COPY app.py .

RUN mkdir -p /app/downloads
VOLUME ["/app/downloads"]

EXPOSE 5022

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5022"]
