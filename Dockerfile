FROM python:3.11-slim

WORKDIR /app

# Install system deps (needed by reportlab, psycopg2-binary, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "chat_client.py"]
