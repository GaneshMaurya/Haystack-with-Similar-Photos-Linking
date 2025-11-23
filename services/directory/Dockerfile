# ...existing code...
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y sqlite3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/directory/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p data

# Add the whole services package so "services.directory.main" imports work
COPY services /app/services

COPY services/directory/main.py .
COPY services/directory/models.py .

CMD ["uvicorn", "services.directory.main:app", "--host", "0.0.0.0", "--port", "8001"]
# ...existing code...