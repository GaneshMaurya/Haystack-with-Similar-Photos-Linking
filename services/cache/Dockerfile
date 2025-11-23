# ...existing code...
FROM python:3.11-slim

WORKDIR /app

COPY services/cache/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p data/cache

# Ensure the whole services package is present so imports like "services.cache.main" work
COPY services /app/services

COPY services/cache/main.py .

ENV PORT=8201

CMD ["uvicorn", "services.cache.main:app", "--host", "0.0.0.0", "--port", "8201"]
# ...existing code...