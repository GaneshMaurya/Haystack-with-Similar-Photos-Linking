# ...existing code...
FROM python:3.11-slim

WORKDIR /app

COPY services/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ensure import path "services.api.main" resolves
COPY services /app/services

COPY services/api/main.py .

CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
# ...existing code...