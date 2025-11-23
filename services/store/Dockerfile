# ...existing code...
FROM python:3.11-slim

WORKDIR /app

COPY services/store/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Provide services package for imports used by uvicorn
COPY services /app/services

COPY services/store/main.py .

ENV PORT=8101

CMD ["uvicorn", "services.store.main:app", "--host", "0.0.0.0", "--port", "8101"]
# ...existing code...