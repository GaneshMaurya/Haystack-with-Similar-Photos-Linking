FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

COPY services/similarity/requirements.txt requirements_similarity.txt

RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir \
    $(grep -v -e '^torch$' -e '^torchvision$' requirements_similarity.txt)

RUN mkdir -p data

COPY services services/

ENV PORT=8301

CMD ["uvicorn", "services.similarity.main:app", "--host", "0.0.0.0", "--port", "8301"]