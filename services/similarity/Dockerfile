FROM python:3.9-slim

WORKDIR /app

# Fix: Use 'libgl1' instead of 'libgl1-mesa-glx'
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Install torch cpu version specifically to keep image size smaller
RUN pip install --no-cache-dir -r requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu

COPY . .

# Create directory for FAISS index and downloaded images
RUN mkdir -p /data/similarity_images

EXPOSE 8000

CMD ["python", "main.py"]