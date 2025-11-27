FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for imaging
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from service folder
COPY services/similarity/requirements.txt .

# Install dependencies (CPU version of torch)
RUN pip install --no-cache-dir -r requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu

# Copy the service code into /app
COPY services/similarity/ .

# Create directory for images and index
RUN mkdir -p /data/similarity_images

EXPOSE 8000

CMD ["python", "main.py"]