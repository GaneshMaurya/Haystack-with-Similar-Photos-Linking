FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code
COPY . .

# Define the volume for cache data so it persists
VOLUME /data/cache

# Expose the default Uvicorn port
EXPOSE 8000

# Run the app
CMD ["python", "main.py"]