# Use the official Python image as the base image
FROM python:3.11-slim

# Set the working directory in the container to the project root
WORKDIR /app

# Install requirements
COPY services/discovery/requirements.txt requirements_discovery.txt
RUN pip install --no-cache-dir -r requirements_discovery.txt

# Create the data directory for the shared discovery file
RUN mkdir -p data

# Copy the entire services structure
COPY services services/

# Command to run the Discovery service
CMD ["uvicorn", "services.discovery.main:app", "--host", "0.0.0.0", "--port", "8501"]