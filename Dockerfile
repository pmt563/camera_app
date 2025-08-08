# Use official Python base image
FROM docker.io/library/python:3.10-slim

# Install dependencies required by OpenCV
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    libopencv-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy application code
COPY app/ /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose any necessary ports (e.g. if Kuksa uses gRPC server or web dashboard)
EXPOSE 55555

# Command to run the application
CMD ["python", "main.py"]
