FROM --platform=$TARGETPLATFORM python:3.12-slim AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

RUN echo "-- Running on $BUILDPLATFORM, building for $TARGETPLATFORM"

# Stage 2: Actual application image
FROM python:3.12-slim

# Install system dependencies, including ffmpeg and libraries needed by OpenCV
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code and configs
COPY . .

# Expose ports if necessary
# EXPOSE 1883

# Run the main script
CMD ["python", "hit_and_run_camera-kuksa.py"]
