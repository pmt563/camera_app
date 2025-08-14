# Use an official lightweight Python image
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

# Copy requirements file (if you have one)
# Or install dependencies directly
COPY requirements.txt .

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code and configs
COPY . .

# Expose any ports if necessary (not mandatory here)
# EXPOSE 1883

# Run the main script
CMD ["python", "hit_and_run_camera-kuksa.py"]
