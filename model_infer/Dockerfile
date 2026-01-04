FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy inference script (fixed filename)
COPY infer.py .

# Use python with infer.py as default, allow arg override
ENTRYPOINT ["python", "infer.py"]