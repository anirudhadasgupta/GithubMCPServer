FROM python:3.11-slim

# Install git for repository cloning
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create repos directory
RUN mkdir -p /tmp/repos

# Environment variables
ENV HOST=0.0.0.0
ENV PORT=8000
ENV REPO_STORAGE_PATH=/tmp/repos
ENV ALLOWED_USERNAME=anirudhadasgupta

EXPOSE 8000

CMD ["python", "main.py"]
