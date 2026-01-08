# HVAC Catalog ETL API - Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for PDF processing and Playwright
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (for AHRI enrichment)
RUN playwright install chromium --with-deps || true

# Copy application code
COPY . .

# Create required directories
RUN mkdir -p /app/jobs /app/data /app/cache /app/logs

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Run the application
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
