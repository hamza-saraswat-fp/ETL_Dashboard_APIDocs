# Lightweight build for Render free tier (no Playwright)
FROM python:3.11-slim

WORKDIR /app

# Minimal system dependencies (no browser deps)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Skip Playwright installation (not needed for dashboard-only mode)

# Copy application code
COPY . .

# Create required directories
RUN mkdir -p /app/jobs /app/data /app/cache /app/logs

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
