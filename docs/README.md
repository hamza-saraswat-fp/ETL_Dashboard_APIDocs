# HVAC Catalog ETL API

A production-ready API service that converts HVAC manufacturer catalogs into standardized costbooks using AI-powered transformation.

---

## Table of Contents

1. [Overview](#overview)
2. [Supported Catalog Formats](#supported-catalog-formats)
3. [API Reference](#api-reference)
4. [Data Flow](#data-flow)
5. [Output Schema](#output-schema)
6. [Configuration](#configuration)
7. [Deployment](#deployment)
8. [Integration Examples](#integration-examples)
9. [Troubleshooting](#troubleshooting)

---

## Overview

### What This Tool Does

The HVAC Catalog ETL system transforms manufacturer pricing catalogs into standardized costbooks. It handles the messy reality of HVAC catalogs—different formats, layouts, and naming conventions—and produces consistent, structured output.

### The 3-Stage Pipeline

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   INPUT     │      │   STAGE 1   │      │   STAGE 2   │      │   STAGE 3   │
│             │      │   BRONZE    │      │   SILVER    │      │    GOLD     │
│ Excel/PDF   │─────▶│  Raw JSON   │─────▶│ Structured  │─────▶│   Excel     │
│ Catalog     │      │ (Extract)   │      │   JSON      │      │  Costbook   │
│             │      │             │      │ (Transform) │      │   (Load)    │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
```

| Stage | Name | What It Does |
|-------|------|--------------|
| 1 | **Bronze (Extract)** | Extracts all tables/sheets from input file into raw JSON |
| 2 | **Silver (Transform)** | AI interprets the data and structures it into standardized schema |
| 3 | **Gold (Load)** | Generates final Excel costbook with 32 standardized columns |

### Target Users

- HVAC distributors converting manufacturer catalogs
- Contractors building pricing databases
- Integration with FieldPulse and other field service software
- Workflow automation via n8n, Zapier, etc.

---

## Supported Catalog Formats

### Supported File Types

| Format | Extensions | Requirements |
|--------|------------|--------------|
| **Excel** | `.xlsx`, `.xls`, `.xlsm` | Microsoft Excel format |
| **PDF** | `.pdf` | Must contain extractable tables (not scanned images) |

---

### What WORKS

#### Excel Catalogs

| Catalog Type | Description | Example |
|--------------|-------------|---------|
| **System Pricing Sheets** | Bundled systems with outdoor + indoor + coil | "Single Stage Cooling", "Heat Pump Systems" |
| **Component Pricing Sheets** | Individual parts with model numbers and prices | "Outdoor Units", "Coils", "Furnaces" |
| **Ductless Systems** | Mini-split and multi-zone pricing | Voltage specs, indoor/outdoor pairings |
| **Dealer Cost Sheets** | Standalone component lists | Any format with models and prices |

**Requirements:**
- At least one visible sheet (hidden sheets are skipped)
- Header row containing recognizable keywords (see below)
- Data in tabular format

**Header Keywords Detected:**
```
model, price, cost, ton, tonnage, seer, btu, outdoor, indoor,
furnace, coil, evap, evaporator, ahri, description, qty, quantity
```

The system searches the first 20 rows for headers containing at least 2 of these keywords.

#### PDF Catalogs

| Requirement | Description |
|-------------|-------------|
| **Table Structure** | Must be actual tables, not text arranged to look like tables |
| **Text-Based** | Content must be selectable text, not images |
| **Clear Headers** | Column headers should be identifiable |

---

### What DOES NOT WORK

| Format | Why It Fails |
|--------|--------------|
| **Scanned PDFs** | Image-based content requires OCR (not supported) |
| **CSV, XML, JSON files** | Only Excel and PDF inputs are supported |
| **DOC, DOCX, ODS files** | Not supported |
| **Excel with all sheets hidden** | No visible data to extract |
| **PDFs without tables** | Unstructured text cannot be parsed |
| **Handwritten content** | Cannot be extracted |
| **Image-embedded data** | Tables as images are not readable |

---

### Tested Manufacturers

The following manufacturers' catalogs have been successfully processed:

- GE / Goodman (various product lines)
- Johnstone Supply (distributor catalogs)
- RunTru (low-GWP systems)
- AOR (price sheets)
- Bayou South Mechanical

**Note:** Any manufacturer's catalog will work as long as it meets the format requirements above. The AI transformation adapts to different layouts and naming conventions.

---

## API Reference

**Base URL:** `http://your-server:8000/api/v1`

### Health Endpoints

#### GET /health

Basic health check.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "uptime_seconds": 3600
}
```

#### GET /health/ready

Readiness check - verifies all dependencies are available.

**Response:**
```json
{
  "ready": true,
  "checks": {
    "database": true,
    "api_key": true,
    "jobs_dir": true,
    "cache_dir": true
  }
}
```

#### GET /health/live

Liveness check - simple ping to verify service is running. Use for container orchestration liveness probes.

**Response:**
```json
{
  "alive": true
}
```

---

### Job Management

#### POST /jobs

Submit a new ETL job.

**Input Methods:**

| Method | Parameter | Description |
|--------|-----------|-------------|
| File Upload | `file` | Direct file upload (multipart/form-data) |
| URL | `url` | URL to download the catalog from |
| S3 | `s3_bucket` + `s3_key` | S3 bucket and object key |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `costbook_title` | string | "WinSupply" | Title for the output costbook |
| `enable_ahri_enrichment` | boolean | false | Enable AHRI database lookup for missing specs |

**Example - File Upload:**
```bash
curl -X POST "http://localhost:8000/api/v1/jobs" \
  -F "file=@catalog.xlsx" \
  -F "costbook_title=My Company Costbook" \
  -F "enable_ahri_enrichment=true"
```

**Example - URL:**
```bash
curl -X POST "http://localhost:8000/api/v1/jobs" \
  -F "url=https://example.com/catalog.xlsx" \
  -F "costbook_title=Downloaded Catalog"
```

**Example - S3:**
```bash
curl -X POST "http://localhost:8000/api/v1/jobs" \
  -F "s3_bucket=my-bucket" \
  -F "s3_key=catalogs/2025/goodman.xlsx"
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2025-01-15T10:30:00Z",
  "message": "Job queued for processing"
}
```

---

#### GET /jobs/{job_id}

Get status of a specific job.

**Response (Processing):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "stage2",
  "progress": {
    "current_stage": "stage2",
    "stage_progress": 45,
    "message": "Processing sheet 3 of 5"
  },
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:05Z",
  "completed_at": null,
  "error": null,
  "result": null
}
```

**Response (Completed):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "progress": {
    "current_stage": "stage3",
    "stage_progress": 100,
    "message": "Processing complete"
  },
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:05Z",
  "completed_at": "2025-01-15T10:32:05Z",
  "error": null,
  "result": {
    "output_file": "My_Company_Costbook_20250115_103500.xlsx",
    "download_url": "/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000/download",
    "stats": {
      "total_systems": 150,
      "total_components": 425,
      "processing_time_seconds": 120
    }
  }
}
```

**Response (Failed):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "progress": {
    "current_stage": "stage1",
    "stage_progress": 0,
    "message": null
  },
  "created_at": "2025-01-15T10:30:00Z",
  "started_at": "2025-01-15T10:30:05Z",
  "completed_at": "2025-01-15T10:30:10Z",
  "error": "No valid sheets found in Excel file",
  "result": null
}
```

---

#### GET /jobs

List all jobs with pagination and optional filtering.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 1 | Page number (starts at 1) |
| `page_size` | int | 20 | Number of records per page |
| `status` | string | - | Filter by status: pending, processing, completed, failed, cancelled |

**Example:**
```bash
# Get first page
curl "http://localhost:8000/api/v1/jobs?page=1&page_size=10"

# Filter by status
curl "http://localhost:8000/api/v1/jobs?status=completed"
```

**Response:**
```json
{
  "jobs": [
    {
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "status": "completed",
      "input_filename": "catalog.xlsx",
      "created_at": "2025-01-15T10:30:00Z",
      "completed_at": "2025-01-15T10:32:05Z"
    }
  ],
  "total": 45,
  "page": 1,
  "page_size": 10
}
```

---

#### DELETE /jobs/{job_id}

Cancel a pending job or delete a completed/failed/cancelled job.

**Behavior:**
| Job Status | Action |
|------------|--------|
| `pending` | Cancels the job |
| `completed`, `failed`, `cancelled` | Deletes job and all artifacts |
| `processing`, `stage1`, `stage2`, `stage3` | Not allowed (400 error) |

**Response (Cancelled):**
```json
{
  "message": "Job cancelled",
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response (Deleted):**
```json
{
  "message": "Job deleted",
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Error Response (Job in progress):**
```json
{
  "detail": "Cannot cancel/delete job with status: stage2. Only pending jobs can be cancelled, and only completed/failed/cancelled jobs can be deleted."
}
```

---

### Results

#### GET /jobs/{job_id}/download

Download the final Gold Excel costbook.

**Response:** Excel file download (`.xlsx`)

**Example:**
```bash
curl -O -J "http://localhost:8000/api/v1/jobs/{job_id}/download"
```

---

#### GET /jobs/{job_id}/artifacts/{stage}

Download intermediate processing files.

**Stages:**
| Stage | Description | Format |
|-------|-------------|--------|
| `input` | Original uploaded file | Excel/PDF |
| `bronze` | Raw extracted data | JSON |
| `silver` | Transformed structured data | JSON |
| `gold` | Final costbook | Excel |

**Example:**
```bash
# Download bronze JSON
curl -O "http://localhost:8000/api/v1/jobs/{job_id}/artifacts/bronze"

# Download silver JSON
curl -O "http://localhost:8000/api/v1/jobs/{job_id}/artifacts/silver"
```

---

### Dashboard (Admin UI)

The API includes a built-in admin dashboard for monitoring jobs and debugging. Access it at `/dashboard`.

#### Dashboard Pages

| URL | Description |
|-----|-------------|
| `/dashboard` | Main dashboard with job list and metrics |
| `/dashboard/jobs/{job_id}` | Job detail view with lineage, logs, and LLM metrics |
| `/dashboard/diff?job1=...&job2=...&stage=silver` | Compare outputs between two jobs |

#### Dashboard API Endpoints

These JSON endpoints power the dashboard and can be used programmatically:

| Endpoint | Description |
|----------|-------------|
| `GET /dashboard/api/jobs` | List jobs (supports `limit`, `status` params) |
| `GET /dashboard/api/jobs/{job_id}/lineage` | Get data lineage for a job |
| `GET /dashboard/api/jobs/{job_id}/llm-calls` | Get individual LLM calls |
| `GET /dashboard/api/jobs/{job_id}/llm-metrics` | Get aggregated LLM metrics (tokens, cost) |
| `GET /dashboard/api/jobs/{job_id}/logs` | Get structured job logs |
| `GET /dashboard/api/metrics` | Get dashboard summary metrics |
| `GET /dashboard/api/diff?job1=...&job2=...&stage=silver` | Compare two jobs |

**Note:** The dashboard uses HTMX for real-time updates. HTMX partial endpoints (`/dashboard/*-partial`) are for internal use.

---

## Data Flow

### Job Lifecycle States

```
pending ──▶ processing ──▶ stage1 ──▶ stage2 ──▶ stage3 ──▶ completed
    │                                                           │
    │                                                           │
    ▼                                                           ▼
cancelled                                                    failed
```

| Status | Description |
|--------|-------------|
| `pending` | Job created, waiting to start |
| `processing` | Job picked up by worker |
| `stage1` | Extracting data from input file |
| `stage2` | AI transformation in progress |
| `stage3` | Generating final Excel output |
| `completed` | Successfully finished |
| `failed` | Error occurred during processing |
| `cancelled` | Cancelled by user |

### File Storage

Each job creates a directory structure:

```
jobs/
└── {job_id}/
    ├── input/          # Original uploaded file
    │   └── catalog.xlsx
    ├── bronze/         # Stage 1 output
    │   └── catalog_bronze.json
    ├── silver/         # Stage 2 output
    │   └── catalog_silver.json
    └── gold/           # Stage 3 output (final result)
        └── My_Costbook_20250115.xlsx
```

---

## Output Schema

### Silver Layer Structure

The Stage 2 transformation produces structured JSON with the following schema:

```json
{
  "systems": [
    {
      "system_id": "216723483",
      "system_attributes": {
        "tonnage": 1.5,
        "capacity_btu": 18000,
        "seer2": 16.5,
        "eer2": 14.0,
        "hspf2": null,
        "system_type": "AC",
        "total_price": 2498.00,
        "ahri_number": "216723483",
        "source_sheet": "Single Stage Cooling",
        "voltage": "230V",
        "stages": "single",
        "configuration": "split"
      },
      "components": [
        {
          "component_type": "ODU",
          "model_number": "NS16A18SA5",
          "manufacturer": "Goodman",
          "price": 1157.00,
          "role": "outdoor_unit",
          "description": "16 SEER2 Single Stage AC",
          "specifications": {}
        },
        {
          "component_type": "Coil",
          "model_number": "NCHC24AT5S",
          "price": 506.00
        },
        {
          "component_type": "Furnace",
          "model_number": "NF80U045S3A",
          "price": 835.00
        }
      ],
      "metadata": {
        "raw_row_index": 5,
        "extraction_date": "2025-01-15T10:31:00Z",
        "catalog_name": "GE_ELITE_MA_2025.xlsx",
        "data_quality": "high",
        "notes": []
      }
    }
  ]
}
```

### Component Types

| Type | Description |
|------|-------------|
| `ODU` | Outdoor Unit (condenser, compressor) |
| `IDU` | Indoor Unit (ductless wall mount, cassette) |
| `Coil` | Evaporator coil |
| `Furnace` | Gas/oil/electric furnace with blower |
| `AirHandler` | Air handler / fan coil |
| `AuxHeat` | Auxiliary electric heat kit |
| `Thermostat` | Thermostat/control system |
| `Accessory` | Other accessories |
| `LineSet` | Refrigerant line set |
| `Other` | Uncategorized component |

### System Types

| Type | Description |
|------|-------------|
| `AC` | Air Conditioning (cooling only) |
| `HP` | Heat Pump (cooling + heating) |
| `Ductless` | Ductless mini-split system |
| `MultiZone` | Multi-zone ductless system |
| `Package` | Packaged unit (all-in-one) |
| `Unknown` | Cannot be determined |

### Gold Layer (Excel Output)

The final Excel costbook contains 32 standardized columns including:

- System identification (AHRI number, system type)
- Performance specs (tonnage, SEER2, EER2, HSPF2, BTU capacity)
- Component details (model numbers, prices for ODU, coil, furnace, etc.)
- Pricing (component prices, total system price)
- Metadata (source sheet, catalog name)

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENROUTER_API_KEY` | Yes | - | API key for LLM transformation |
| `DATABASE_URL` | No | `sqlite:///./data/jobs.db` | Database connection string |
| `JOBS_DIR` | No | `./jobs` | Directory for job artifacts |
| `CACHE_DIR` | No | `./cache` | Directory for AHRI cache |
| `LOGS_DIR` | No | `./logs` | Directory for application logs |
| `LLM_MODEL` | No | `anthropic/claude-sonnet-4` | LLM model to use |
| `MAX_CONCURRENT_JOBS` | No | `3` | Maximum parallel jobs |
| `MAX_FILE_SIZE_MB` | No | `100` | Maximum upload file size |
| `JOB_RETENTION_DAYS` | No | `7` | Days to keep completed jobs |
| `CORS_ORIGINS` | No | `["*"]` | Allowed CORS origins |
| `AWS_ACCESS_KEY_ID` | No | - | AWS credentials for S3 input |
| `AWS_SECRET_ACCESS_KEY` | No | - | AWS credentials for S3 input |
| `AWS_REGION` | No | `us-east-1` | AWS region for S3 |
| `LANGWATCH_API_KEY` | No | - | LangWatch API key for LLM observability |
| `LANGWATCH_ENABLED` | No | `false` | Enable LangWatch tracing |

### Example .env File

```env
# Required
OPENROUTER_API_KEY=sk-or-v1-your-key-here

# Optional - Directories
JOBS_DIR=./jobs
CACHE_DIR=./cache
LOGS_DIR=./logs

# Optional - LLM
LLM_MODEL=anthropic/claude-sonnet-4

# Optional - AWS S3
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1

# Optional - LangWatch Observability
LANGWATCH_API_KEY=lw-...
LANGWATCH_ENABLED=true
```

---

## Deployment

### Docker (Recommended)

**1. Create `.env` file:**
```bash
cp .env.example .env
# Edit .env and add your OPENROUTER_API_KEY
```

**2. Build and run:**
```bash
docker compose build
docker compose up -d
```

**3. Verify:**
```bash
curl http://localhost:8000/api/v1/health
```

### Docker Compose Configuration

```yaml
services:
  etl-api:
    build: .
    container_name: hvac-etl-api
    ports:
      - "8000:8000"
    environment:
      - OPENROUTER_API_KEY=${OPENROUTER_API_KEY}
      - DATABASE_URL=sqlite:////app/data/jobs.db
    volumes:
      - ./data:/app/data      # SQLite database
      - ./jobs:/app/jobs      # Job artifacts
      - ./cache:/app/cache    # AHRI cache
      - ./logs:/app/logs      # Application logs
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/v1/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped
```

### Useful Docker Commands

```bash
# View logs
docker compose logs -f

# Stop
docker compose down

# Restart
docker compose restart

# Rebuild after code changes
docker compose build && docker compose up -d
```

---

## Integration Examples

### n8n Workflow

Use HTTP Request nodes to integrate with the ETL API:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Trigger    │     │  Submit Job  │     │  Wait/Poll   │     │ Send Result  │
│  (Email/     │────▶│  POST /jobs  │────▶│  GET /jobs   │────▶│  (Slack/     │
│   Webhook)   │     │              │     │   /{id}      │     │   Email)     │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

**Step 1 - Submit Job (HTTP Request node):**
- Method: POST
- URL: `http://your-server:8000/api/v1/jobs`
- Body Type: Form-Data
- Parameters:
  - `file`: Binary data from trigger
  - `costbook_title`: Your title

**Step 2 - Poll Status (Loop + HTTP Request):**
- Method: GET
- URL: `http://your-server:8000/api/v1/jobs/{{ $json.job_id }}`
- Loop until: `status === "completed" || status === "failed"`

**Step 3 - Download Result (HTTP Request):**
- Method: GET
- URL: `http://your-server:8000/api/v1/jobs/{{ $json.job_id }}/download`
- Response Format: File

### cURL Examples

```bash
# Submit a job
JOB_ID=$(curl -s -X POST "http://localhost:8000/api/v1/jobs" \
  -F "file=@catalog.xlsx" \
  -F "costbook_title=Test" | jq -r '.job_id')

# Poll until complete
while true; do
  STATUS=$(curl -s "http://localhost:8000/api/v1/jobs/$JOB_ID" | jq -r '.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
  sleep 5
done

# Download result
curl -O -J "http://localhost:8000/api/v1/jobs/$JOB_ID/download"
```

---

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `No valid sheets found` | Excel has no recognizable data | Ensure file has visible sheets with header keywords |
| `Invalid file format` | Wrong file extension | Only `.xlsx`, `.xls`, `.xlsm`, `.pdf` supported |
| `API key not configured` | Missing `OPENROUTER_API_KEY` | Set the environment variable |
| `Job stuck in processing` | Server restarted during job | Delete job and resubmit |
| `PDF extraction failed` | Scanned/image PDF | Use text-based PDF or convert to Excel |

### Checking Logs

**Docker:**
```bash
docker compose logs -f etl-api
```

**Local:**
```bash
tail -f logs/app.log
```

### Verifying System Health

```bash
# Basic health
curl http://localhost:8000/api/v1/health

# Full readiness check
curl http://localhost:8000/api/v1/health/ready
```

### Cleaning Up Failed Jobs

Failed jobs leave artifacts in the `jobs/` directory. To clean up:

```bash
# List all job directories
ls -la jobs/

# Remove a specific job
rm -rf jobs/{job_id}
```

---

## API Errors

### Error Response Format

```json
{
  "detail": "Error message describing what went wrong"
}
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success (including job creation) |
| 400 | Bad Request (invalid input, invalid status, can't cancel/delete job) |
| 404 | Not Found (job doesn't exist, file not found) |
| 500 | Internal Server Error |

---

## Support

For issues or questions:
- Check the troubleshooting section above
- Review logs for detailed error messages
- Ensure input catalog meets format requirements
