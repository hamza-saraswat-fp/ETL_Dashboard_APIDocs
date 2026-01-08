"""
HVAC Catalog ETL API - Main FastAPI Application
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import logging

from .config import settings
from .database.connection import init_db
from .routes import jobs, health, results, dashboard, app
from .services.langwatch_service import init_langwatch


# API Description for Swagger UI
API_DESCRIPTION = """
## HVAC Catalog ETL API

Converts HVAC manufacturer catalogs into standardized costbooks using AI-powered transformation.

---

### Supported File Formats

| Format | Extensions | Requirements |
|--------|------------|--------------|
| **Excel** | `.xlsx`, `.xls`, `.xlsm` | Microsoft Excel format |
| **PDF** | `.pdf` | Must contain extractable tables (not scanned images) |

---

### What Works

**Excel Catalogs:**
- System pricing sheets (outdoor + indoor + coil bundles)
- Component pricing sheets (individual parts with model numbers and prices)
- Ductless/mini-split pricing with voltage specifications
- Any manufacturer format - AI interprets the structure

**PDF Catalogs:**
- Tables with clear column headers
- Text-based content (not scanned images)
- Multiple tables per document

---

### What Does NOT Work

| Input Type | Why It Fails |
|------------|--------------|
| Scanned PDFs | Image-based content requires OCR (not supported) |
| CSV, XML, JSON | Only Excel and PDF inputs supported |
| DOC, DOCX, ODS | Not supported |
| PDFs without tables | Unstructured text cannot be parsed |
| Excel with all sheets hidden | No visible data to extract |

---

### Quick Start

1. **Submit a job:** `POST /api/v1/jobs` with your catalog file
2. **Poll for status:** `GET /api/v1/jobs/{job_id}` until `status: completed`
3. **Download result:** `GET /api/v1/jobs/{job_id}/download`

---

### Pipeline Stages

```
Input → Stage 1 (Bronze) → Stage 2 (Silver) → Stage 3 (Gold) → Output
        Extract tables      AI Transform       Generate Excel
```

| Status | Description |
|--------|-------------|
| `pending` | Job queued |
| `stage1` | Extracting data |
| `stage2` | AI transformation |
| `stage3` | Generating Excel |
| `completed` | Ready for download |
| `failed` | Error occurred |

---

📚 **[Full Documentation](/api/v1/guide)** - Complete guide with examples, schema reference, and troubleshooting.
"""

# Tags for organizing endpoints in Swagger UI
TAGS_METADATA = [
    {
        "name": "Jobs",
        "description": "Submit ETL jobs and check their status. Jobs process HVAC catalogs through the 3-stage pipeline.",
    },
    {
        "name": "Results",
        "description": "Download processed files - final Excel costbook or intermediate artifacts (bronze/silver JSON).",
    },
    {
        "name": "Health",
        "description": "Service health and readiness checks.",
    },
    {
        "name": "Dashboard",
        "description": "Admin dashboard for monitoring jobs, viewing lineage, and debugging. Access the UI at /dashboard.",
    },
    {
        "name": "Documentation",
        "description": "Full system documentation and guides.",
    },
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown"""
    # Startup
    logger.info("Starting HVAC Catalog ETL API...")

    # Ensure directories exist
    settings.ensure_directories()
    logger.info(f"Jobs directory: {settings.JOBS_DIR}")
    logger.info(f"Cache directory: {settings.CACHE_DIR}")

    # Initialize database
    init_db()
    logger.info(f"Database initialized: {settings.DATABASE_URL}")

    # Check for API key
    if not settings.OPENROUTER_API_KEY:
        logger.warning("OPENROUTER_API_KEY not set - LLM transformation will fail")

    # Initialize LangWatch
    if settings.LANGWATCH_API_KEY and settings.LANGWATCH_ENABLED:
        if init_langwatch(settings.LANGWATCH_API_KEY, settings.LANGWATCH_ENABLED):
            logger.info("LangWatch initialized successfully")
        else:
            logger.warning("LangWatch initialization failed")
    else:
        logger.info("LangWatch not configured (set LANGWATCH_API_KEY to enable)")

    # Initialize templates for dashboard and app
    templates_dir = Path(__file__).parent / "templates"
    if templates_dir.exists():
        templates = Jinja2Templates(directory=str(templates_dir))
        dashboard.templates = templates
        app.templates = templates
        logger.info(f"Templates loaded from {templates_dir}")
    else:
        logger.warning(f"Templates directory not found: {templates_dir}")

    yield

    # Shutdown
    logger.info("Shutting down HVAC Catalog ETL API...")


app = FastAPI(
    title="HVAC Catalog ETL API",
    description=API_DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=TAGS_METADATA,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(jobs.router, prefix="/api/v1", tags=["Jobs"])
app.include_router(results.router, prefix="/api/v1", tags=["Results"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])

# Import app router with alias to avoid name conflict
from .routes import app as app_routes
app.include_router(app_routes.router, prefix="/app", tags=["App"])

# Mount static files for dashboard
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "name": "HVAC Catalog ETL API",
        "version": "1.0.0",
        "description": "Convert HVAC manufacturer catalogs to standardized costbooks",
        "docs": "/docs",
        "guide": "/api/v1/guide",
        "health": "/api/v1/health"
    }


@app.get("/api/v1/guide", response_class=HTMLResponse, tags=["Documentation"])
async def documentation_guide():
    """
    Full system documentation with examples, schema reference, and troubleshooting.

    This page provides comprehensive documentation for the HVAC Catalog ETL API.
    """
    import pathlib

    # Try to read the docs/README.md file
    docs_path = pathlib.Path(__file__).parent.parent / "docs" / "README.md"

    if docs_path.exists():
        content = docs_path.read_text()
    else:
        content = "# Documentation\n\nDocumentation file not found."

    # Simple HTML wrapper with markdown content
    # In production, you'd use a proper markdown renderer
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>HVAC Catalog ETL API - Documentation</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/github-markdown-css/github-markdown.min.css">
        <style>
            body {{
                box-sizing: border-box;
                min-width: 200px;
                max-width: 980px;
                margin: 0 auto;
                padding: 45px;
                background-color: #0d1117;
            }}
            .markdown-body {{
                background-color: #0d1117;
                color: #c9d1d9;
            }}
            .markdown-body h1, .markdown-body h2, .markdown-body h3 {{
                border-bottom-color: #21262d;
            }}
            .markdown-body code {{
                background-color: #161b22;
            }}
            .markdown-body pre {{
                background-color: #161b22;
            }}
            .markdown-body table tr {{
                background-color: #0d1117;
                border-top-color: #21262d;
            }}
            .markdown-body table tr:nth-child(2n) {{
                background-color: #161b22;
            }}
            .markdown-body table th, .markdown-body table td {{
                border-color: #30363d;
            }}
            .markdown-body a {{
                color: #58a6ff;
            }}
            .back-link {{
                margin-bottom: 20px;
            }}
            .back-link a {{
                color: #58a6ff;
                text-decoration: none;
            }}
            @media (max-width: 767px) {{
                body {{
                    padding: 15px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="back-link">
            <a href="/docs">← Back to API Documentation</a>
        </div>
        <article class="markdown-body" id="content"></article>
        <script>
            const markdown = {repr(content)};
            document.getElementById('content').innerHTML = marked.parse(markdown);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
