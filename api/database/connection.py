"""
Database connection and session management
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from pathlib import Path
import logging

from ..config import settings
from .models import Base

logger = logging.getLogger(__name__)


def get_engine():
    """Create SQLAlchemy engine"""
    db_url = settings.DATABASE_URL

    if db_url.startswith("sqlite:///"):
        # SQLite: ensure directory exists and use check_same_thread
        db_path = db_url.replace("sqlite:///", "")
        if db_path.startswith("./"):
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        return create_engine(
            db_url,
            connect_args={"check_same_thread": False}
        )
    else:
        # PostgreSQL and other databases
        return create_engine(db_url)


# Create engine and session factory
engine = get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Create all tables if they don't exist"""
    logger.info(f"Initializing database: {settings.DATABASE_URL}")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")


@contextmanager
def get_db():
    """Database session context manager"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """FastAPI dependency for database sessions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
