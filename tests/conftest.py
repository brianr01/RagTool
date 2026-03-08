import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Set test environment before importing shared modules
os.environ.setdefault("POSTGRES_HOST", "postgres")
os.environ.setdefault("OLLAMA_BASE_URL", "http://ollama:11434")

from shared.config import settings
from shared.models import Base

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_engine():
    engine = create_async_engine(settings.database_url_async, echo=False)
    async with engine.begin() as conn:
        await conn.execute(sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def db_session(db_engine):
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture(scope="session", autouse=True)
def generate_binary_fixtures():
    """Generate binary fixtures (PDF, DOCX) that can't be stored as plain text."""
    import subprocess
    import sys
    script = Path(__file__).parent / "create_fixtures.py"
    subprocess.run([sys.executable, str(script)], check=True)


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


def make_mock_embedding(dim=768):
    """Create a deterministic mock embedding."""
    import hashlib

    def _mock(texts):
        embeddings = []
        for text in texts:
            h = hashlib.md5(text.encode()).hexdigest()
            # Create a repeating pattern from the hash
            nums = [int(h[i : i + 2], 16) / 255.0 for i in range(0, len(h), 2)]
            embedding = (nums * (dim // len(nums) + 1))[:dim]
            embeddings.append(embedding)
        return embeddings

    return _mock


@pytest.fixture
def mock_embeddings():
    """Fixture that patches get_embeddings with deterministic mock."""
    mocker = make_mock_embedding()

    async def mock_get_embeddings(texts):
        return mocker(texts)

    async def mock_get_embedding(text):
        return mocker([text])[0]

    with (
        patch("shared.embeddings.get_embeddings", side_effect=mock_get_embeddings) as m1,
        patch("shared.embeddings.get_embedding", side_effect=mock_get_embedding) as m2,
    ):
        yield m1, m2
