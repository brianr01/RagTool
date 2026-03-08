import os
import shutil
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select

from shared.models import Document, Chunk
from ingest.pipeline import ingest_file, derive_collection, compute_file_hash

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestDeriveCollection:
    def test_top_level_folder(self):
        assert derive_collection("code/api.py") == "code"

    def test_nested_folder(self):
        assert derive_collection("code/lib/utils.py") == "code"

    def test_root_file(self):
        assert derive_collection("notes.txt") == "default"

    def test_plans_folder(self):
        assert derive_collection("plans/roadmap.md") == "plans"


class TestComputeFileHash:
    def test_hash_consistency(self):
        path = FIXTURES_DIR / "sample.txt"
        h1 = compute_file_hash(path)
        h2 = compute_file_hash(path)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex


@pytest.mark.asyncio
class TestIngestFile:
    async def test_ingest_txt(self, db_session, mock_embeddings, tmp_path):
        # Set up a data dir structure
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        test_file = code_dir / "test.txt"
        test_file.write_text("This is a test document for the RAG pipeline.")

        os.environ["DATA_DIR"] = str(data_dir)
        # Reimport to pick up the new DATA_DIR
        from shared.config import Settings
        settings = Settings()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            doc = await ingest_file(db_session, test_file)
            assert doc.status == "ready"
            assert doc.collection == "code"
            assert doc.file_type == "txt"
            assert doc.chunk_count >= 1

            # Verify chunks exist
            result = await db_session.execute(
                select(Chunk).where(Chunk.document_id == doc.id)
            )
            chunks = result.scalars().all()
            assert len(chunks) == doc.chunk_count
            assert all(c.collection == "code" for c in chunks)
            assert all(c.embedding is not None for c in chunks)

    async def test_ingest_root_file(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True)
        test_file = data_dir / "notes.txt"
        test_file.write_text("Root level notes file.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            doc = await ingest_file(db_session, test_file)
            assert doc.collection == "default"

    async def test_ingest_unchanged_file_skips(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        test_file = code_dir / "skip_test.txt"
        test_file.write_text("Content that should not be re-ingested.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            doc1 = await ingest_file(db_session, test_file)
            assert doc1.status == "ready"

            # Ingest again - should skip
            doc2 = await ingest_file(db_session, test_file)
            assert doc2.id == doc1.id
