import os
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select

from shared.models import Document, Chunk
from ingest.pipeline import ingest_file
from mcp_server.search import search_chunks
from mcp_server.documents import list_collections, list_documents, get_document


@pytest.mark.asyncio
class TestSearchChunks:
    async def test_search_returns_results(self, db_session, mock_embeddings, tmp_path):
        # Ingest a test file first
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        test_file = code_dir / "search_test.txt"
        test_file.write_text("Python programming with machine learning and neural networks.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, test_file)

            results = await search_chunks(
                db_session,
                query="machine learning",
                similarity_threshold=0.0,
            )
            assert len(results) >= 1
            assert results[0]["collection"] == "code"

    async def test_search_with_collection_filter(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        plans_dir = data_dir / "plans"
        code_dir.mkdir(parents=True)
        plans_dir.mkdir(parents=True)

        (code_dir / "filter_code.txt").write_text("Code related content about APIs.")
        (plans_dir / "filter_plan.txt").write_text("Planning document about roadmap.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, code_dir / "filter_code.txt")
            await ingest_file(db_session, plans_dir / "filter_plan.txt")

            # Search only code collection
            results = await search_chunks(
                db_session,
                query="content",
                collection="code",
                similarity_threshold=0.0,
            )
            assert all(r["collection"] == "code" for r in results)


@pytest.mark.asyncio
class TestListCollections:
    async def test_list(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        (code_dir / "coll_test.txt").write_text("Collection test file.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, code_dir / "coll_test.txt")

            collections = await list_collections(db_session)
            assert any(c["collection"] == "code" for c in collections)


@pytest.mark.asyncio
class TestListDocuments:
    async def test_list_all(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        (code_dir / "list_test.txt").write_text("List test content.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, code_dir / "list_test.txt")

            docs = await list_documents(db_session)
            assert len(docs) >= 1


@pytest.mark.asyncio
class TestGetDocument:
    async def test_get_by_filename(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        (code_dir / "detail_test.txt").write_text("Detail test content for retrieval.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, code_dir / "detail_test.txt")

            result = await get_document(db_session, "code/detail_test.txt")
            assert result is not None
            assert result["collection"] == "code"
            assert result["status"] == "ready"
            assert len(result["preview_chunks"]) >= 1

    async def test_get_partial_match(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        (code_dir / "partial_test.txt").write_text("Partial match test.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)

            await ingest_file(db_session, code_dir / "partial_test.txt")

            result = await get_document(db_session, "partial_test")
            assert result is not None

    async def test_get_nonexistent(self, db_session):
        result = await get_document(db_session, "nonexistent_file.txt")
        assert result is None
