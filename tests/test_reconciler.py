import os
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select

from shared.models import Document
from ingest.reconciler import reconcile, scan_data_directory


@pytest.mark.asyncio
class TestReconciler:
    async def test_add_new_file(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        code_dir = data_dir / "code"
        code_dir.mkdir(parents=True)
        (code_dir / "new_file.txt").write_text("Brand new file for reconciliation test.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)
            mp.setattr("ingest.reconciler.settings", settings)

            stats = await reconcile(db_session)
            assert stats["added"] == 1

            result = await db_session.execute(
                select(Document).where(Document.filename == "code/new_file.txt")
            )
            doc = result.scalar_one()
            assert doc.status == "ready"
            assert doc.collection == "code"

    async def test_detect_changed_file(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        plans_dir = data_dir / "plans"
        plans_dir.mkdir(parents=True)
        test_file = plans_dir / "change_test.txt"
        test_file.write_text("Original content.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)
            mp.setattr("ingest.reconciler.settings", settings)

            stats1 = await reconcile(db_session)
            assert stats1["added"] == 1

            # Modify the file
            test_file.write_text("Modified content with changes.")

            stats2 = await reconcile(db_session)
            assert stats2["updated"] == 1

    async def test_detect_deleted_file(self, db_session, mock_embeddings, tmp_path):
        data_dir = tmp_path / "data"
        jobs_dir = data_dir / "jobs"
        jobs_dir.mkdir(parents=True)
        test_file = jobs_dir / "delete_test.txt"
        test_file.write_text("File to be deleted.")

        from shared.config import Settings
        settings = Settings()
        settings.data_dir = str(data_dir)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("shared.config.settings", settings)
            mp.setattr("ingest.pipeline.settings", settings)
            mp.setattr("ingest.reconciler.settings", settings)

            await reconcile(db_session)

            # Delete the file
            test_file.unlink()

            stats = await reconcile(db_session)
            assert stats["removed"] == 1

            result = await db_session.execute(
                select(Document).where(Document.filename == "jobs/delete_test.txt")
            )
            assert result.scalar_one_or_none() is None
