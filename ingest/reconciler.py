from pathlib import Path

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import settings
from shared.models import Document
from ingest.pipeline import (
    compute_file_hash,
    get_relative_path,
    ingest_file,
    is_supported_file,
    remove_document,
)

logger = structlog.get_logger()


def scan_data_directory() -> dict[str, Path]:
    data_dir = Path(settings.data_dir)
    files: dict[str, Path] = {}
    if not data_dir.exists():
        return files
    for file_path in data_dir.rglob("*"):
        if is_supported_file(file_path):
            relative = get_relative_path(file_path)
            files[relative] = file_path
    return files


async def reconcile(session: AsyncSession) -> dict:
    logger.info("Starting reconciliation")

    # Get all files on disk
    disk_files = scan_data_directory()

    # Get all documents from DB
    result = await session.execute(select(Document))
    db_docs = {doc.filename: doc for doc in result.scalars().all()}

    stats = {"added": 0, "updated": 0, "removed": 0, "unchanged": 0, "errors": 0}

    # Find new and changed files
    for relative, file_path in disk_files.items():
        db_doc = db_docs.get(relative)
        if db_doc is None:
            # New file
            try:
                await ingest_file(session, file_path)
                stats["added"] += 1
            except Exception as e:
                logger.error("Failed to ingest new file", filename=relative, error=str(e))
                stats["errors"] += 1
        else:
            file_hash = compute_file_hash(file_path)
            if db_doc.file_hash != file_hash or db_doc.status in ("processing", "error"):
                # Changed file or previously failed
                try:
                    await ingest_file(session, file_path)
                    stats["updated"] += 1
                except Exception as e:
                    logger.error("Failed to re-ingest file", filename=relative, error=str(e))
                    stats["errors"] += 1
            else:
                stats["unchanged"] += 1

    # Find deleted files
    for filename in db_docs:
        if filename not in disk_files:
            await remove_document(session, filename)
            stats["removed"] += 1

    logger.info("Reconciliation complete", **stats)
    return stats
