from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import Chunk, Document


async def list_collections(session: AsyncSession) -> list[dict]:
    result = await session.execute(
        select(
            Document.collection,
            func.count(Document.id).label("doc_count"),
            func.sum(Document.chunk_count).label("total_chunks"),
        )
        .where(Document.status == "ready")
        .group_by(Document.collection)
    )
    return [
        {
            "collection": row.collection,
            "doc_count": row.doc_count,
            "total_chunks": row.total_chunks or 0,
        }
        for row in result
    ]


async def list_documents(
    session: AsyncSession,
    collection: str | None = None,
    file_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    stmt = select(Document)

    if collection and collection.lower() != "all":
        stmt = stmt.where(Document.collection == collection)
    if file_type:
        stmt = stmt.where(Document.file_type == file_type)
    if status:
        stmt = stmt.where(Document.status == status)

    stmt = stmt.order_by(Document.created_at.desc())
    result = await session.execute(stmt)

    return [
        {
            "filename": doc.filename,
            "collection": doc.collection,
            "file_type": doc.file_type,
            "chunk_count": doc.chunk_count,
            "file_size": doc.file_size,
            "status": doc.status,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
        }
        for doc in result.scalars()
    ]


async def get_document(session: AsyncSession, filename: str) -> dict | None:
    # Try exact match first, then partial
    result = await session.execute(
        select(Document).where(Document.filename == filename)
    )
    doc = result.scalar_one_or_none()

    if doc is None:
        result = await session.execute(
            select(Document).where(Document.filename.ilike(f"%{filename}%"))
        )
        doc = result.scalars().first()

    if doc is None:
        return None

    # Get first 3 chunks as preview
    chunk_result = await session.execute(
        select(Chunk)
        .where(Chunk.document_id == doc.id)
        .order_by(Chunk.chunk_index)
        .limit(3)
    )
    preview_chunks = [
        {"chunk_index": c.chunk_index, "content": c.content[:500], "metadata": c.metadata_}
        for c in chunk_result.scalars()
    ]

    return {
        "filename": doc.filename,
        "collection": doc.collection,
        "file_type": doc.file_type,
        "file_hash": doc.file_hash,
        "file_size": doc.file_size,
        "page_count": doc.page_count,
        "chunk_count": doc.chunk_count,
        "status": doc.status,
        "error_message": doc.error_message,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
        "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
        "preview_chunks": preview_chunks,
    }
