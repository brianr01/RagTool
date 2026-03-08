from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.embeddings import get_embedding
from shared.models import Chunk, Document


async def search_chunks(
    session: AsyncSession,
    query: str,
    collection: str | None = None,
    top_k: int = 5,
    similarity_threshold: float = 0.3,
    file_type: str | None = None,
    filename: str | None = None,
) -> list[dict]:
    query_embedding = await get_embedding(query)

    similarity_expr = 1 - Chunk.embedding.cosine_distance(query_embedding)

    # Build the query with cosine distance
    stmt = (
        select(
            Chunk.content,
            Chunk.collection,
            Chunk.chunk_index,
            Chunk.metadata_,
            Chunk.document_id,
            Document.filename,
            Document.file_type,
            similarity_expr.label("similarity"),
        )
        .join(Document, Chunk.document_id == Document.id)
        .where(Document.status == "ready")
        .where(similarity_expr >= similarity_threshold)
    )

    if collection and collection.lower() != "all":
        stmt = stmt.where(Chunk.collection == collection)

    if file_type:
        stmt = stmt.where(Document.file_type == file_type)

    if filename:
        stmt = stmt.where(Document.filename.ilike(f"%{filename}%"))

    stmt = stmt.order_by(similarity_expr.desc()).limit(top_k)

    result = await session.execute(stmt)
    rows = result.all()

    return [
        {
            "content": row.content,
            "collection": row.collection,
            "filename": row.filename,
            "file_type": row.file_type,
            "chunk_index": row.chunk_index,
            "metadata": row.metadata_,
            "similarity": round(float(row.similarity), 4),
        }
        for row in rows
    ]
