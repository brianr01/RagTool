from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_session
from shared.models import Document
from ingest.reconciler import reconcile

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/resync")
async def resync(session: AsyncSession = Depends(get_session)):
    stats = await reconcile(session)
    return {"status": "ok", "stats": stats}


@router.get("/status")
async def status(session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(
            Document.status,
            func.count(Document.id).label("count"),
        ).group_by(Document.status)
    )
    status_counts = {row.status: row.count for row in result}

    result = await session.execute(
        select(
            Document.collection,
            func.count(Document.id).label("doc_count"),
            func.sum(Document.chunk_count).label("chunk_count"),
        )
        .where(Document.status == "ready")
        .group_by(Document.collection)
    )
    collections = [
        {
            "collection": row.collection,
            "documents": row.doc_count,
            "chunks": row.chunk_count or 0,
        }
        for row in result
    ]

    return {
        "status_counts": status_counts,
        "collections": collections,
    }
