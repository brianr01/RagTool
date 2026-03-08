import hashlib
from pathlib import Path

import structlog
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import settings
from shared.embeddings import get_embeddings
from shared.models import Document, Chunk
from ingest.extractors.base import extract_file, get_file_type
from ingest.chunker import chunk_text, estimate_tokens

logger = structlog.get_logger()

SUPPORTED_EXTENSIONS = {"txt", "md", "pdf", "docx", "csv", "json"}


def compute_file_hash(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            sha256.update(block)
    return sha256.hexdigest()


def derive_collection(relative_path: str) -> str:
    parts = Path(relative_path).parts
    if len(parts) > 1:
        return parts[0]
    return "default"


def get_relative_path(file_path: Path) -> str:
    data_dir = Path(settings.data_dir)
    return str(file_path.relative_to(data_dir))


def is_supported_file(file_path: Path) -> bool:
    return (
        file_path.is_file()
        and file_path.suffix.lstrip(".").lower() in SUPPORTED_EXTENSIONS
        and not file_path.name.startswith(".")
    )


async def ingest_file(session: AsyncSession, file_path: Path) -> Document:
    relative = get_relative_path(file_path)
    collection = derive_collection(relative)
    file_type = get_file_type(file_path)
    file_hash = compute_file_hash(file_path)
    file_size = file_path.stat().st_size

    logger.info("Ingesting file", filename=relative, collection=collection, file_type=file_type)

    # Check if document exists already
    result = await session.execute(
        select(Document).where(Document.filename == relative)
    )
    existing = result.scalar_one_or_none()

    if existing:
        if existing.file_hash == file_hash and existing.status == "ready":
            logger.info("File unchanged, skipping", filename=relative)
            return existing
        # Delete old chunks and update document
        await session.execute(
            delete(Chunk).where(Chunk.document_id == existing.id)
        )
        doc = existing
        doc.file_hash = file_hash
        doc.file_size = file_size
        doc.file_type = file_type
        doc.collection = collection
        doc.status = "processing"
        doc.error_message = None
    else:
        doc = Document(
            collection=collection,
            filename=relative,
            file_hash=file_hash,
            file_size=file_size,
            file_type=file_type,
            status="processing",
        )
        session.add(doc)

    await session.flush()

    try:
        # Extract text
        segments = extract_file(file_path)
        if not segments:
            doc.status = "ready"
            doc.chunk_count = 0
            await session.commit()
            return doc

        # Get page count for PDFs
        if file_type == "pdf":
            from ingest.extractors.pdf_extractor import PdfExtractor
            doc.page_count = PdfExtractor.get_page_count(file_path)

        # Chunk all segments
        all_chunks: list[tuple[str, dict]] = []
        for segment in segments:
            text_chunks = chunk_text(segment.text)
            for chunk_text_content in text_chunks:
                all_chunks.append((chunk_text_content, segment.metadata))

        if not all_chunks:
            doc.status = "ready"
            doc.chunk_count = 0
            await session.commit()
            return doc

        # Embed all chunks
        texts = [c[0] for c in all_chunks]
        embeddings = await get_embeddings(texts)

        # Store chunks
        for i, ((content, metadata), embedding) in enumerate(zip(all_chunks, embeddings)):
            chunk = Chunk(
                document_id=doc.id,
                collection=collection,
                chunk_index=i,
                content=content,
                token_count=estimate_tokens(content),
                metadata_=metadata,
                embedding=embedding,
            )
            session.add(chunk)

        doc.chunk_count = len(all_chunks)
        doc.status = "ready"
        await session.commit()
        logger.info(
            "File ingested successfully",
            filename=relative,
            chunks=len(all_chunks),
            collection=collection,
        )
        return doc

    except Exception as e:
        logger.error("Ingestion failed", filename=relative, error=str(e))
        doc.status = "error"
        doc.error_message = str(e)
        await session.commit()
        raise


async def remove_document(session: AsyncSession, filename: str):
    result = await session.execute(
        select(Document).where(Document.filename == filename)
    )
    doc = result.scalar_one_or_none()
    if doc:
        await session.delete(doc)
        await session.commit()
        logger.info("Document removed", filename=filename)
