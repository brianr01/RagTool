import json
import logging

import structlog
from mcp.server.fastmcp import FastMCP

from shared.config import settings
from shared.db import async_session_factory, init_db
from shared.embeddings import ensure_model
from mcp_server.search import search_chunks
from mcp_server.documents import list_collections, list_documents, get_document

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelNamesMapping().get(settings.log_level, logging.INFO)
    ),
)
logger = structlog.get_logger()

mcp = FastMCP("RAG Database", port=settings.mcp_port, host="0.0.0.0")


@mcp.tool()
async def search_documents(
    query: str,
    collection: str = "all",
    top_k: int = 5,
    similarity_threshold: float = 0.3,
    file_type: str = "",
    filename: str = "",
) -> str:
    """Search for documents using semantic similarity.

    Args:
        query: The search query text.
        collection: Collection to search (e.g., "code", "plans"). Use "all" for all collections.
        top_k: Maximum number of results to return.
        similarity_threshold: Minimum similarity score (0-1).
        file_type: Filter by file type (e.g., "pdf", "md").
        filename: Filter by filename (partial match).
    """
    async with async_session_factory() as session:
        results = await search_chunks(
            session,
            query=query,
            collection=collection if collection != "all" else None,
            top_k=top_k,
            similarity_threshold=similarity_threshold,
            file_type=file_type or None,
            filename=filename or None,
        )
    return json.dumps(results, indent=2)


@mcp.tool()
async def list_all_collections() -> str:
    """List all available collections with document and chunk counts."""
    async with async_session_factory() as session:
        results = await list_collections(session)
    return json.dumps(results, indent=2)


@mcp.tool()
async def list_all_documents(
    collection: str = "all",
    file_type: str = "",
    status: str = "",
) -> str:
    """List documents in the database.

    Args:
        collection: Filter by collection name. Use "all" for all collections.
        file_type: Filter by file type (e.g., "pdf", "md").
        status: Filter by status (e.g., "ready", "error").
    """
    async with async_session_factory() as session:
        results = await list_documents(
            session,
            collection=collection if collection != "all" else None,
            file_type=file_type or None,
            status=status or None,
        )
    return json.dumps(results, indent=2, default=str)


@mcp.tool()
async def get_document_detail(filename: str) -> str:
    """Get detailed information about a specific document including content preview.

    Args:
        filename: Exact or partial filename to look up.
    """
    async with async_session_factory() as session:
        result = await get_document(session, filename)
    if result is None:
        return json.dumps({"error": f"Document not found: {filename}"})
    return json.dumps(result, indent=2, default=str)


async def startup():
    logger.info("Starting MCP server")
    await ensure_model()
    await init_db()
    logger.info("MCP server ready")


if __name__ == "__main__":
    import asyncio

    asyncio.run(startup())
    mcp.run(transport="streamable-http")
