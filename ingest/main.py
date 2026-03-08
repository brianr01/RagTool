import asyncio
import logging

import structlog
import uvicorn
from fastapi import FastAPI

from shared.config import settings
from shared.db import init_db, async_session_factory
from shared.embeddings import ensure_model
from ingest.api import router
from ingest.reconciler import reconcile
from ingest.watcher import start_watcher

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelNamesMapping().get(settings.log_level, logging.INFO)
    ),
)
logger = structlog.get_logger()

app = FastAPI(title="Ingest Worker")
app.include_router(router)


@app.on_event("startup")
async def startup():
    logger.info("Starting ingest worker")

    # Ensure embedding model is available
    await ensure_model()

    # Initialize database tables
    await init_db()

    # Full reconciliation
    async with async_session_factory() as session:
        await reconcile(session)

    # Start file watcher
    loop = asyncio.get_event_loop()
    start_watcher(loop)

    logger.info("Ingest worker ready")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8100)
