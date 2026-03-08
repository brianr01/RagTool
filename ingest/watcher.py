import asyncio
import threading
import time
from pathlib import Path

import structlog
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

from shared.config import settings
from shared.db import async_session_factory
from ingest.pipeline import (
    get_relative_path,
    ingest_file,
    is_supported_file,
    remove_document,
)

logger = structlog.get_logger()

DEBOUNCE_SECONDS = 2.0


class IngestHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def _schedule_process(self, file_path: str, event_type: str):
        with self._lock:
            self._pending[file_path] = time.time()
            if event_type == "deleted":
                self._pending[file_path] = -1  # Sentinel for deletion

        if self._timer:
            self._timer.cancel()
        self._timer = threading.Timer(DEBOUNCE_SECONDS, self._flush)
        self._timer.daemon = True
        self._timer.start()

    def _flush(self):
        with self._lock:
            pending = dict(self._pending)
            self._pending.clear()

        for file_path_str, timestamp in pending.items():
            if timestamp == -1:
                asyncio.run_coroutine_threadsafe(
                    self._handle_delete(file_path_str), self._loop
                )
            else:
                asyncio.run_coroutine_threadsafe(
                    self._handle_upsert(file_path_str), self._loop
                )

    async def _handle_upsert(self, file_path_str: str):
        file_path = Path(file_path_str)
        if not file_path.exists() or not is_supported_file(file_path):
            return
        try:
            async with async_session_factory() as session:
                await ingest_file(session, file_path)
        except Exception as e:
            logger.error("Watch ingest failed", path=file_path_str, error=str(e))

    async def _handle_delete(self, file_path_str: str):
        try:
            file_path = Path(file_path_str)
            data_dir = Path(settings.data_dir)
            relative = str(file_path.relative_to(data_dir))
            async with async_session_factory() as session:
                await remove_document(session, relative)
        except Exception as e:
            logger.error("Watch delete failed", path=file_path_str, error=str(e))

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_process(event.src_path, "created")

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_process(event.src_path, "modified")

    def on_deleted(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_process(event.src_path, "deleted")

    def on_moved(self, event: FileSystemEvent):
        if not event.is_directory:
            self._schedule_process(event.src_path, "deleted")
            self._schedule_process(event.dest_path, "created")


def start_watcher(loop: asyncio.AbstractEventLoop) -> Observer:
    data_dir = settings.data_dir
    logger.info("Starting file watcher", path=data_dir)
    handler = IngestHandler(loop)
    observer = Observer()
    observer.schedule(handler, data_dir, recursive=True)
    observer.daemon = True
    observer.start()
    return observer
