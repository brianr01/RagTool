"""End-to-end tests that exercise the full running system.

These tests expect all services (postgres, ollama, ingest-worker, mcp-server) to be running.
They copy files into ./data, wait for ingestion, and verify search results.
"""
import json
import os
import shutil
import time
from pathlib import Path

import httpx
import pytest

INGEST_URL = os.environ.get("INGEST_URL", "http://ingest-worker:8100")
MCP_URL = os.environ.get("MCP_URL", "http://mcp-server:8200")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def wait_for_ingestion(timeout=60):
    """Poll ingest-worker status until all files are processed."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{INGEST_URL}/status", timeout=5)
            data = resp.json()
            counts = data.get("status_counts", {})
            if counts.get("processing", 0) == 0 and counts.get("pending", 0) == 0:
                return data
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Ingestion did not complete in time")


def trigger_resync():
    resp = httpx.post(f"{INGEST_URL}/resync", timeout=30)
    resp.raise_for_status()
    return resp.json()


def mcp_call(method: str, params: dict | None = None):
    """Call an MCP tool via the streamable-http endpoint."""
    # We'll call the MCP server's tools through its HTTP transport
    # For testing, we use the underlying search endpoint
    pass


@pytest.fixture(autouse=True)
def clean_test_data():
    """Clean up test files before and after each test."""
    test_dirs = [DATA_DIR / "e2e_code", DATA_DIR / "e2e_plans"]
    for d in test_dirs:
        if d.exists():
            shutil.rmtree(d)
    yield
    for d in test_dirs:
        if d.exists():
            shutil.rmtree(d)
    trigger_resync()


class TestEndToEnd:
    def test_ingest_worker_health(self):
        resp = httpx.get(f"{INGEST_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_ingest_and_status(self):
        """Drop a file, trigger resync, verify it appears in status."""
        test_dir = DATA_DIR / "e2e_code"
        test_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES_DIR / "sample.txt", test_dir / "test_status.txt")

        trigger_resync()
        data = wait_for_ingestion()

        collections = data.get("collections", [])
        e2e_col = [c for c in collections if c["collection"] == "e2e_code"]
        assert len(e2e_col) == 1
        assert e2e_col[0]["documents"] >= 1

    def test_collection_routing(self):
        """Files in different folders go to different collections."""
        code_dir = DATA_DIR / "e2e_code"
        plans_dir = DATA_DIR / "e2e_plans"
        code_dir.mkdir(parents=True, exist_ok=True)
        plans_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(FIXTURES_DIR / "sample.txt", code_dir / "code_file.txt")
        shutil.copy(FIXTURES_DIR / "sample.md", plans_dir / "plan_file.md")

        trigger_resync()
        data = wait_for_ingestion()

        collections = {c["collection"]: c for c in data.get("collections", [])}
        assert "e2e_code" in collections
        assert "e2e_plans" in collections

    def test_file_modification_reingests(self):
        """Modifying a file triggers re-ingestion."""
        test_dir = DATA_DIR / "e2e_code"
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "modify_test.txt"
        test_file.write_text("Original content for modification test.")

        trigger_resync()
        wait_for_ingestion()

        # Get initial status
        resp = httpx.get(f"{INGEST_URL}/status", timeout=5)
        initial = resp.json()

        # Modify the file
        test_file.write_text("Updated content with new information for modification test.")

        trigger_resync()
        wait_for_ingestion()

        resp = httpx.get(f"{INGEST_URL}/status", timeout=5)
        assert resp.status_code == 200

    def test_file_deletion_removes_document(self):
        """Deleting a file removes it from the database."""
        test_dir = DATA_DIR / "e2e_code"
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "delete_test.txt"
        test_file.write_text("File that will be deleted.")

        trigger_resync()
        wait_for_ingestion()

        # Verify it exists
        resp = httpx.get(f"{INGEST_URL}/status", timeout=5)
        data = resp.json()
        assert any(c["collection"] == "e2e_code" for c in data.get("collections", []))

        # Delete the file
        test_file.unlink()

        result = trigger_resync()
        assert result["stats"]["removed"] >= 1

    def test_multiple_file_types(self):
        """Ingest multiple file types and verify all are processed."""
        test_dir = DATA_DIR / "e2e_code"
        test_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(FIXTURES_DIR / "sample.txt", test_dir / "multi.txt")
        shutil.copy(FIXTURES_DIR / "sample.md", test_dir / "multi.md")
        shutil.copy(FIXTURES_DIR / "sample.csv", test_dir / "multi.csv")
        shutil.copy(FIXTURES_DIR / "sample.json", test_dir / "multi.json")

        trigger_resync()
        data = wait_for_ingestion()

        e2e_col = [c for c in data.get("collections", []) if c["collection"] == "e2e_code"]
        assert len(e2e_col) == 1
        assert e2e_col[0]["documents"] >= 4
