FROM python:3.12-slim

WORKDIR /app

COPY tests/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY ingest/ /app/ingest/
COPY mcp_server/ /app/mcp_server/
COPY tests/ /app/tests/
COPY pytest.ini /app/pytest.ini

# Generate binary fixtures (PDF, DOCX)
RUN python tests/create_fixtures.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["pytest", "tests/", "-v", "--tb=short"]
