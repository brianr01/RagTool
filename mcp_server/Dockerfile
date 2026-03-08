FROM python:3.12-slim

WORKDIR /app

COPY mcp_server/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY mcp_server/ /app/mcp_server/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "mcp_server.main"]
