import structlog
import httpx

from shared.config import settings

logger = structlog.get_logger()

BATCH_SIZE = 32


async def get_embeddings(texts: list[str]) -> list[list[float]]:
    all_embeddings = []
    async with httpx.AsyncClient(timeout=120.0) as client:
        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i : i + BATCH_SIZE]
            response = await client.post(
                f"{settings.ollama_base_url}/api/embed",
                json={"model": settings.ollama_embed_model, "input": batch},
            )
            response.raise_for_status()
            data = response.json()
            all_embeddings.extend(data["embeddings"])
    return all_embeddings


async def get_embedding(text: str) -> list[float]:
    results = await get_embeddings([text])
    return results[0]


async def ensure_model():
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.get(f"{settings.ollama_base_url}/api/tags")
        response.raise_for_status()
        models = [m["name"] for m in response.json().get("models", [])]
        model_name = settings.ollama_embed_model

        if not any(model_name in m for m in models):
            logger.info("Pulling embedding model", model=model_name)
            response = await client.post(
                f"{settings.ollama_base_url}/api/pull",
                json={"name": model_name},
                timeout=600.0,
            )
            response.raise_for_status()
            logger.info("Model pulled successfully", model=model_name)
        else:
            logger.info("Embedding model already available", model=model_name)
