from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_db: str = "ragdb"
    postgres_user: str = "raguser"
    postgres_password: str = "ragpass"
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    ollama_base_url: str = "http://ollama:11434"
    ollama_embed_model: str = "nomic-embed-text"
    embedding_dimensions: int = 768

    chunk_size: int = 512
    chunk_overlap: int = 50

    data_dir: str = "/app/data"
    log_level: str = "INFO"
    mcp_port: int = 8200

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_async(self) -> str:
        return (
            f"postgresql+psycopg_async://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
