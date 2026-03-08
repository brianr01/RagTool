from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TextSegment:
    text: str
    metadata: dict = field(default_factory=dict)


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, file_path: Path) -> list[TextSegment]:
        ...

    @property
    @abstractmethod
    def supported_extensions(self) -> list[str]:
        ...


def get_file_type(file_path: Path) -> str:
    return file_path.suffix.lstrip(".").lower()


def extract_file(file_path: Path) -> list[TextSegment]:
    from ingest.extractors import EXTRACTORS

    file_type = get_file_type(file_path)
    extractor_cls = EXTRACTORS.get(file_type)
    if extractor_cls is None:
        raise ValueError(f"Unsupported file type: {file_type}")
    return extractor_cls().extract(file_path)
