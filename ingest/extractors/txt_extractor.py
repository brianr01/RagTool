from pathlib import Path

from ingest.extractors.base import BaseExtractor, TextSegment


class TxtExtractor(BaseExtractor):
    @property
    def supported_extensions(self) -> list[str]:
        return ["txt"]

    def extract(self, file_path: Path) -> list[TextSegment]:
        text = file_path.read_text(encoding="utf-8", errors="replace")
        return [TextSegment(text=text)]
