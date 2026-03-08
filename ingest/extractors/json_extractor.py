import json
from pathlib import Path

from ingest.extractors.base import BaseExtractor, TextSegment


class JsonExtractor(BaseExtractor):
    @property
    def supported_extensions(self) -> list[str]:
        return ["json"]

    def extract(self, file_path: Path) -> list[TextSegment]:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)

        text = json.dumps(data, indent=2, ensure_ascii=False)
        return [TextSegment(text=text)]
