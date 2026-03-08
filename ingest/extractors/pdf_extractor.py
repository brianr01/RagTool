from pathlib import Path

from pypdf import PdfReader

from ingest.extractors.base import BaseExtractor, TextSegment


class PdfExtractor(BaseExtractor):
    @property
    def supported_extensions(self) -> list[str]:
        return ["pdf"]

    def extract(self, file_path: Path) -> list[TextSegment]:
        reader = PdfReader(str(file_path))
        segments = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            if text.strip():
                segments.append(
                    TextSegment(text=text, metadata={"page_number": i + 1})
                )
        return segments

    @staticmethod
    def get_page_count(file_path: Path) -> int:
        reader = PdfReader(str(file_path))
        return len(reader.pages)
