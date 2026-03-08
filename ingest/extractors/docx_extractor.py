from pathlib import Path

from docx import Document

from ingest.extractors.base import BaseExtractor, TextSegment


class DocxExtractor(BaseExtractor):
    @property
    def supported_extensions(self) -> list[str]:
        return ["docx"]

    def extract(self, file_path: Path) -> list[TextSegment]:
        doc = Document(str(file_path))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n\n".join(paragraphs)
        return [TextSegment(text=text)]
