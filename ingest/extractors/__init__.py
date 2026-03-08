from ingest.extractors.base import TextSegment, extract_file
from ingest.extractors.txt_extractor import TxtExtractor
from ingest.extractors.markdown_extractor import MarkdownExtractor
from ingest.extractors.pdf_extractor import PdfExtractor
from ingest.extractors.docx_extractor import DocxExtractor
from ingest.extractors.csv_extractor import CsvExtractor
from ingest.extractors.json_extractor import JsonExtractor

EXTRACTORS = {
    "txt": TxtExtractor,
    "md": MarkdownExtractor,
    "pdf": PdfExtractor,
    "docx": DocxExtractor,
    "csv": CsvExtractor,
    "json": JsonExtractor,
}

__all__ = ["TextSegment", "extract_file", "EXTRACTORS"]
