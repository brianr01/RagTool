import pytest
from pathlib import Path

from ingest.extractors.txt_extractor import TxtExtractor
from ingest.extractors.markdown_extractor import MarkdownExtractor
from ingest.extractors.csv_extractor import CsvExtractor
from ingest.extractors.json_extractor import JsonExtractor
from ingest.extractors.base import extract_file, get_file_type


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / "fixtures"


class TestTxtExtractor:
    def test_extract(self, fixtures_dir):
        extractor = TxtExtractor()
        segments = extractor.extract(fixtures_dir / "sample.txt")
        assert len(segments) == 1
        assert "quick brown fox" in segments[0].text
        assert "vector databases" in segments[0].text.lower()

    def test_supported_extensions(self):
        assert "txt" in TxtExtractor().supported_extensions


class TestMarkdownExtractor:
    def test_extract(self, fixtures_dir):
        extractor = MarkdownExtractor()
        segments = extractor.extract(fixtures_dir / "sample.md")
        assert len(segments) == 1
        assert "# Sample Markdown Document" in segments[0].text
        assert "Introduction" in segments[0].text

    def test_supported_extensions(self):
        assert "md" in MarkdownExtractor().supported_extensions


class TestCsvExtractor:
    def test_extract(self, fixtures_dir):
        extractor = CsvExtractor()
        segments = extractor.extract(fixtures_dir / "sample.csv")
        assert len(segments) >= 1
        # Should contain header + data
        assert "name" in segments[0].text
        assert "Alice Smith" in segments[0].text

    def test_row_range_metadata(self, fixtures_dir):
        extractor = CsvExtractor()
        segments = extractor.extract(fixtures_dir / "sample.csv")
        assert "row_range" in segments[0].metadata


class TestJsonExtractor:
    def test_extract(self, fixtures_dir):
        extractor = JsonExtractor()
        segments = extractor.extract(fixtures_dir / "sample.json")
        assert len(segments) == 1
        assert "Introduction to RAG" in segments[0].text
        assert "Vector Databases" in segments[0].text


class TestPdfExtractor:
    def test_extract(self, fixtures_dir):
        pdf_path = fixtures_dir / "sample.pdf"
        if not pdf_path.exists():
            pytest.skip("PDF fixture not generated yet")
        from ingest.extractors.pdf_extractor import PdfExtractor
        extractor = PdfExtractor()
        segments = extractor.extract(pdf_path)
        assert len(segments) == 2
        assert "page_number" in segments[0].metadata
        assert segments[0].metadata["page_number"] == 1

    def test_page_count(self, fixtures_dir):
        pdf_path = fixtures_dir / "sample.pdf"
        if not pdf_path.exists():
            pytest.skip("PDF fixture not generated yet")
        from ingest.extractors.pdf_extractor import PdfExtractor
        assert PdfExtractor.get_page_count(pdf_path) == 2


class TestDocxExtractor:
    def test_extract(self, fixtures_dir):
        docx_path = fixtures_dir / "sample.docx"
        if not docx_path.exists():
            pytest.skip("DOCX fixture not generated yet")
        from ingest.extractors.docx_extractor import DocxExtractor
        extractor = DocxExtractor()
        segments = extractor.extract(docx_path)
        assert len(segments) == 1
        assert "Sample Document" in segments[0].text


class TestExtractFile:
    def test_extract_txt(self, fixtures_dir):
        segments = extract_file(fixtures_dir / "sample.txt")
        assert len(segments) >= 1

    def test_extract_unsupported(self, fixtures_dir, tmp_path):
        unsupported = tmp_path / "test.xyz"
        unsupported.write_text("test")
        with pytest.raises(ValueError, match="Unsupported"):
            extract_file(unsupported)

    def test_get_file_type(self):
        assert get_file_type(Path("test.pdf")) == "pdf"
        assert get_file_type(Path("test.TXT")) == "txt"
        assert get_file_type(Path("dir/file.md")) == "md"
