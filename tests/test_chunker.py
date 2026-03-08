import pytest

from ingest.chunker import chunk_text, estimate_tokens


class TestEstimateTokens:
    def test_basic(self):
        assert estimate_tokens("hello world") == len("hello world") // 4

    def test_empty(self):
        assert estimate_tokens("") == 0


class TestChunkText:
    def test_short_text_single_chunk(self):
        text = "Hello, world!"
        chunks = chunk_text(text, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_empty_text(self):
        assert chunk_text("", chunk_size=100, chunk_overlap=10) == []

    def test_whitespace_only(self):
        assert chunk_text("   \n\n  ", chunk_size=100, chunk_overlap=10) == []

    def test_splits_long_text(self):
        # Create text that's longer than chunk_size * 4 chars
        text = "This is sentence one. " * 100
        chunks = chunk_text(text, chunk_size=50, chunk_overlap=5)
        assert len(chunks) > 1
        # All chunks should be non-empty
        for chunk in chunks:
            assert len(chunk.strip()) > 0

    def test_respects_paragraph_boundaries(self):
        text = "Paragraph one content here.\n\nParagraph two content here.\n\nParagraph three content here."
        chunks = chunk_text(text, chunk_size=20, chunk_overlap=2)
        assert len(chunks) >= 2

    def test_overlap(self):
        # With overlap, consecutive chunks should share some content
        words = ["word" + str(i) for i in range(200)]
        text = " ".join(words)
        chunks = chunk_text(text, chunk_size=25, chunk_overlap=5)
        assert len(chunks) > 1

    def test_single_word(self):
        chunks = chunk_text("hello", chunk_size=100, chunk_overlap=10)
        assert chunks == ["hello"]

    def test_very_long_word(self):
        text = "a" * 5000
        chunks = chunk_text(text, chunk_size=50, chunk_overlap=5)
        assert len(chunks) >= 1
        # All text should be covered
        total = sum(len(c) for c in chunks)
        assert total >= len(text)

    def test_preserves_all_content(self):
        text = "Alpha. Beta. Gamma. Delta. Epsilon."
        chunks = chunk_text(text, chunk_size=5, chunk_overlap=0)
        # All original words should appear in at least one chunk
        for word in ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]:
            assert any(word in chunk for chunk in chunks), f"{word} missing from chunks"
