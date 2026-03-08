from shared.config import settings

SEPARATORS = ["\n\n", "\n", ". ", " ", ""]


def estimate_tokens(text: str) -> int:
    return len(text) // 4


def chunk_text(
    text: str,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
) -> list[str]:
    if chunk_size is None:
        chunk_size = settings.chunk_size
    if chunk_overlap is None:
        chunk_overlap = settings.chunk_overlap

    # Convert token counts to approximate character counts
    max_chars = chunk_size * 4
    overlap_chars = chunk_overlap * 4

    if not text or not text.strip():
        return []

    return _recursive_split(text, max_chars, overlap_chars, SEPARATORS)


def _recursive_split(
    text: str,
    max_chars: int,
    overlap_chars: int,
    separators: list[str],
) -> list[str]:
    if len(text) <= max_chars:
        stripped = text.strip()
        return [stripped] if stripped else []

    separator = separators[-1]
    for sep in separators:
        if sep in text:
            separator = sep
            break

    parts = text.split(separator) if separator else list(text)

    chunks: list[str] = []
    current = ""

    for part in parts:
        candidate = current + separator + part if current else part

        if len(candidate) > max_chars and current:
            chunks.append(current.strip())
            # Keep overlap from end of current chunk
            if overlap_chars > 0 and len(current) > overlap_chars:
                current = current[-overlap_chars:] + separator + part
            else:
                current = part
        else:
            current = candidate

    if current.strip():
        chunks.append(current.strip())

    # If any chunk is still too large, split further with next separator
    if len(separators) > 1:
        final_chunks = []
        for chunk in chunks:
            if len(chunk) > max_chars:
                final_chunks.extend(
                    _recursive_split(chunk, max_chars, overlap_chars, separators[1:])
                )
            else:
                final_chunks.append(chunk)
        return final_chunks

    return chunks
