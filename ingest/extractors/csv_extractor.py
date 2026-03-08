import csv
from pathlib import Path

from ingest.extractors.base import BaseExtractor, TextSegment

ROWS_PER_GROUP = 50


class CsvExtractor(BaseExtractor):
    @property
    def supported_extensions(self) -> list[str]:
        return ["csv"]

    def extract(self, file_path: Path) -> list[TextSegment]:
        segments = []
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if headers is None:
                return []

            rows: list[list[str]] = []
            row_start = 1
            for row_num, row in enumerate(reader, start=2):
                rows.append(row)
                if len(rows) >= ROWS_PER_GROUP:
                    text = self._format_rows(headers, rows)
                    segments.append(
                        TextSegment(
                            text=text,
                            metadata={"row_range": f"{row_start}-{row_start + len(rows) - 1}"},
                        )
                    )
                    row_start += len(rows)
                    rows = []

            if rows:
                text = self._format_rows(headers, rows)
                segments.append(
                    TextSegment(
                        text=text,
                        metadata={"row_range": f"{row_start}-{row_start + len(rows) - 1}"},
                    )
                )

        return segments

    @staticmethod
    def _format_rows(headers: list[str], rows: list[list[str]]) -> str:
        lines = [", ".join(headers)]
        for row in rows:
            lines.append(", ".join(row))
        return "\n".join(lines)
