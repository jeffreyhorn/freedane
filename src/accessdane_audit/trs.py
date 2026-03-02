from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

DEFAULT_SPLIT_PARTS = ("NE", "NW", "SE", "SW")


@dataclass(frozen=True)
class TRSBlock:
    township: int
    range: int
    section: int
    subsection: str | None = None

    @property
    def trs_code(self) -> str:
        return f"{self.township:02d}/{self.range:02d}"

    def to_row(self) -> dict[str, str]:
        return {
            "trs_code": self.trs_code,
            "township": str(self.township),
            "range": str(self.range),
            "section": str(self.section),
            "subsection": self.subsection or "",
            "quarter": self.subsection or "",
            "quarter_quarter": "",
        }


def parse_trs_code(code: str) -> tuple[int, int]:
    parts = code.strip().split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid TRS code: {code}")
    return int(parts[0]), int(parts[1])


def enumerate_trs(
    trs_code: str,
    sections: Iterable[int],
    split_sections: Mapping[int, Sequence[str]] | None = None,
    default_parts: Sequence[str] = DEFAULT_SPLIT_PARTS,
) -> list[TRSBlock]:
    township, range_ = parse_trs_code(trs_code)
    blocks: list[TRSBlock] = []
    split_sections = split_sections or {}
    for section in sections:
        if section in split_sections:
            parts = split_sections[section] or default_parts
            for part in parts:
                blocks.append(
                    TRSBlock(
                        township=township,
                        range=range_,
                        section=section,
                        subsection=part,
                    )
                )
        else:
            blocks.append(TRSBlock(township=township, range=range_, section=section))
    return blocks
