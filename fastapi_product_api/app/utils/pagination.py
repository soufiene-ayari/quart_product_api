from __future__ import annotations

from typing import Iterable, TypeVar

T = TypeVar("T")


def paginate(items: Iterable[T], page: int, page_size: int) -> list[T]:
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return list(items)[start:end]


__all__ = ["paginate"]
