from typing import TypeVar

KT = TypeVar("KT", bound=(str | bytes))
VT = TypeVar("VT", bound=(str | bytes | int | float | dict | list | None))
