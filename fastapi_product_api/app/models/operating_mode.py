"""Pydantic schemas for operating mode resources."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from .sku import Buttons, Certification, Price, Section, SectionContent


class OperatingMode(BaseModel):
    id: str
    parentId: str
    vendorId: str
    name: str
    shortName: Optional[str] = ""
    description: Optional[str] = None
    specificationText: Optional[str] = None
    tagline: Optional[str] = None
    active: bool
    expired: bool
    approved: bool
    releaseDate: Optional[str] = None
    selectionTool: Optional[bool] = False
    designTool: Optional[bool] = False
    magicadBim: Optional[bool] = False
    sort: Optional[int] = 0
    price: Optional[Price] = None
    default: Optional[bool] = False
    certifications: list[Certification] = Field(default_factory=list)
    images: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    buttons: list[Buttons] = Field(default_factory=list)
    sections: list[Section] = Field(default_factory=list)
    skuId: str


class OperatingModeListMeta(BaseModel):
    offset: int
    limit: int
    total: int


class OperatingModeListResponse(BaseModel):
    meta: OperatingModeListMeta
    items: list[OperatingMode]


__all__ = [
    "OperatingMode",
    "OperatingModeListMeta",
    "OperatingModeListResponse",
    "Buttons",
    "Certification",
    "Price",
    "Section",
    "SectionContent",
]
