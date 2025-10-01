from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Certification(BaseModel):
    id: str
    name: str
    label: str
    image: str | None = None
    text: str | None = None


class Attribute(BaseModel):
    name: str
    attribute: str
    value: Any
    unit: str | None = None


class Buttons(BaseModel):
    name: str
    type: str
    icon: str | None = None
    url: str | None = None


class Price(BaseModel):
    ondemand: bool
    string: str
    float: float | None = None
    currency: str | None = None


class SectionContent(BaseModel):
    type: str
    content: Any


class Section(BaseModel):
    name: str
    contents: list[SectionContent] = Field(default_factory=list)


class Document(BaseModel):
    type: str
    name: str
    url: str
    mime: str
    viewable: bool


class Relation(BaseModel):
    id: str
    vendorId: str
    operatingMode: str | None = None
    parentId: str | None = None
    type: str
    group: str | None = None
    name: str
    image: list[str] = Field(default_factory=list)
    priority: str | None = None


class RelationShop(BaseModel):
    id: str
    vendorId: str
    type: str
    group: str | None = None
    name: str


class RelationListResponse(BaseModel):
    meta: dict[str, int | str]
    items: list[Relation]


class DocumentListResponse(BaseModel):
    meta: dict[str, int | str]
    items: list[Document]


class CertificationListResponse(BaseModel):
    meta: dict[str, int | str]
    items: list[Certification]


class ShopSku(BaseModel):
    id: str
    parentId: str
    vendorId: str
    name: str
    tagline: str | None = None
    active: bool
    expired: bool
    approved: bool
    releaseDate: str | None = None
    description: str | None = None
    specificationText: str | None = None
    price: Price | None = None
    images: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    deleted: bool = False
    technicalParameters: dict[str, Any] = Field(default_factory=dict)
    relations: list[RelationShop] = Field(default_factory=list)


class Sku(BaseModel):
    id: str
    parentId: str
    vendorId: str
    maintenanceId: str
    defaultOperatingModeId: str | None = None
    successorsIds: list[str] = Field(default_factory=list)
    name: str
    shortName: str | None = None
    description: str | None = None
    specificationText: str | None = None
    tagline: str | None = None
    active: bool
    expired: bool
    approved: bool
    deleted: bool = False
    releaseDate: str | None = None
    selectionTool: bool | None = False
    designTool: bool | None = False
    magicadBim: bool | None = False
    sort: int | None = 0
    price: Price | None = None
    default: bool | None = False
    certifications: list[Certification] = Field(default_factory=list)
    images: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    buttons: list[Buttons] = Field(default_factory=list)
    sections: list[Section] = Field(default_factory=list)


class SkuListResponse(BaseModel):
    meta: dict[str, int | str]
    items: list[Sku]


__all__ = [
    "Attribute",
    "Buttons",
    "Certification",
    "CertificationListResponse",
    "Document",
    "DocumentListResponse",
    "Price",
    "Relation",
    "RelationListResponse",
    "RelationShop",
    "Section",
    "SectionContent",
    "ShopSku",
    "Sku",
    "SkuListResponse",
]
