from __future__ import annotations


from typing import Any, Optional


from pydantic import BaseModel, Field


class Certification(BaseModel):
    id: str
    name: str
    label: str

    image: Optional[str] = None
    text: Optional[str] = None



class Attribute(BaseModel):
    name: str
    attribute: str
    value: Any

    unit: Optional[str] = None



class Buttons(BaseModel):
    name: str
    type: str

    icon: Optional[str] = None
    url: Optional[str] = None


class Price(BaseModel):
    ondemand: bool
    string: str

    float: Optional[float] = None
    currency: Optional[str] = None



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

    operatingMode: Optional[str] = None
    parentId: Optional[str] = None
    type: str
    group: Optional[str] = None
    name: str
    image: list[str] = Field(default_factory=list)
    priority: Optional[str] = None



class RelationShop(BaseModel):
    id: str
    vendorId: str
    type: str

    group: Optional[str] = None



class RelationListResponse(BaseModel):

    meta: dict[str, Any]

    items: list[Relation]


class DocumentListResponse(BaseModel):

    meta: dict[str, Any]

    items: list[Document]


class CertificationListResponse(BaseModel):

    meta: dict[str, Any]

    items: list[Certification]


class ShopSku(BaseModel):
    id: str
    parentId: str
    vendorId: str
    name: str

    tagline: Optional[str] = None
    active: bool
    expired: bool
    approved: bool
    releaseDate: Optional[str] = None
    description: Optional[str] = None
    specificationText: Optional[str] = None
    price: Optional[Price] = None

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

    defaultOperatingModeId: Optional[str] = None
    successorsIds: list[str] = Field(default_factory=list)
    name: str
    shortName: Optional[str] = None
    description: Optional[str] = None
    specificationText: Optional[str] = None
    tagline: Optional[str] = None

    active: bool
    expired: bool
    approved: bool
    deleted: bool = False

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


class SkuListResponse(BaseModel):

    meta: dict[str, Any]

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
