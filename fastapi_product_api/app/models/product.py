from __future__ import annotations


from typing import Optional, Union


from pydantic import BaseModel, Field


class SkuValue(BaseModel):
    label: str
    value: Union[str, int]
    id: Optional[int] = None
    skus: list[str] = Field(default_factory=list)


class SkuOption(BaseModel):
    name: str

    unit: Optional[str] = None
    attribute: str
    type: Optional[str] = None
    values: list[SkuValue] = Field(default_factory=list)


class Product(BaseModel):
    id: int
    parentId: int
    oldExternalIds: list[str] = Field(default_factory=list)
    secondaryParents: list[str] = Field(default_factory=list)
    name: str
    shortName: str

    description: Optional[str] = None
    tagline: Optional[str] = None

    sort: int
    active: bool
    hidden: bool
    approved: bool

    releaseDate: Optional[str] = None
    importance: Optional[str] = None

    deleted: bool = False
    images: list[str] = Field(default_factory=list)
    skuOptions: list[SkuOption] = Field(default_factory=list)
    attributes: dict[str, object] = Field(default_factory=dict)


class ProductDocument(BaseModel):
    type: str
    name: str
    url: str
    mime: str
    viewable: bool


class ProductDocumentsResponse(BaseModel):
    meta: dict[str, object]
    items: list[ProductDocument]


class ProductListResponse(BaseModel):
    meta: dict[str, object]
    items: list[Product]


__all__ = [
    "Product",
    "ProductListResponse",
    "ProductDocument",
    "ProductDocumentsResponse",
    "SkuOption",
    "SkuValue",
]
