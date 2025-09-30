from pydantic import BaseModel
from typing import List, Optional, Union

class SkuValue(BaseModel):
    label: str
    value: Union[str, int]
    id: Optional[int] = None
    skus: List[str]

class SkuOption(BaseModel):
    name: str
    unit: Optional[str] = None
    attribute: str
    type: Optional[str] = None
    values: List[SkuValue] = []

class Product(BaseModel):
    id: int
    parentId: int
    oldExternalIds: List[str] = []
    secondaryParents: List[str] = []
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
    images: List[str] = []
    skuOptions: List[SkuOption] = []
    attributes: dict = {}

class ProductDocument(BaseModel):
    """Represents a document associated with a product"""
    type: str  # e.g., 'brochures', 'manuals', etc.
    name: str  # Name of the document file
    url: str  # Full URL to download the document
    mime: str  # MIME type of the document (e.g., 'application/pdf')
    viewable: bool  # Whether the document can be viewed in browser


class ProductDocumentsResponse(BaseModel):
    """Response model for product documents endpoint"""
    meta: dict  # Metadata about the response
    items: List[ProductDocument]  # List of documents


class ProductListResponse(BaseModel):
    meta: dict
    items: List[Product]
