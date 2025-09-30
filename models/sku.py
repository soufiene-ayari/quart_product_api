from pydantic import BaseModel
from typing import List, Optional, Dict, Union,Any

class Certification(BaseModel):
    id: str
    name: str
    label: str
    image: Optional[str] = None
    text: Optional[str] = None

class Attribute(BaseModel):
    name: str
    attribute: str
    value: Union[str, int, float, bool, List[Union[str, int, float, bool]]]
    unit: Optional[str] = None
    
class Buttons(BaseModel):
    name: str
    type: str
    icon: Optional[str] = None
    url: Optional[str] = None

class Price(BaseModel):
    ondemand: bool
    string: str
    float: Optional[float]
    currency: Optional[str] = None

class SectionContent(BaseModel):
    type: str  # e.g. "table", "image", "text"
    content: Any

class Section(BaseModel):
    name: str
    contents: List[SectionContent]

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
    image: List[str] = []
    priority: Optional[str] = None
class RelationShop(BaseModel):
    id: str
    vendorId: str
    type: str
    group: Optional[str] = None
    name: str

class RelationListResponse(BaseModel):
    meta: Dict[str, Union[int, str]]
    items: List[Relation]

class DocumentListResponse(BaseModel):
    meta: Dict[str, Union[int, str]]
    items: List[Document]

class CertificationListResponse(BaseModel):
    meta: Dict[str, Union[int, str]]
    items: List[Certification]

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
    images: List[str] = []
    attributes: dict = {}
    deleted: bool = False
    technicalParameters: dict = {}
    relations: List[RelationShop]


class Sku(BaseModel):
    id: str
    parentId: str
    vendorId: str
    maintenanceId:str
    defaultOperatingModeId: Optional[str] = None
    successorsIds:List[str] = []
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
    certifications: List[Certification] = []
    images: List[str] = []
    attributes: dict = {}
    buttons: List[Buttons] = []
    sections: List[Section] = []

class SkuListResponse(BaseModel):
    meta: Dict[str, Union[int, str]]
    items: List[Sku]
