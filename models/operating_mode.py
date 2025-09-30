from pydantic import BaseModel
from typing import List, Optional, Dict, Union

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
    float: Optional[float] = None
    currency: Optional[str] = None

class SectionContent(BaseModel):
    type: str  # e.g. "table", "image", "text"
    content: Union[str, List[dict]]

class Section(BaseModel):
    section: str
    name: str
    contents: List[SectionContent]

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
    certifications: List[Certification] = []
    images: List[str] = []
    attributes: dict = {}
    buttons: List[Buttons] = []
    sections: List[Section] = []
    skuId: str
class OperatingModeListResponse(BaseModel):
    meta: Dict[str, Union[int, str]]
    items: List[OperatingMode]