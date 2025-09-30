from models.category import Category
from queries.category_queries import (
    query_category_by_id,query_attributes,query_texts,query_images,query_secondaryParents,query_elements_attributes,
)
from utils.utilities import inject_fallback_sort
from typing import Optional, List, Dict, Union, Any
import logging
import asyncio
import json
import xml.etree.ElementTree as ET
import re
logger = logging.getLogger(__name__)

class CategoryBuilder:
    def __init__(self, es_client):
        self.es = es_client

    async def build_category(self, identifier: str, lang: str, brand: str) -> Optional[Category]:
        try:
            category = await self.get_category(identifier, lang, brand)
            if not category:
                return None
                
            # Get basic category data
            category_id = str(identifier)
            #index = f"systemair_ds_attributes_{lang}"
            pgrs = [h["id"] for h in category["hierarchies"]]
            pgrs.append(category_id)

            indices, attQuery = inject_fallback_sort(query_attributes(pgrs), lang, "systemair_ds_attributes_","attributeParentId")
            attributes_res = self.es.search(indices, attQuery)
            attributes = attributes_res.get("hits", {}).get("hits", [])

            #attributes = list(self.es.getScrollObject(index, query_attributes(pgrs), 10000, "1m"))
            get_texts_ids = await self.get_texts_ids(category, None)
            index = f"systemair_ds_elements_{lang}"
            texts_res = self.es.search(index, query_texts(get_texts_ids))
            texts = texts_res.get("hits", {}).get("hits", [])

            # Get all necessary data in parallel
            (
                parent_id,
                old_external_ids,
                name,
                description,
                tagline,
                sort,
                active,
                hidden,
                approved,
                release_date,
                type,
                importance,
                icon,
                attributes,
                secondaryParents
            ) = await asyncio.gather(
                self.get_parent_id(category),
                self.get_old_external_ids(attributes,category),
                self.get_name(category),
                self.get_description(texts),
                self.get_tagline(attributes),
                self.get_sort_order(category),
                self.get_active_status(category_id,attributes,category,lang),
                self.get_hidden_status(attributes,category,lang),
                self.get_approved_status(category),
                self.get_release_date(attributes),
                self.get_type(category),
                self.get_importance(category),
                self.get_icon(category, lang),
                self.get_attributes(attributes),
                self.get_secondary_parents(category_id,lang)
            )
            
            # Build and return the Category object
            return Category(
                id=category_id,
                parentId=str(parent_id) if parent_id else "",
                oldExternalIds=old_external_ids,
                name=name,
                description=description,
                tagline=tagline,
                sort=sort,
                active=active,
                hidden=hidden,
                approved=approved,
                releaseDate=release_date,
                type=type,
                importance=importance,
                icon=icon,
                attributes=attributes,
                secondaryParents=secondaryParents
            )
            
        except Exception as e:
            logger.exception(f"Failed to build category {identifier}: {str(e)}")
            return None

    async def get_category(self, identifier: str, lang: str, brand: str) -> Optional[dict]:
        index = f"systemair_ds_hierarchies_{lang}"  # Categories are typically stored in hierarchies index
        try:
            response = self.es.search(index, query_category_by_id(identifier))
            hits = response.get("hits", {}).get("hits", [])
            return hits[0]["_source"] if hits else None
        except Exception as e:
            logger.exception(f"Error fetching category {identifier}: {e}")
            return None

    async def get_type(self, category: dict) -> Optional[str]:
        planLevel=category.get("planningLevel")
        if planLevel:
            return "product-range"
        return "category"
    async def get_parent_id(self, category: dict) -> Optional[str]:
        return category.get("parentHierarchy")
    
    async def get_old_external_ids(self, attributes: List[dict],category: dict) -> List[str]:
        """
        Extract old external IDs from attributes
        
        Args:
            attributes: List of attribute documents from Elasticsearch
            
        Returns:
            List of old external IDs as strings
        """


        try:
            for att in attributes:
                src = att.get("_source", {})
                name = src.get("name", "")
                if category.get("planningLevel")=="Product Range":
                    if name == "ecom-old-matching-ids-level-range":
                        return [str(val.get("value")) for val in src.get("values", []) if val.get("value") is not None]
                else:
                    if name == str("ecom-old-matching-ids-level" + str(category["level"])):
                        return [str(val.get("value")) for val in src.get("values", []) if val.get("value") is not None]
            return []
        except Exception as e:
            logger.exception(f"Failed to fetch old external IDs: {e}")
            return []
    
    async def get_name(self, category: dict) -> Optional[str]:
        """Get the category name from the category document"""
        return category.get("name")

    async def get_description(self, texts: List[dict]) -> Optional[str]:
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") == 6 for cat in categories):
                #temp = await self.transform_xml_to_json(text["_source"].get("xmlText"))
                return text["_source"].get("xmlText")
                # return text["_source"].get("text")
        return None

    async def get_tagline(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "M3-ITEM-DESCRIPTION":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None
    
    async def get_sort_order(self, category: dict) -> int:
        """Get the sort order, defaulting to 0 if not specified"""
        return category.get("seqorderNr", 0)

    async def get_active_status(self, category_id: Union[str, int],attributes: List[dict],category: dict,lang: str) -> bool:
        # add the hidden active +hidden status true else false
        hidden=await self.get_hidden_status(attributes,category,lang)
        if category_id and not hidden:
            return True
        return False

    async def get_secondary_parents(self, category_id: Union[str, int], lang: str) -> List[str]:
        res = []
        index = f"systemair_ds_hierarchies_{lang}"
        try:
            response = self.es.search(index, query_secondaryParents(category_id))
            hits = response.get("hits", {}).get("hits", [])
            for hit in hits:
                parent = hit.get("_source", {}).get("parentHierarchy")
                if parent:
                    res.append(str(parent))
            return res
        except Exception as e:
            logger.exception(f"Error secondary_parents {category_id}: {e}")
            return []

    async def get_hidden_status(self, attributes: List[dict],category: dict,lang: str) -> bool:
        refId=category.get("referenceId")
        catId=category.get("epimId")

        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name", "")
            if  name == str("is-hidden-level-"+str(category["level"])):
                values = src.get("values", [])
                if values:
                    value = values[0].get("value")
                    return str(value).lower() in ["1", "true", "yes"]  # interprets as boolean

        return False

    async def get_approved_status(self, product: dict) -> bool:

        try:
            workflows = product.get("workflows", [])
            for wf in workflows:
                if wf.get("workflow") == "PRODUCTS_state" and wf.get("stateName") in ["WfState_14","WfState_15"]:
                    return True
            return False
        except Exception as e:
            logger.exception(f"Failed to determine approved status: {e}")
            return False

    async def get_release_date(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            name=src.get("name")
            if re.match(r"Release-date.*", name):
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None
    
    async def get_importance(self, category: dict) -> Optional[str]:
        """Get the importance level of the category"""
        return category.get("importance")
    
    async def get_icon(self, category: dict, lang: str) -> Optional[str]:
        planLevel = category.get("planningLevel")
        if planLevel=="Product Range":
            final_category = 121
        else:
            final_category = 39
        #39 for categories and 121 is for product range
        resolved_epim_ids = []

        if category:
            for assignment in category.get("imageAssignments"):
                for obj in assignment["objects"]:
                    if not obj.get("isResolved") and not obj.get("isInherited"):
                        resolved_epim_ids.append(obj["epimId"])
        #print(resolved_epim_ids)
        index = f"systemair_ds_elements_{lang}"
        res = {}
        vaiants_elements = []
        final_response = []
        #print(query_images(resolved_epim_ids))
        try:
            response = self.es.search(index, query_images(resolved_epim_ids))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return None
            for hit in hits:
                categories = hit.get("_source", {}).get("categories", [])
                if any(cat.get("id") == final_category for cat in categories):
                    epim_id = hit["_source"].get("epimId")
                    if epim_id not in res:
                        res[epim_id] = []
                    res[epim_id].append(hit["_source"].get("dsElementPreviewFile"))
                    vaiants_elements.append(hit["_source"].get("epimId"))
            # Return the first image URL if available, otherwise None
            att_index = f"systemair_ds_attributes_{lang}"
            att_response = list(self.es.getScrollObject(att_index, query_elements_attributes(vaiants_elements), 10000,"1m"))

            check_inactive_internal = {}
            for hit in att_response:
                src = hit["_source"]
                elementId=int(src.get("parentId"))
                values = src.get("values", [])
                value = values[0].get("value")

                if elementId not in check_inactive_internal:
                    check_inactive_internal[elementId] = {"inactive":True,"internal":True}
                if src.get("name") == "inactive":
                    check_inactive_internal[elementId]["inactive"] = bool(value)
                if src.get("name") == "internal":
                    check_inactive_internal[elementId]["internal"] = bool(value)
            for id in res:
                if id in check_inactive_internal and not (check_inactive_internal[id]["inactive"] )and not(check_inactive_internal[id]["internal"]):
                    final_response.append(res[id][0])
            return final_response[0] if final_response else None
        except Exception as e:
            logger.exception(f"Failed to fetch images {resolved_epim_ids} from ES: {e}")
            return None
    
    async def get_attributes(self, attributes: List[dict]) -> dict:
        # make sure to process multi value attributes
        # attribute from the above levels with name accessory it is a flag
        result = {}
        try:
            for att in attributes:
                src = att.get("_source", {})
                values = src.get("values", [])
                if values:
                    val=values[0].get("value")
                    if src.get("datatype") == "FLAG":
                        val=bool(val)
                    result[src.get("name").upper()] = val  # <-- "value" must be quoted!

            return result
        except Exception as e:
            logger.exception(f"Failed to fetch rest of the attributes: {e}")
            return result


    async def get_texts_ids(self, ref_object: Optional[dict], object: Optional[dict]) -> List[int]:
        resolved_epim_ids = []
        for obj in (ref_object or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])
        for obj in (object or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])
        return resolved_epim_ids

    async def transform_xml_to_json(self,xml_text: str,tag_map: Optional[Dict[str, str]] = None) -> str:
        r"""
        Parse <FT>...</FT> fragments in xml_text, applying inline formatting tags, and
        return a single escaped JSON array literal string.

        The output is suitable for direct embedding, e.g.:
        "[{\"format\":\"paragraph\",...}]"

        Args:
            xml_text: Raw XML containing one or more <FT> elements.
            tag_map: Optional dict mapping XML tags (e.g. 'b','i','u') to JSON format keys.
                     Defaults to {'b': 'bold', 'i': 'italic', 'u': 'underline'}.

        Returns:
            A JSON-quoted string literal of the array, with all internal quotes and
            backslashes properly escaped.
        """
        # Default formatting map
        if tag_map is None:
            tag_map = {'b': 'bold', 'i': 'italic', 'u': 'underline', 'bl': 'bullet'}

        # Wrap xml_text for valid parsing
        fragment = f"<root>{xml_text}</root>"
        try:
            root = ET.fromstring(fragment)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML input: {e}")

        paragraphs: List[Dict[str, Any]] = []
        for ft in root.findall('FT'):
            parts: List[Any] = []
            # Leading text
            lead = ft.text or ''
            if lead.strip():
                parts.append(lead)
            # Inline tags
            for el in ft:
                fmt_key = tag_map.get(el.tag, el.tag)
                parts.append({'format': fmt_key, 'value': [el.text or '']})
                tail = el.tail or ''
                if tail:
                    parts.append(tail)
            # Empty FT fallback
            if not parts:
                parts = ['&nbsp;']
            paragraphs.append({'format': 'paragraph', 'value': parts})

        # Dump compact JSON array
        compact = json.dumps(paragraphs, ensure_ascii=False, separators=(',', ':'))
        return compact
        # Return as a JSON-quoted string literal
        return json.dumps(compact, ensure_ascii=False)