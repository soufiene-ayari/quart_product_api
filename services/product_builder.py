from sqlalchemy import false

from models.product import Product, SkuOption, SkuValue
from queries.product_queries import query_product_by_id,query_images,query_attributes,query_texts,query_sku_options_definitions, query_child_objects,query_child_objects_attributes,query_productNrs,query_secondaryParents
from typing import Optional, List, Dict, Union
import logging
import asyncio
import re
import json
from collections import defaultdict
from datetime import datetime, timezone
from utils.utilities import inject_fallback_sort
logger = logging.getLogger(__name__)

class ProductBuilder:
    def __init__(self, es_client):
        self.es = es_client

    async def build_product(self, identifier: str, lang: str, brand: str ) -> Optional[Product]:
        try:
            product = await self.get_product(identifier, lang, brand)
            if not product:
                return None
            # If product is deleted, return minimal Product with deleted=True
            if product.get('deleted'):
                return Product(
                    id=identifier,
                    parentId=product.get("parentHierarchy",None),
                    name=product.get("name",None),
                    shortName=product.get("name",None),
                    sort=0,
                    active=False,
                    hidden=False,
                    approved=False,
                    deleted=True
                )
            product_id= identifier
            refProd_id=await self.get_ref_id(product)
            if refProd_id:
                refProd= await self.get_product(refProd_id, lang, brand)
            else:
                refProd=None  
            identifiers = [i for i in [product_id, refProd_id] if i is not None]
            pgrs = [h["id"] for h in product["hierarchies"]]
            pgrs.append(product_id)
            pgrs.append(refProd_id)
            #get also the accessory attributes from above levels
            indices, attQuery = inject_fallback_sort(query_attributes(pgrs), lang, "systemair_ds_attributes_","attributeParentId")
            attributes_res = self.es.search(indices, attQuery)
            attributes = attributes_res.get("hits", {}).get("hits", [])
            #index = f"systemair_ds_attributes_{lang}"
            #attributes= list(self.es.getScrollObject(index, query_attributes(identifiers),10000,"1m"))

            get_texts_ids=await self.get_texts_ids(refProd,product)
            index=f"systemair_ds_elements_{lang}"
            texts_res= self.es.search(index, query_texts(get_texts_ids))  
            texts = texts_res.get("hits", {}).get("hits", [])            
            (
                parent_id,
                old_external_ids,
                name,
                short_name,
                description,
                tagline,
                sort,
                active,
                hidden,
                approved,
                release_date,
                importance,
                images,
                sku_options,
                attributes,
                secondaryParents
            ) = await asyncio.gather(
                self.get_parent_id(product),
                self.get_old_external_ids(attributes),
                self.get_name(product),
                self.get_short_name(attributes,product),
                self.get_description(texts),
                self.get_tagline(attributes),
                self.get_sort_order(product),
                self.get_active_status(refProd_id),
                self.get_hidden_status(attributes),
                self.get_approved_status(product,attributes),
                self.get_release_date(attributes),
                self.get_importance(product_id),
                self.get_images(refProd,product,lang),
                self.get_sku_options(refProd,product,lang),
                self.get_additional_attributes(attributes),
                self.get_secondary_parents(identifier,lang)
            )

            return Product(
                id=product_id,
                parentId=parent_id,
                oldExternalIds=old_external_ids,
                secondaryParents=secondaryParents,
                name=name,
                shortName=short_name,
                description=description,
                tagline=tagline,
                sort=sort,
                active=active,
                hidden=hidden,
                approved=approved,
                releaseDate=release_date,
                importance=importance,
                images=images,
                skuOptions=sku_options,
                attributes=attributes
            )
        except Exception as e:
            logger.exception(f"Error building product {identifier}: {str(e)}")
            return None
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
    async def get_product(self, identifier: str, lang: str, brand: str) -> Optional[dict]:
        index = f"systemair_ds_hierarchies_{lang}"
        try:
            response = self.es.search(index,query_product_by_id(identifier,brand))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return None
            
            return hits[0]["_source"]  # return the full product document
        except Exception as e:
            logger.exception(f"Failed to fetch product {identifier} from ES: {e}")
            return None

    async def get_parent_id(self, product: dict) -> int: return  product.get("parentHierarchy",None)
    
    async def get_old_external_ids(self, attributes: List[dict]) -> List[str]:
        try:
            for att in attributes:
                src = att.get("_source", {})
                if src.get("name") == "ecom-old-matching-ids-products":
                    return [str(val.get("value")) for val in src.get("values", []) if val.get("value") is not None]
            return []
        except Exception as e:
            logger.exception(f"Failed to fetch old external IDs: {e}")
            return []
        
        
    async def get_name(self, product: dict) -> str: return product.get("name",None)
    #menu-title-level-*
    async def get_short_name(self, attributes: List[dict], product: dict) -> Optional[str]:
        for att in attributes:
            name = att.get("_source", {}).get("name", "")
            if re.match(r"menu-title-level-.*", name):
                values = att["_source"].get("values", [])
                return str(values[0].get("value")) if values else product.get("name")
        return product.get("name")
    
    async def get_description(self,texts: List[dict]) -> Optional[str]: 
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") == 138 for cat in categories):
                return text["_source"].get("text")
        return None

    # take the tagline from attribute: M3-ITEM-DESCRIPTIO
    async def get_tagline(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "M3-ITEM-DESCRIPTION":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None
    async def get_sort_order(self, product: dict) -> int: return product.get("seqorderNr",0)
    async def get_active_status(self, product_id: str) -> bool:
        if product_id:
            return True
        return False
    #is hidden level*
    async def get_hidden_status(self, attributes: List[dict]) -> bool:
        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name", "")
            if re.match(r"is-hidden-level-.*", name):
                values = src.get("values", [])
                if values:
                    value = values[0].get("value")
                    return str(value).lower() in ["1", "true", "yes"]  # interprets as boolean
        return False
    # wf state 
    async def get_approved_status(self, product: dict,attributes: List[dict]) -> bool:
        release_date = None

        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "Release-date":
                values = src.get("values", [])
                if values:
                    date_str = str(values[0].get("value"))
                    try:
                        release_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
                    except Exception as e:
                        logger.warning(f"Invalid release date format: {date_str}")
                        release_date = None

        try:
            workflows = product.get("workflows", [])
            for wf in workflows:
                if wf.get("workflow") == "PRODUCTS_state" and wf.get("stateName") in ["WfState_14","WfState_15"]:
                    if release_date is None or release_date < datetime.now(timezone.utc):
                        return True
            return False
        except Exception as e:
            logger.exception(f"Failed to determine approved status: {e}")
            return False
    
    #Release-date from original
    async def get_release_date(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name", "")
            if name=="Release-date":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
    async def get_importance(self, product_id: str) -> Optional[str]: return None
    async def get_images(self, ref_product: dict, product: dict, lang: str) -> List[str]: 
        resolved_epim_ids = []
        if ref_product:
            for assignment in ref_product.get("imageAssignments"):
                for obj in assignment["objects"]:
                        resolved_epim_ids.append(obj["epimId"])
        if product:
            for assignment in product.get("imageAssignments"):
                for obj in assignment["objects"]:
                        resolved_epim_ids.append(obj["epimId"])
        #print(resolved_epim_ids)
        index = f"systemair_ds_elements_{lang}"
        res = []
        #print(query_images(resolved_epim_ids))
        try:
            response = self.es.search(index, query_images(resolved_epim_ids))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return []
            for hit in hits:
                res.append(hit["_source"].get("dsElementPreviewFile"))
            #print(res)
            return res
        except Exception as e:
            logger.exception(f"Failed to fetch images  {resolved_epim_ids} from ES: {e}")
            return []
        
    async def get_sku_options(self, ref_product: dict, product: dict, lang: str) -> List[SkuOption]: 
        
        #in case of  dict we take the dict id 
        # the order the same from the product table not the attribute in the views
        
        prodtables_ids=await self.get_prodtable_ids(ref_product,product)
        #print(prodtables_ids)
        index = f"systemair_ds_producttables_{lang}"
        res = []
        grouped = defaultdict(lambda: defaultdict(list))
        try:
            response = self.es.search(index, query_sku_options_definitions(prodtables_ids))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return []
            for hit in hits:
                table= hit["_source"].get("table", [])

                # Run the parser
                sku_option_definitions = await  self.parse_sku_option_definitions(table)
                
                unique_attributes = list(set(
                    attr for data in sku_option_definitions.values() for attr in data["attribute"]
                ))
                # for next week  should we get both in all cases or only when ref id exist
                childrens=await self.get_child_objects(product.get("epimId"),lang)
                skus=list(childrens.keys())
                #print(query_child_objects_attributes(unique_attributes,skus))
                
                productNrs=await self.get_productNrs(skus,lang)

                indices, attQuery = inject_fallback_sort(query_child_objects_attributes(unique_attributes,skus), lang,"systemair_ds_attributes_", "attributeParentId")
                attributes_res = self.es.search(indices, attQuery)
                att_response = attributes_res.get("hits", {}).get("hits", [])

                #att_index=f"systemair_ds_attributes_{lang}"
                #att_response = list(self.es.getScrollObject(att_index, query_child_objects_attributes(unique_attributes,skus),10000,"1m"))

                #for hit in att_response.get("hits", {}).get("hits", []):
                for hit in att_response:
                    src = hit["_source"]
                    attr = src.get("name")
                    datatype = src.get("datatype", "STRING")
                    if datatype == "DICTIONARY":
                        attr_id = src.get("values", [{}])[-1].get("dictId")
                    else:
                        attr_id = src.get("attributeId")
                    #attr_id = src.get("values", [{}])[-1].get("dictId")
                    parent_id = str(src.get("parentId"))
                    # stupid take the unit based on the LANGUAGE
                    unit = src.get("values", [{}])[-1].get("unit", "")
                    value = src.get("values", [{}])[-1].get("value")
            
                    if not all([attr, value]):
                        continue  # Skip incomplete entries
            
                    key = (attr, value)  # Group by attribute and value
                    productNr=productNrs.get(parent_id, {}).get('productNr')
                    #grouped[key]["skus"].append(parent_id)
                    grouped[key]["skus"].append(productNr)
                    grouped[key]["id"] = attr_id
                    grouped[key]["unit"] = unit
                    grouped[key]["datatype"] = datatype
            
                sku_options = []
                for (attribute, value), data in grouped.items():
                    label = attribute  # Default fallback
                
                    for entry in sku_option_definitions.values():
                        if attribute in entry["attribute"]:
                            label = entry["label"][0] if entry["label"] else attribute
                            break
                
                    sku_value = SkuValue(
                        label=str(value) + (f" {data['unit']}" if data.get("unit") else ""),
                        value=value,
                        id=data["id"],
                        skus=data["skus"]
                    )
                
                    existing = next((o for o in sku_options if o.attribute == attribute), None)
                    if existing:
                        existing.values.append(sku_value)
                    else:
                        sku_options.append(SkuOption(
                            name=label,
                            unit=data["unit"],
                            attribute=attribute,
                            type=data["datatype"].lower(),
                            values=[sku_value]
                        ))
                
                          
            return sku_options
        except Exception as e:
            logger.exception(f"Failed to fetch prodtables  {prodtables_ids} from ES: {e}")
            return []
        
    async def get_additional_attributes(self, attributes: List[dict]) -> Dict[str, Union[str, int]]:
        result = {}
        try:
            for att in attributes:
                src = att.get("_source", {})
                values = src.get("values", [])
                if values:
                    result[src.get("name")] = values[0].get("value")  # <-- "value" must be quoted!
            return result
        except Exception as e:
            logger.exception(f"Failed to fetch rest of the attributes: {e}")
            return result
        
        
        
    async def get_ref_id(self, product: dict)->int:        
        return product.get("referenceId", None)
    async def get_texts_ids(self, ref_product: dict, product: dict) -> List[int]: 
        resolved_epim_ids = []
        if ref_product:
            for assignment in ref_product.get("textAssignments"):
                for obj in assignment["objects"]:
                    if obj.get("isResolved"):
                        resolved_epim_ids.append(obj["epimId"])
        if product:                
            for assignment in product.get("textAssignments"):
                for obj in assignment["objects"]:
                    if obj.get("isResolved"):
                        resolved_epim_ids.append(obj["epimId"])
        return resolved_epim_ids
    
    async def get_prodtable_ids(self, ref_product: dict, product: dict) -> List[int]: 
        resolved_epim_ids = []
        if ref_product:
            for assignment in ref_product.get("productTableAssignments"):
                for obj in assignment["objects"]:
                    resolved_epim_ids.append(obj["epimId"])
        if product:
            for assignment in product.get("productTableAssignments"):
                for obj in assignment["objects"]:
                    resolved_epim_ids.append(obj["epimId"])
        return resolved_epim_ids
    
    async def parse_sku_option_definitions(self, table: list) -> Dict[int, Dict[str, list]]:
        """
        Collect label and attribute lists for each seqorderNr from all rows and cells.
        Groups them by seqorderNr and filters out entries with both fields empty.
        """
        definitions = {}
    
        for block in table:
            rows = block.get("rows", [])
            for row in rows:
                for cell in row.get("cells", []):
                    seq = cell.get("seqorderNr")
                    if seq is None:
                        continue
    
                    label_data = cell.get("label", [])
                    attribute_data = cell.get("attribute", [])
    
                    if seq not in definitions:
                        definitions[seq] = {
                            "label": [],
                            "attribute": []
                        }
    
                    # Accumulate non-empty lists
                    if label_data:                    
                        definitions[seq]["label"].append(label_data[0].get("content"))
                    if attribute_data:
                        definitions[seq]["attribute"].append(attribute_data[0].get("name"))
    
        # Remove empty entries
        cleaned_definitions = {
            seq: data for seq, data in definitions.items()
            if data["label"] or data["attribute"]
        }
        
        return cleaned_definitions
    
    async def get_child_objects(self, product_id: str, lang: str) -> Dict[str, Dict[str, str]]:
        index = f"systemair_ds_products_{lang}"
        result = {}
    
        try:
            response = list(self.es.getScrollObject(index, query_child_objects(product_id),10000,"1m"))
            #hits = response.get("hits", {}).get("hits", [])
    
            if not response:
                return {}
    
            for hit in response:
                src = hit.get("_source", {})
                reference_id = src.get("referenceId")
                if reference_id:
                    result[str(reference_id)] = {
                        "epimId": str(src.get("epimId", "")),
                        "productNr": str(src.get("productNr", ""))
                    }
            
            return result
    
        except Exception as e:
            logger.exception(f"Failed to fetch products child of {product_id} from ES: {e}")
            return {}
    
    async def get_productNrs(self, skus: List[str], lang: str) -> Dict[str, Dict[str, str]]:
        if not skus:
            return {}

        prod_index = f"systemair_ds_products_{lang}"
        result = {}
                  
        try:
            prod_response = list(self.es.getScrollObject(prod_index, query_productNrs(skus), 10000, "1m"))
                
            if not prod_response:
                return {}

            for hit in prod_response:
                src = hit.get("_source", {})
                epimId = src.get("epimId")
                if epimId:
                    result[str(epimId)] = {
                        "productNr": str(src.get("productNr", ""))
                    }

            #print(result)
            return result

        except Exception as e:
            logger.exception(f"Failed to fetch products productNrs for {skus} from ES: {e}")
            return {}
        