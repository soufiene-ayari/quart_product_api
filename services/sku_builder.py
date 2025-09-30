import logging
from typing import Optional, List, Dict, Union

from models.sku import RelationShop
from models.sku import Sku, Certification, Attribute, Section, Price, Buttons, Relation, Document, SectionContent

from queries.sku_queries import (query_sku_by_id, query_attributes, query_texts, query_images,
                                 query_price, query_default_operating_mode, query_attr_definitions,
                                 query_sku_attributes,
                                 query_certifications, query_cert_definitions, query_image_byId, query_attr_buttons,
                                 query_sku_relations,
                                 query_documents, query_shop_attr_definitions, query_attr_TP_definitions,
                                 query_elements_attributes, query_uom)
from utils.mapping import map_brand
from utils.utilities import parse_piped_value, inject_fallback_sort
from services.elasticsearch_service import ESConnection
from services.database_service import DBConnection
import asyncio
import re
import json
import locale
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)


class SkuBuilder:
    def __init__(self, es_client: ESConnection, db_client: DBConnection):
        self.es = es_client
        self.db = db_client

    async def build_sku(self, identifier: str, lang: str, brand: str, market: str) -> Optional[Sku]:
        try:
            sku = await self.get_sku(identifier, lang, brand)
            if not sku:
                return None
            if sku.get("deleted"):
                return Sku(
                    id=identifier,
                    parentId=sku.get("parentHierarchy", ""),
                    vendorId="",
                    name=sku.get("name", ""),
                    active=False,
                    expired=True,
                    approved=False,
                    deleted=True,
                    attributes={}
                )

            sku_id = identifier
            refSku_id = await self.get_ref_id(sku)
            refSku = await self.get_sku(refSku_id, lang, None) if refSku_id else None
            identifiers = [i for i in [sku_id, refSku_id] if i is not None]
            # index = f"systemair_ds_attributes_{lang}"
            # raw_attributes  = list(self.es.getScrollObject(index, query_attributes(identifiers), 10000, "1m"))
            indices, attQuery = inject_fallback_sort(query_attributes(identifiers), lang, "systemair_ds_attributes_",
                                                     "attributeParentId")
            attributes_res = await self.es.asearch(indices, attQuery)

            #attributes_task = asyncio.create_task(asyncio.to_thread(self.es.search, indices, attQuery))
            #texts_ids_task = asyncio.create_task(self.get_texts_ids(refSku, sku))
            #attributes_res, get_texts_ids = await asyncio.gather(attributes_task, texts_ids_task)

            raw_attributes = attributes_res.get("hits", {}).get("hits", [])
            # print(attributes)
            get_texts_ids = await self.get_texts_ids(refSku, sku)
            index = f"systemair_ds_elements_{lang}"
            texts_res = await self.es.asearch(index, query_texts(get_texts_ids))

            #texts_res = await asyncio.to_thread(self.es.search, index, query_texts(get_texts_ids))

            texts = texts_res.get("hits", {}).get("hits", [])

            (
                parent_id,
                price,
                certifications,
                parsed_attributes,
                sections,
                expired,
                vendor_id,
                default_mode_id,
                name,
                short_name,
                description,
                specification,
                tagline,
                active,
                release_date,
                approved,
                sort,
                images,
                buttons,
                magic_ad_bim,
                selection_tool,
                successorsIds

            ) = await asyncio.gather(
                # get the parent id as the ref id not the pgpr id  (check it first)
                self.get_parent_id(sku),
                self.parse_price_async(sku, refSku, market),
                self.parse_certifications_async(identifiers, lang),
                self.get_additional_attributes(sku, refSku, lang, brand, market),
                # self.parse_sections_async(sku.get("sections", [])),
                self.get_technical_sections(sku, refSku, lang, brand, market),
                self.get_expired_status(raw_attributes, market),
                # we get productNr from the source decesion: should we priortize the original or source (take it from the original)
                self.get_vendor_id(sku, refSku),
                self.get_default_operating_mode_id(sku, refSku, lang),
                self.get_name(raw_attributes, sku),
                self.get_short_name(raw_attributes, sku),
                self.get_description(texts, "xmlText"),
                self.get_specification(texts, "xmlText"),
                self.get_tagline(raw_attributes),
                self.get_active_status(refSku_id, market, raw_attributes),
                self.get_release_date(raw_attributes),
                self.get_approved_status(sku, raw_attributes),
                self.get_sort_order(sku),
                self.get_images(refSku, sku, lang),
                self.get_buttons(identifiers, lang),
                self.get_magicadBim(raw_attributes),
                self.get_selectionTool(raw_attributes, brand),
                self.get_successors_ids(sku, refSku, lang, brand, market)
            )

            return Sku(
                id=str(sku_id),
                parentId=parent_id,
                vendorId=vendor_id,
                maintenanceId=str(refSku_id),
                defaultOperatingModeId=default_mode_id,
                successorsIds=successorsIds,
                name=name,
                shortName=short_name,
                description=description,
                specificationText=specification,
                tagline=tagline,
                active=active,
                expired=expired,
                approved=approved,
                releaseDate=release_date,
                selectionTool=selection_tool,
                designTool=sku.get("designTool", False),
                magicadBim=magic_ad_bim,
                sort=sort,
                price=price,
                default=sku.get("default", False),
                certifications=certifications,
                images=images,
                attributes=parsed_attributes,
                sections=sections,
                buttons=buttons
            )

        except Exception as e:
            logger.exception(f"Failed to build SKU {identifier}: {str(e)}")
            return None

    async def build_shop_sku(self, identifier: str, lang: str, brand: str, market: str) -> Optional[dict]:
        """
        Build only the minimal SKU fields needed for the shop view endpoint for performance.
        Returns a dict with only the required fields or None if not found.
        """
        try:
            sku = await self.get_sku(identifier, lang, brand)
            if not sku:
                return None
            if sku.get("deleted"):
                return Sku(
                    id=identifier,
                    parentId=sku.get("parentHierarchy", ""),
                    vendorId="",
                    name=sku.get("name", ""),
                    active=False,
                    expired=True,
                    approved=False,
                    deleted=True,
                    attributes={}
                )

            sku_id = identifier
            refSku_id = await self.get_ref_id(sku)
            refSku = await self.get_sku(refSku_id, lang, None) if refSku_id else None
            identifiers = [i for i in [sku_id, refSku_id] if i is not None]
            # index = f"systemair_ds_attributes_{lang}"
            # raw_attributes = list(self.es.getScrollObject(index, query_attributes(identifiers), 10000, "1m"))
            indices, attQuery = inject_fallback_sort(query_attributes(identifiers), lang, "systemair_ds_attributes_",
                                                     "attributeParentId")
            attributes_res = await self.es.asearch(indices, attQuery)
            raw_attributes = attributes_res.get("hits", {}).get("hits", [])

            # print(attributes)
            get_texts_ids = await self.get_texts_ids(refSku, sku)
            index = f"systemair_ds_elements_{lang}"
            texts_res = await self.es.asearch(index, query_texts(get_texts_ids))
            texts = texts_res.get("hits", {}).get("hits", [])

            (
                parent_id,
                vendor_id,
                default_mode_id,
                name,
                tagline,
                active,
                expired,
                approved,
                release_date,
                description,
                specification,
                price,
                parsed_attributes,
                images,
                technicalParameters,
                relations

            ) = await asyncio.gather(
                # get the parent id as the ref id not the pgpr id  (check it first)
                self.get_parent_id(sku),
                self.get_vendor_id(sku, refSku),
                self.get_default_operating_mode_id(sku, refSku, lang),
                self.get_name(raw_attributes, sku),
                self.get_tagline(raw_attributes),
                self.get_active_status(refSku_id, market, raw_attributes),
                self.get_expired_status(raw_attributes, market),
                self.get_approved_status(sku, raw_attributes),
                self.get_release_date(raw_attributes),
                self.get_description(texts),
                self.get_specification(texts),
                self.parse_price_async(sku, refSku, market),
                self.get_shop_additional_attributes(sku, refSku, lang, brand, market),
                self.get_images_shops(refSku, sku, lang),
                self.get_technical_parameters_shops(sku, refSku, lang, brand, market),
                self.get_shop_relations(sku, refSku, lang, brand, market)

            )
            from models.sku import ShopSku
            return ShopSku(
                id=str(sku_id),
                parentId=parent_id,
                vendorId=vendor_id,
                defaultOperatingModeId=default_mode_id,
                name=name,
                tagline=tagline,
                active=active,
                expired=expired,
                approved=approved,
                releaseDate=release_date,
                selectionTool=sku.get("selectionTool", False),
                designTool=sku.get("designTool", False),
                description=description,
                specificationText=specification,
                price=price,
                attributes=parsed_attributes,
                images=images,
                technicalParameters=technicalParameters,
                relations=relations

            )

        except Exception as e:
            logger.exception(f"Failed to build SHOP SKU {identifier}: {str(e)}")
            return None

    async def get_technical_parameters_shops(self, sku: dict, refSku: dict, lang: str, brand: str, market: str) -> \
    Optional[dict]:
        # technical parameters
        mapped_brand = map_brand(brand)
        variants = await self.get_default_operating_mode_id(sku, refSku, lang)
        identifiers = [i for i in [sku.get("epimId"), refSku.get("epimId"), variants] if i is not None]

        prodtables_ids = await self.get_prodtable_ids(refSku, sku)
        index = f"systemair_ds_producttables_{lang}"

        response = await self.es.asearch(index, query_attr_TP_definitions(prodtables_ids, mapped_brand))
        hits = response.get("hits", {}).get("hits", [])
        techs = {}
        final_techs = {}

        for hit in hits:
            table = hit["_source"].get("table", [])
            att_definitions = await self.parse_att_definitions(table, market)

            # att_definitions is already in the ascending order you want
            for seq, att in att_definitions.items():
                if att["attribute"][0] in ["dummy-tab", "dummy-tab-td"] and att["shortcut"][
                    0] == "Technical-parameters":
                    sec = att["label"][0]
                    techs[sec] = []
                elif att["attribute"][0] in ["dummy-tab", "dummy-tab-td"]:
                    break
                else:
                    techs[sec].append(att)

        for secName, tech in techs.items():

            # -------- ORDER-PRESERVING de-dupe (replaces set(...)) --------
            seen = set()
            unique_attributes = []
            for data in tech:
                for attr in data["attribute"]:
                    if attr not in seen:
                        seen.add(attr)
                        unique_attributes.append(attr)
            # ----------------------------------------------------------------

            # att_index = f"systemair_ds_attributes_{lang}"
            # att_response = list(self.es.getScrollObject(att_index,query_sku_attributes(unique_attributes, identifiers),10000,"1m"))
            indices, attQuery = inject_fallback_sort(query_sku_attributes(unique_attributes, identifiers), lang,
                                                     "systemair_ds_attributes_",
                                                     "attributeParentId")
            attributes_res = await self.es.asearch(indices, attQuery)
            att_response = attributes_res.get("hits", {}).get("hits", [])
            # att_response = list(self.es.getScrollObject(indices, attQuery, 10000, "1m"))

            rows = {}
            sub_section = ""
            for entry in tech:
                attrs = entry["attribute"]  # the list of attribute keys for this tech-group
                label_txt = entry["label"][0]  # the display label for this group

                if attrs == "dummy-tab":
                    continue

                # 1) Dummy-header rows
                if "dummy-table-header" in attrs or "dummy-table-header-td" in attrs:
                    sub_section = label_txt
                    continue

                # 2) For each real attribute key in this group, find its hit
                for hit in att_response:
                    src = hit["_source"]
                    name = src.get("name")
                    if name not in attrs:
                        continue

                    # extract unit + value(s)
                    vals = src.get("values", [])
                    if not vals:
                        continue

                    unit = (
                        next(
                            (u["unitShortName"]
                             for v in vals
                             for u in v.get("unitList", [])
                             if u["langIso"] == lang),
                            vals[0].get("unit", "") if vals else ""
                        )
                    )
                    if len(vals) == 1:
                        value = vals[0].get("value")
                    else:
                        # value = [v["value"] for v in vals if v.get("value") is not None]
                        # value = [v["value"] for v in sorted((item for item in vals if item["value"] is not None), key=lambda x: x.get("seqorderNr", 0), reverse=True)]
                        value = [
                            d["value"]
                            for d in sorted(
                                (d for d in vals if d.get("value") is not None),
                                key=lambda d: (d.get("seqorderNr") is None, d.get("seqorderNr") or 0)
                            )
                        ]
                    if value is None:
                        continue  # skip empties
                    value, unit2 = parse_piped_value(value)
                    if unit2:
                        unit = unit2

                    # Initialize the subsection if it doesn't exist
                    if sub_section not in rows:
                        rows[sub_section] = []
                    # build the row
                    rows[sub_section].append({label_txt: {"value": value, "unit": unit}})
                    # once matched, break out to the next entry
                    break

            if secName not in final_techs:
                final_techs[secName] = []
                final_techs[secName].append(rows)

            # keep the early return to preserve original behavior exactly
        return final_techs

    async def get_technical_sections(self, sku: dict, refSku: dict, lang: str, brand: str, market: str) -> List[
        Section]:
        # technical parameters
        mapped_brand = map_brand(brand)
        variants = await self.get_default_operating_mode_id(sku, refSku, lang)
        identifiers = [i for i in [sku.get("epimId"), refSku.get("epimId"), variants] if i is not None]

        prodtables_ids = await self.get_prodtable_ids(refSku, sku)
        index = f"systemair_ds_producttables_{lang}"

        response = await self.es.asearch(index, query_attr_TP_definitions(prodtables_ids, mapped_brand))
        hits = response.get("hits", {}).get("hits", [])
        techs = {}
        final_techs = {}

        for hit in hits:
            table = hit["_source"].get("table", [])
            att_definitions = await self.parse_att_definitions(table, market)

            # att_definitions is already in the ascending order you want
            for seq, att in att_definitions.items():
                if att["attribute"][0] in ["dummy-tab", "dummy-tab-td"] and not (
                        att["label"][0] == "Energy class label"):
                    sec = att["label"][0]
                    techs[sec] = []
                elif att["attribute"][0] in ["dummy-tab", "dummy-tab-td"]:
                    break
                else:
                    techs[sec].append(att)

        for secName, tech in techs.items():

            # -------- ORDER-PRESERVING de-dupe (replaces set(...)) --------
            seen = set()
            unique_attributes = []
            for data in tech:
                for attr in data["attribute"]:
                    if attr not in seen:
                        seen.add(attr)
                        unique_attributes.append(attr)
            # ----------------------------------------------------------------

            # att_index = f"systemair_ds_attributes_{lang}"
            # att_response = list(self.es.getScrollObject(att_index,query_sku_attributes(unique_attributes, identifiers),10000,"1m"))
            indices, attQuery = inject_fallback_sort(query_sku_attributes(unique_attributes, identifiers), lang,
                                                     "systemair_ds_attributes_",
                                                     "attributeParentId")
            attributes_res = await self.es.asearch(indices, attQuery)
            att_response = attributes_res.get("hits", {}).get("hits", [])
            # att_response = list(self.es.getScrollObject(indices, attQuery, 10000, "1m"))

            rows = {}
            sub_section = ""
            for entry in tech:
                attrs = entry["attribute"]  # the list of attribute keys for this tech-group
                label_txt = entry["label"][0]  # the display label for this group

                if attrs == "dummy-tab":
                    continue

                # 1) Dummy-header rows
                if "dummy-table-header" in attrs or "dummy-table-header-td" in attrs:
                    sub_section = label_txt
                    continue

                # 2) For each real attribute key in this group, find its hit
                for hit in att_response:
                    src = hit["_source"]
                    name = src.get("name")
                    if name not in attrs:
                        continue

                    # extract unit + value(s)
                    vals = src.get("values", [])
                    if not vals:
                        continue

                    unit = (
                        next(
                            (u["unitShortName"]
                             for v in vals
                             for u in v.get("unitList", [])
                             if u["langIso"] == lang),
                            vals[0].get("unit", "") if vals else ""
                        )
                    )
                    if len(vals) == 1:
                        value = vals[0].get("value")
                    else:
                        # value = [v["value"] for v in vals if v.get("value") is not None]
                        # value = [v["value"] for v in sorted((item for item in vals if item["value"] is not None), key=lambda x: x.get("seqorderNr", 0), reverse=True)]
                        value = [
                            d["value"]
                            for d in sorted(
                                (d for d in vals if d.get("value") is not None),
                                key=lambda d: (d.get("seqorderNr") is None, d.get("seqorderNr") or 0)
                            )
                        ]
                    if value is None:
                        continue  # skip empties
                    value, unit2 = parse_piped_value(value)
                    if unit2:
                        unit = unit2

                    # Initialize the subsection if it doesn't exist
                    if sub_section not in rows:
                        rows[sub_section] = []
                    # build the row
                    rows[sub_section].append({label_txt: {"value": value, "unit": unit}})
                    # once matched, break out to the next entry
                    break

            if secName not in final_techs:
                final_techs[secName] = []
                final_techs[secName].append(rows)

            # keep the early return to preserve original behavior exactly
        sections: List[Section] = []
        for section_name, section in final_techs.items():
            contents: List[SectionContent] = []
            for sub_section in section:
                contents.append(SectionContent(type="attributes", content=sub_section))
            sections.append(Section(name=section_name, contents=contents))

        return sections

    async def get_sku(self, identifier: str, lang: str, brand: str) -> Optional[dict]:
        index = f"systemair_ds_products_{lang}"
        try:
            response = await self.es.asearch(index, query_sku_by_id(identifier, brand))
            hits = response.get("hits", {}).get("hits", [])
            return hits[0]["_source"] if hits else None
        except Exception as e:
            logger.exception(f"Error fetching SKU {identifier}: {e}")
            return None

    async def get_selectionTool(self, attributes: List[dict], brand: str) -> bool:
        for att in attributes:
            name = att.get("_source", {}).get("name", "")
            if re.match(rf"export-promaster-{brand}.*-selector", name):
                values = att["_source"].get("values", [])
                return str(values[0].get("value")) if values else False
        return False

    async def get_magicadBim(self, attributes: List[dict]) -> bool:

        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name", "")
            if name == "BIM-enabled":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
                break

        # no exact match found:
        return False

    async def get_ref_id(self, sku: dict) -> Union[str, int, None]:
        return sku.get("referenceId", None)

    async def get_parent_id(self, sku: dict) -> Union[str, int, None]:
        return sku.get("parentHierarchy")

    async def get_vendor_id(self, sku: dict, refSku: dict) -> Union[str, int, None]:
        if refSku:
            return refSku.get("productNr")
        return sku.get("productNr")

    async def get_default_operating_mode_id(self, sku: dict, refsku: dict, lang: str) -> Optional[str]:
        """
        Looks up the defaultOperatingModeId for a SKU by querying
        the attributes index for any of its variantAssignments.
        Returns the first parentId found, or None.
        """
        # 1) collect all variant IDs
        variant_ids = []
        for assign in sku.get("variantAssignments", []):
            for obj in assign.get("objects", []):
                variant_ids.append(obj["epimId"])

        for assign in refsku.get("variantAssignments", []):
            for obj in assign.get("objects", []):
                variant_ids.append(obj["epimId"])

        # nothing to look up
        if not variant_ids:
            return None

        index = f"systemair_ds_attributes_{lang}"
        try:
            indices, attQuery = inject_fallback_sort(query_default_operating_mode(variant_ids), lang,
                                                     "systemair_ds_attributes_",
                                                     "attributeParentId")
            attributes_res = await self.es.asearch(indices, attQuery)
            hits = attributes_res.get("hits", {}).get("hits", [])

            # resp = await self.es.asearch(index, query_default_operating_mode(variant_ids))
            # hits = resp.get("hits", {}).get("hits", [])
            if not hits:
                return None

            parent_id = hits[0]["_source"].get("parentId")
            return str(parent_id) if parent_id is not None else None

        except Exception as e:
            logger.exception(
                f"Failed to lookup default operating mode for variants {variant_ids}: {e}"
            )
            return None

    async def get_name(self, attributes: List[dict], sku: dict) -> Optional[str]:
        """
        Look for an attribute whose name exactly equals "M3-ITEM-NAME"
        and return its first value; otherwise fall back to sku["name"].
        """
        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name", "")
            if name == "M3-ITEM-NAME":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
                break

        # no exact match found:
        return sku.get("name")

    async def get_short_name(self, attributes: List[dict], sku: dict) -> Optional[str]:
        for att in attributes:
            name = att.get("_source", {}).get("name", "")
            if re.match(r"M3-ITEM-NAME.*", name):
                values = att["_source"].get("values", [])
                return str(values[0].get("value")) if values else sku.get("name")
        return sku.get("name")

    async def get_texts_ids(self, ref_sku: Optional[dict], sku: Optional[dict]) -> List[int]:
        resolved_epim_ids = []
        for obj in (ref_sku or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])
        for obj in (sku or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])
        return resolved_epim_ids

    async def get_description(self, texts: List[dict], field: str = "text") -> Optional[str]:
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") == 6 for cat in categories):
                return text.get("_source", {}).get(field)
        return None

    async def get_tagline(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "M3-ITEM-DESCRIPTION":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None

    async def get_specification(self, texts: List[dict], field: str = "text") -> Optional[str]:
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") == 7 for cat in categories):
                return text.get("_source", {}).get(field)
        return None

    async def get_sort_order(self, sku: dict) -> int:
        return sku.get("seqorderNr", 0)

    async def get_active_status(self, sku_id: Union[str, int], market: str, attributes: List[dict]) -> bool:
        market_corrected = market.lower().replace("_", "-")
        market_flag = None
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == market_corrected:
                values = src.get("values", [])
                if values:
                    market_flag = values[0].get("value")
        if sku_id and market_flag:
            return True
        return False

    async def get_expired_status(self, attributes: List[dict], market: str) -> bool:
        marketCorrected = market.lower().replace("_", "-")
        expired_market_flag = None
        m3_status = 0
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == str(marketCorrected + "-expired"):
                values = src.get("values", [])
                if values:
                    # return values[0].get("value")
                    expired_market_flag = values[0].get("value")
            elif src.get("name") == "M3-ITEM-STATUS":
                values = src.get("values", [])
                if values:
                    m3_status = values[0].get("value")
        if expired_market_flag or int(m3_status) > 50:
            return True
        return False

    # check None and null
    async def get_approved_status(self, sku: dict, attributes: List[dict]) -> bool:
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
            workflows = sku.get("workflows", [])
            for wf in workflows:
                if wf.get("workflow") == "SKU_STATE" and wf.get("stateName") in ["WfState_42", "WfState_44"]:
                    if release_date is None or release_date < datetime.now(timezone.utc):
                        return True
            return False
        except Exception as e:
            logger.exception(f"Failed to determine approved status: {e}")
            return False

    async def get_release_date(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "Release-date":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None

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

    async def parse_att_definitions(self, table: list, market) -> Dict[int, Dict[str, list]]:
        """
        Collect label and attribute lists for each seqorderNr from all rows and cells.
        Groups them by seqorderNr and filters out entries with both fields empty.
        """
        market_id = market[-3:]
        definitions = {}
        rows = await self.db.aexecute_query(query_uom(), {"division": market_id})
        mapping = {
            d["BASE_ATTRIBUTE"]: d["CONVERTED_ATTRIBUTE"]
            for d in rows
            if "BASE_ATTRIBUTE" in d and "CONVERTED_ATTRIBUTE" in d
        }

        for block in table:
            rows = block.get("rows", [])
            for row in rows:
                for cell in row.get("cells", []):
                    seq = cell.get("seqorderNr")
                    # seq = str(cell.get("seqorderNr")).strip()
                    if seq is None:
                        continue

                    label_data = cell.get("label", [])
                    attribute_data = cell.get("attribute", [])

                    if seq not in definitions:
                        definitions[seq] = {
                            "label": [],
                            "attribute": [],
                            "shortcut": []
                        }

                    # Accumulate non-empty lists
                    if label_data:
                        definitions[seq]["label"].append(label_data[0].get("content"))
                        definitions[seq]["shortcut"].append(label_data[0].get("dictShortcut"))
                    if attribute_data:
                        att_name = attribute_data[0].get("name")
                        if att_name in mapping:
                            att = mapping[att_name]
                        else:
                            att = att_name
                        definitions[seq]["attribute"].append(att)

        # Remove empty entries
        cleaned_definitions = {
            seq: data for seq, data in definitions.items()
            if data["label"] or data["attribute"]
        }
        # Sort the dictionary by keys in ascending order
        cleaned_definitions = dict(sorted(cleaned_definitions.items()))

        return cleaned_definitions

    async def get_additional_attributes(self, sku: Optional[dict], ref_sku: Optional[dict], lang: str, brand: str,
                                        market) -> Dict[str, Attribute]:
        # Initialize result dict to return
        brand = map_brand(brand)
        result = {}

        # Skip if both sku and ref_sku are None
        if sku is None and ref_sku is None:
            return result

        # Get product table IDs, handling None cases
        ref_sku_safe = ref_sku or {}
        sku_safe = sku or {}
        prodtables_ids = await self.get_prodtable_ids(ref_sku_safe, sku_safe)
        if not prodtables_ids:
            return result

        # Get SKU IDs, handling None cases
        skus = []
        if sku:
            skus.append(sku["epimId"])
        if ref_sku:
            skus.append(ref_sku["epimId"])

        index = f"systemair_ds_producttables_{lang}"
        grouped = {}
        try:
            response = await self.es.asearch(index, query_attr_definitions(prodtables_ids, brand))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return []
            for hit in hits:
                table = hit["_source"].get("table", [])

                # Run the parser
                sku_att_definitions = await  self.parse_att_definitions(table, market)

                unique_attributes = list(set(
                    attr for data in sku_att_definitions.values() for attr in data["attribute"]
                ))

                # att_index = f"systemair_ds_attributes_{lang}"
                # att_response = list(self.es.getScrollObject(att_index, query_sku_attributes(unique_attributes, skus), 10000,"1m"))
                indices, attQuery = inject_fallback_sort(query_sku_attributes(unique_attributes, skus), lang,
                                                         "systemair_ds_attributes_",
                                                         "attributeParentId")
                attributes_res = await self.es.asearch(indices, attQuery)
                att_response = attributes_res.get("hits", {}).get("hits", [])

                # for hit in att_response.get("hits", {}).get("hits", []):
                for hit in att_response:
                    src = hit["_source"]
                    attr = src.get("name")

                    # start
                    # extract unit + value(s)
                    vals = src.get("values", [])
                    if not vals:
                        continue

                    unit = (
                        next(
                            (u["unitShortName"]
                             for v in vals
                             for u in v.get("unitList", [])
                             if u["langIso"] == lang),
                            vals[0].get("unit", "") if vals else ""
                        )
                    )
                    if len(vals) == 1:
                        value = vals[0].get("value")
                    else:
                        value = [
                            d["value"]
                            for d in sorted(
                                (d for d in vals if d.get("value") is not None),
                                key=lambda d: (d.get("seqorderNr") is None, d.get("seqorderNr") or 0)
                            )
                        ]
                    if value is None:
                        continue  # skip empties
                    value, unit2 = parse_piped_value(value)
                    if unit2:
                        unit = unit2
                    # end
                    '''
                    unit = src.get("values", [{}])[0].get("unit", "")
                    vals = src.get("values")

                    if len(vals) == 1:
                        value = vals[0]["value"]
                    else:
                        # comprehension with local lookup
                        value = [v["value"] for v in vals if v["value"] is not None]
                    '''
                    if not all([attr, value]):
                        continue  # Skip incomplete entries
                    label = attr  # Default fallback
                    for entry in sku_att_definitions.values():
                        if attr in entry["attribute"]:
                            label = entry["label"][0] if entry["label"] else attr
                            break
                    result[attr] = Attribute(
                        name=label,
                        attribute=attr,
                        value=value,
                        unit=unit,
                    )

            return result
        except Exception as e:
            logger.exception(f"Failed to fetch additional attributes: {e}")
            return result

    async def get_shop_additional_attributes(self, sku: Optional[dict], ref_sku: Optional[dict], lang: str, brand: str,
                                             market) -> Dict[str, Attribute]:
        # Initialize result dict to return
        result = {}

        # Skip if both sku and ref_sku are None
        if sku is None and ref_sku is None:
            return result

        # Get SKU IDs, handling None cases
        skus = []
        if sku:
            skus.append(sku["epimId"])
        if ref_sku:
            skus.append(ref_sku["epimId"])

        index = f"systemair_ds_producttables_{lang}"

        try:
            response = await self.es.asearch(index, query_shop_attr_definitions())
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return []
            for hit in hits:
                table = hit["_source"].get("table", [])

                # Run the parser
                sku_att_definitions = await  self.parse_att_definitions(table, market)

                unique_attributes = list(set(
                    attr for data in sku_att_definitions.values() for attr in data["attribute"]
                ))

                # att_index = f"systemair_ds_attributes_{lang}"
                # att_response = list(self.es.getScrollObject(att_index, query_sku_attributes(unique_attributes, skus), 10000,"1m"))

                indices, attQuery = inject_fallback_sort(query_sku_attributes(unique_attributes, skus), lang,
                                                         "systemair_ds_attributes_",
                                                         "attributeParentId")
                attributes_res = await self.es.asearch(indices, attQuery)
                att_response = attributes_res.get("hits", {}).get("hits", [])

                # for hit in att_response.get("hits", {}).get("hits", []):
                for hit in att_response:
                    src = hit["_source"]
                    attr = src.get("name")

                    unit = src.get("values", [{}])[0].get("unit", "")
                    vals = src.get("values")

                    if len(vals) == 1:
                        value = vals[0]["value"]
                    else:
                        # comprehension with local lookup
                        value = [v["value"] for v in vals if v["value"] is not None]

                    if not all([attr, value]):
                        continue  # Skip incomplete entries
                    label = attr  # Default fallback
                    for entry in sku_att_definitions.values():
                        if attr in entry["attribute"]:
                            label = entry["label"][0] if entry["label"] else attr
                            break
                    result[attr] = Attribute(
                        name=label,
                        attribute=attr,
                        value=value,
                        unit=unit,
                    )

            return result
        except Exception as e:
            logger.exception(f"Failed to fetch additional attributes: {e}")
            return result

    async def get_images(self, ref_sku: Optional[dict], sku: Optional[dict], lang: str) -> List[str]:
        resolved_epim_ids = []
        cat_ids = [125, 126, 120, 119]
        for obj in (ref_sku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])
        for obj in (sku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        index = f"systemair_ds_elements_{lang}"
        res = []
        try:
            response = await self.es.asearch(index, query_images(resolved_epim_ids, cat_ids))
            for hit in response.get("hits", {}).get("hits", []):
                res.append(hit["_source"].get("dsElementPreviewFile"))
            return res
        except Exception as e:
            logger.exception(f"Failed to fetch images {resolved_epim_ids} from ES: {e}")
            return []

    async def get_images_old(self, ref_sku: Optional[dict], sku: Optional[dict], lang: str) -> List[str]:
        # Category IDs
        CAT_SKU_MAIN = 125
        CAT_PROD_MAIN = 119
        CAT_SKU_ADDL = 126
        CAT_PROD_ADDL = 120
        # First-match placeholders (one pass)
        sku_main_url = None
        prod_main_url = None
        sku_addl_url = None
        prod_addl_url = None
        resolved_epim_ids = []

        for obj in (ref_sku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])
        for obj in (sku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        index = f"systemair_ds_elements_{lang}"
        elementCategories = {}
        res = []
        try:
            response = await self.es.asearch(index, query_images(resolved_epim_ids))
            for hit in response.get("hits", {}).get("hits", []):
                elementId = hit["_source"]["epimId"]
                if elementId not in elementCategories:
                    cat_ids = {c.get("id") for c in hit["_source"].get("categories") if
                               isinstance(c, dict) and "id" in c}
                    elementCategories[elementId] = {"categories": cat_ids,
                                                    "preview": hit["_source"].get("dsElementPreviewFile")}
            for ele in elementCategories.items():
                if 125 in ele.get["categories"]:
                    res.append(ele.get("preview"))
                    continue
                    if 119 in ele.get["categories"]:
                        res.append(ele.get("preview"))
                        continue
                if 126 in ele.get["categories"]:
                    res.append(ele.get("preview"))
                    continue
                    if 120 in ele.get["categories"]:
                        res.append(ele.get("preview"))
                        continue
            return res
        except Exception as e:
            logger.exception(f"Failed to fetch images {resolved_epim_ids} from ES: {e}")
            return []

    async def get_images_shops(self, ref_sku: Optional[dict], sku: Optional[dict], lang: str) -> List[str]:
        """
                        Return a single image URL in a list, preferring category 125 over 119.
                        Falls back to [] if nothing is found.
                        """
        PREFERRED_CAT = 125
        FALLBACK_CAT = 119
        TARGET_CATS = {PREFERRED_CAT, FALLBACK_CAT}

        # Collect unique epimIds (preserving order)
        resolved_epim_ids: List[str] = []
        seen = set()
        for sku_dict in (ref_sku, sku):
            for ia in (sku_dict or {}).get("imageAssignments", []):
                for obj in ia.get("objects", []):
                    epim_id = obj.get("epimId")
                    if epim_id is not None and epim_id not in seen:
                        seen.add(epim_id)
                        resolved_epim_ids.append(epim_id)

        if not resolved_epim_ids:
            return []
        res = {}
        vaiants_elements = []
        final_response = []
        index = f"systemair_ds_elements_{lang}"
        try:
            response = await self.es.asearch(index, query_images(resolved_epim_ids, list(TARGET_CATS)))
            hits = response.get("hits", {}).get("hits", [])

            for hit in hits:
                src = hit.get("_source", {}) or {}
                file = src.get("dsElementPreviewFile")
                epim_id = hit["_source"].get("epimId")
                if epim_id not in res:
                    res[epim_id] = {"best": False, "fallback": False, "preview": None}
                res[epim_id]["preview"] = file
                vaiants_elements.append(epim_id)
                if not file:
                    continue

                hit_cat_ids = {
                    c.get("id")
                    for c in (src.get("categories") or [])
                    if isinstance(c, dict) and "id" in c
                }

                if PREFERRED_CAT in hit_cat_ids:
                    res[epim_id]["best"] = True

                if FALLBACK_CAT in hit_cat_ids:
                    res[epim_id]["fallback"] = True
                    # Return the first image URL if available, otherwise None
            att_index = f"systemair_ds_attributes_{lang}"
            att_response = list(
                self.es.getScrollObject(att_index, query_elements_attributes(vaiants_elements), 10000, "1m"))

            check_inactive_internal = {}
            for hit in att_response:
                src = hit["_source"]
                elementId = int(src.get("parentId"))
                values = src.get("values", [])
                value = values[0].get("value")

                if elementId not in check_inactive_internal:
                    check_inactive_internal[elementId] = {"inactive": True, "internal": True}
                if src.get("name") == "inactive":
                    check_inactive_internal[elementId]["inactive"] = bool(value)
                if src.get("name") == "internal":
                    check_inactive_internal[elementId]["internal"] = bool(value)
            for id in res:
                if id in check_inactive_internal and not (check_inactive_internal[id]["inactive"]) and not (
                check_inactive_internal[id]["internal"]):
                    if res[id]["best"]:
                        final_response = []
                        final_response.append(res[id]["preview"])
                        break
                    if res[id]["fallback"]:
                        final_response.append(res[id]["preview"])
            return final_response
        except Exception as e:
            logger.exception("Failed to fetch images %s from ES: %s", resolved_epim_ids, e)
            return []

    async def get_images_shops_backup(self, ref_sku: Optional[dict], sku: Optional[dict], lang: str) -> List[str]:
        """
        Return a single image URL in a list, preferring category 125 over 119.
        Falls back to [] if nothing is found.
        """
        PREFERRED_CAT = 125
        FALLBACK_CAT = 119
        TARGET_CATS = {PREFERRED_CAT, FALLBACK_CAT}

        # Collect unique epimIds (preserving order)
        resolved_epim_ids: List[str] = []
        seen = set()
        for sku_dict in (ref_sku, sku):
            for ia in (sku_dict or {}).get("imageAssignments", []):
                for obj in ia.get("objects", []):
                    epim_id = obj.get("epimId")
                    if epim_id is not None and epim_id not in seen:
                        seen.add(epim_id)
                        resolved_epim_ids.append(epim_id)

        if not resolved_epim_ids:
            return []

        index = f"systemair_ds_elements_{lang}"
        try:
            response = await self.es.asearch(index, query_images(resolved_epim_ids, list(TARGET_CATS)))
            hits = response.get("hits", {}).get("hits", [])

            best = None  # cat 125
            fallback = None  # cat 119

            for hit in hits:
                src = hit.get("_source", {}) or {}
                file = src.get("dsElementPreviewFile")
                if not file:
                    continue

                hit_cat_ids = {
                    c.get("id")
                    for c in (src.get("categories") or [])
                    if isinstance(c, dict) and "id" in c
                }

                if PREFERRED_CAT in hit_cat_ids:
                    best = file
                    break  # earliest preferred hit wins
                if fallback is None and FALLBACK_CAT in hit_cat_ids:
                    fallback = file  # keep first fallback candidate

            if best:
                return [best]
            if fallback:
                return [fallback]
            return []

        except Exception as e:
            logger.exception("Failed to fetch images %s from ES: %s", resolved_epim_ids, e)
            return []

    async def parse_price_async_old(self, sku: dict) -> Price:
        # execute_query returns a list of dicts
        rows = await self.db.aexecute_query(
            query_price(),
            {"productnr": sku.get("productNr")}
        )

        if rows:
            row = rows[0]
            price_val = row.get("MARKET_670_PRICE")
            currency = row.get("MARKET_670_CURRENCY")
            # if price_val is None, it's onâ€�demand
            return Price(
                ondemand=price_val is None,
                string=str(price_val) if price_val is not None else "On demand",
                float=float(price_val) if price_val is not None else None,
                currency=currency
            )
        else:
            # no row â†’ default to onâ€�demand
            return Price(
                ondemand=True,
                string="On demand",
                float=None,
                currency=None
            )

    async def parse_price_async(self, sku: dict, refSku: dict, market: str) -> Price:
        """
        Pulls the latest price row from your DB (via execute_query),
        then formats it as: { ondemand, string, float, currency }.
        """
        market = market.replace("-", "_")

        prodNr = refSku.get("productNr") if refSku else sku.get("productNr")
        rows = await self.db.aexecute_query(query_price(), {"productnr": prodNr})
        if not rows:
            # no price → on-demand
            return Price(
                ondemand=True,
                string="On demand",
                float=None,
                currency=None,
            )

        row = rows[0]
        raw = row[str(market + "_PRICE")]
        curr = row[str(market + "_CURRENCY")]

        # numeric value
        try:
            p = float(raw)
        except (ValueError, TypeError):
            p = 1  # or 0.0 or whatever makes sense
        # If price is less than 1, return on-demand
        if p <= 1:
            return Price(
                ondemand=True,
                string="On demand",
                float=None,
                currency=None,
            )

        # -- format "11.700,00 €" --
        # 1) produce "11,700.00"
        s = f"{p:,.2f}"
        # 2) swap , → temporary, .→ ,, temp → .
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        s = f"{s} {curr}"  # non-breaking space before €

        # Use the parse_price method to ensure consistent Price object creation
        return self.parse_price({
            "ondemand": False,
            "string": s,
            "float": float(p),
            "currency": curr
        })

    async def parse_certifications_async(self, identifiers: List[int], lang: str) -> List[Certification]:
        """
        From a list of ES attribute hits, return only those contain  "-CERT-"
        whose first 'value' == 1, as Certification objects.
        """
        cert_table = await self.es.asearch(f"systemair_ds_producttables_{lang}", query_cert_definitions())
        hits = cert_table.get("hits", {}).get("hits", [])
        table_rows = hits[0]["_source"].get("table", [])[0].get("rows", [])
        # Build seq â†’ label mapping from row 0
        label_by_seq = {}
        for cell in table_rows[0]["cells"]:
            seq = cell.get("seqorderNr")
            if seq is not None:
                label_by_seq[seq] = cell.get("label", [])[0].get("content", "")

        merged_dict = {}
        for cell in table_rows[1]["cells"]:
            seq = cell.get("seqorderNr")
            if seq is None or seq not in label_by_seq:
                continue

            # Assume exactly one attribute dict in the list; adjust if you expect multiple
            attrs = cell.get("attribute", [])
            if not attrs or "name" not in attrs[0]:
                continue
            key = attrs[0]["name"]

            merged_dict[key] = {
                "label": label_by_seq[seq],
                "nullFallbackText": cell.get("nullFallbackText"),
                "nullFallbackTextDictId": cell.get("nullFallbackTextDictId"),
            }
        result: List[Certification] = []
        try:
            response = await self.es.asearch(f"systemair_ds_attributes_{lang}", query_certifications(identifiers))
            for att in response.get("hits", {}).get("hits", []):
                # Some callers pass the raw dict or an ES hitâ€”normalize both
                src = att.get("_source", att)

                name = src.get("name", "")
                cert = Certification(
                    id=str(src.get("attributeId", "")),
                    name=name,
                    label=merged_dict[name]["label"],
                    image=await self.get_image_byId(str(src.get("flag1ObjeId", "")), lang),
                    text=merged_dict[name]["nullFallbackText"]
                )
                result.append(cert)

        except Exception:
            logger.exception("Error parsing certifications")

        return result

    async def get_image_byId(self, id: str, lang: str) -> Optional[str]:
        response = await self.es.asearch(f"systemair_ds_elements_{lang}", query_image_byId(id))
        hits = response.get("hits", {}).get("hits", [])
        if hits:
            return hits[0]["_source"].get("dsElementPreviewFile")
        return ""

    async def parse_attributes_async(self, data: Dict[str, dict]) -> Dict[str, Attribute]:
        return self.parse_attributes(data)

    async def parse_sections_async(self, sections: List[dict]) -> List[Section]:
        return self.parse_sections(sections)

    def parse_price(self, data: dict) -> Price:
        return Price(
            ondemand=data.get("ondemand", True),
            string=data.get("string", "On demand"),
            float=data.get("float"),
            currency=data.get("currency")
        )

    def parse_certifications(self, certs: List[dict]) -> List[Certification]:
        return [
            Certification(
                id=str(cert.get("id")),
                name=cert.get("name"),
                label=cert.get("label"),
                image=cert.get("image"),
                text=cert.get("text")
            ) for cert in certs
        ]

    def parse_attributes(self, data: Dict[str, dict]) -> Dict[str, Attribute]:
        return {
            key: Attribute(
                name=attr.get("name"),
                attribute=attr.get("attribute"),
                value=attr.get("value"),
                unit=attr.get("unit")
            ) for key, attr in data.items()
        }

    def parse_sections(self, sections: List[dict]) -> List[Section]:
        return [
            Section(
                section=section.get("section"),
                name=section.get("name"),
                contents=section.get("contents", [])
            ) for section in sections
        ]

    async def get_buttons(self, identifiers: List[int], lang: str) -> List[Buttons]:
        """
        Fetch and parse button attributes for a SKU.

        Args:
            sku: The SKU data dictionary
            ref_sku: The reference SKU data dictionary (if any)
            lang: Language code

        Returns:
            List of Buttons objects
        """
        # Skip if both sku and ref_sku are None

        try:
            # Query button attributes from Elasticsearch
            index = f"systemair_ds_attributes_{lang}"
            response = await self.es.asearch(index, query_attr_buttons(identifiers))
            hits = response.get("hits", {}).get("hits", [])

            if not hits:
                return []

            # Group button attributes by their base name (without -LNK or -IMG suffix)
            from collections import defaultdict
            grouped = defaultdict(lambda: {"icon": None, "url": None})

            for hit in hits:
                src = hit.get("_source", {})
                name = src.get("name", "")
                # Split into base + suffix
                parts = name.rsplit("-", 1)

                base, suffix = parts[0], parts[1].upper()
                values = src.get("values", [])
                if not values:
                    continue

                # Get the first value (assuming single value for buttons)
                val = values[0].get("value") if values else None
                if val is None:
                    continue

                # Store based on suffix
                if suffix == "LNK":
                    grouped[base]["url"] = val
                elif suffix == "IMG":
                    grouped[base]["icon"] = await self.get_image_byId(src.get("flag0ObjeId", ""), lang)

            # Button type to name mapping
            BUTTON_MAP = {
                # base_key_without_suffix : (type, display-name)
                "BTN-ahu-compact-configurator": (
                    "AHU-COMPACT-CONFIGURATOR",
                    "KompaktlÃ¼ftungsgerÃ¤te Configurator"
                ),
                # Add more button mappings as needed
            }

            # Create Buttons objects
            buttons: List[Buttons] = []
            for base, data in grouped.items():
                # Only include buttons that have both URL and icon
                if not (data["url"] and data["icon"]):
                    continue

                # Get button type and name from mapping, or use defaults
                btn_type, btn_name = BUTTON_MAP.get(base, (base, base.replace("BTN-", "").replace("-", " ").title()))

                buttons.append(
                    Buttons(
                        name=btn_name,
                        type=btn_type,
                        icon=data["icon"],
                        url=data["url"]
                    )
                )

            return buttons

        except Exception as e:
            logger.exception(f"Failed to fetch button attributes: {e}")
            return []

    async def extract_identifiers(self, identifier, lang, brand):
        """
        Extracts a list of identifiers: [sku_id, refSku_id] if not None.
        """
        sku = await self.get_sku(identifier, lang, brand)
        sku_id = identifier
        refSku_id = await self.get_ref_id(sku)
        identifiers = [i for i in [sku_id, refSku_id] if i is not None]
        return identifiers

    async def get_relations(self, identifier, lang, brand) -> List["Relation"]:
        """
        Fetches related SKUs/accessories for a given SKU identifier.
        Returns a list of Relation dicts.
        """
        sku = await self.get_sku(identifier, lang, brand)
        sku_id = identifier
        refSku_id = await self.get_ref_id(sku)
        refSku = await self.get_sku(refSku_id, lang, None) if refSku_id else None

        identifiers = [i for i in [sku_id, refSku_id] if i is not None]
        resolved_epim_ids = defaultdict(lambda: defaultdict(list))

        # Collect from refSku
        for obj in (refSku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            for o in obj.get("objects", []):
                object_type = o.get("objectType")
                object_id = o.get("objectId")
                if assignment_type and object_type and object_id is not None:
                    resolved_epim_ids[assignment_type][object_type].append(object_id)

        # Collect from sku
        for obj in (sku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            for o in obj.get("objects", []):
                object_type = o.get("objectType")
                object_id = o.get("objectId")
                if assignment_type and object_type and object_id is not None:
                    resolved_epim_ids[assignment_type][object_type].append(object_id)

        relations: List[Relation] = []

        for assignment, object_map in resolved_epim_ids.items():
            for obj_type, ids in object_map.items():
                if not ids:
                    continue

                if obj_type == "product":
                    index = f"systemair_ds_products_{lang}"
                elif obj_type == "variant":
                    index = f"systemair_ds_variants_{lang}"
                else:
                    continue  # Skip unknown object types

                scroll_results = await self.es.agetScrollObject(index, query_sku_relations(ids), 10000, "1m")

                for hit in scroll_results:
                    src = hit.get("_source", {})
                    relation = Relation(
                        id=str(src.get("epimId")),
                        vendorId=src.get("productNr"),
                        operatingMode=None,
                        parentId=src.get("parentHierarchy"),
                        type=assignment,
                        group=src.get("group"),
                        name=src.get("name"),
                        image=await self.get_images(src, None, lang),
                        priority=src.get("priority")
                    )
                    relations.append(relation)

        return relations

    async def get_shop_relations(self, sku: Optional[dict], refsku: Optional[dict], lang, brand, market) -> List[
        "RelationShop"]:
        """
        Fetches related SKUs/accessories for a given SKU identifier.
        Returns a list of Relation dicts.
        """

        # identifiers = [i for i in [sku.get("epimId"), refsku.get("epimId")] if i is not None]
        resolved_epim_ids = defaultdict(lambda: defaultdict(list))

        # Collect from refSku
        for obj in (refsku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            for o in obj.get("objects", []):
                object_type = o.get("objectType")
                object_id = o.get("objectId")
                if assignment_type and object_type and object_id is not None:
                    resolved_epim_ids[assignment_type][object_type].append(object_id)

        # Collect from sku
        for obj in (sku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            for o in obj.get("objects", []):
                object_type = o.get("objectType")
                object_id = o.get("objectId")
                if assignment_type and object_type and object_id is not None:
                    resolved_epim_ids[assignment_type][object_type].append(object_id)

        relations: List[RelationShop] = []

        for assignment, object_map in resolved_epim_ids.items():
            for obj_type, ids in object_map.items():
                if not ids:
                    continue

                if obj_type == "product":
                    index = f"systemair_ds_products_{lang}"
                elif obj_type == "variant":
                    index = f"systemair_ds_variants_{lang}"
                else:
                    continue  # Skip unknown object types

                scroll_results = await self.es.agetScrollObject(index, query_sku_relations(ids), 10000, "1m")
                # must be flaged on the market and not expired
                epimIds = [hit["_source"].get("epimId") for hit in scroll_results if
                           hit.get("_source", {}).get("epimId") is not None]

                markets = await self.check_market_status(epimIds, market, lang)
                for hit in scroll_results:
                    src = hit.get("_source", {})
                    epim_id = src.get("epimId")
                    if epim_id in markets and markets.get(epim_id).get("market") and not markets.get(epim_id).get(
                            "expired"):
                        relation = RelationShop(
                            id=str(epim_id),
                            vendorId=src.get("productNr"),
                            type="accessory",
                            group=assignment,
                            # name=src.get("name"),
                            name=markets.get(epim_id).get("m3-item-name")

                        )
                        relations.append(relation)

        return relations

    async def get_documents(self, identifier, lang, brand) -> List["Document"]:
        """
        Fetches related documents for a given SKU identifier.
        Returns a list of Document dicts.
        """
        sku = await self.get_sku(identifier, lang, brand)
        refSku_id = await self.get_ref_id(sku)
        refSku = await self.get_sku(refSku_id, lang, None) if refSku_id else None

        resolved_epim_ids = []

        # Collect epimIds from imageAssignments (refSku and sku)
        for obj in (refSku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        for obj in (sku or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])
        # Collect epimIds from documents (refSku and sku)
        for obj in (refSku or {}).get("documentAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        for obj in (sku or {}).get("documentAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        # Collect epimIds from audioAssignments (refSku and sku)
        for obj in (refSku or {}).get("audioAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        for obj in (sku or {}).get("audioAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        # Collect epimIds from graphicAssignments (refSku and sku)
        for obj in (refSku or {}).get("graphicAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        for obj in (sku or {}).get("graphicAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        # Collect epimIds from videoAssignments (refSku and sku)
        for obj in (refSku or {}).get("videoAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        for obj in (sku or {}).get("videoAssignments", []):
            for o in obj.get("objects", []):
                resolved_epim_ids.append(o["epimId"])

        if not resolved_epim_ids:
            return []

        # Search Elasticsearch index for documents
        index = f"systemair_ds_elements_{lang}"
        documents: List[Document] = []

        response = await self.es.asearch(index, query_documents(resolved_epim_ids))

        for hit in response.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})

            # Extract fields
            file_path = src.get("dsElementPreviewFile")
            file_ext = str(src.get("phyFileExtension", "")).lower()
            category_list = src.get("categories", [])
            last_category = category_list[-1]["category"] if category_list else "document"

            document = Document(
                type=last_category,
                name=src.get("phyFileName"),
                url=file_path,
                mime=f"application/{file_ext}" if file_ext else "application/octet-stream",
                viewable=(file_ext == "pdf")
            )
            documents.append(document)

        return documents

    async def check_market_status(self, sku_ids: Union[str, List[str]], market: str, lang: str) -> Dict[
        str, Dict[str, bool]]:
        """
        Check the market status for one or more SKUs.

        Args:
            sku_ids: A single SKU ID (str) or list of SKU IDs to check
            market: The market code (e.g., 'EU', 'US')
            lang: The language code (e.g., 'en', 'sv')

        Returns:
            A dictionary where keys are SKU IDs and values are dictionaries with status information:
            {
                'sku_id_1': {
                    'market': bool,    # If the SKU is available in the market
                    'expired': bool    # If the SKU is expired in the market
                },
                ...
            }
        """
        # Convert single ID to list for consistent processing
        if isinstance(sku_ids, str):
            sku_ids = [sku_ids]

        if not sku_ids:
            return {}

        market_corrected = market.lower().replace("_", "-")
        market_attr = market_corrected
        expired_attr = f"{market_corrected}-expired"
        m3_name = f"M3-ITEM-NAME"
        attributes = [market_attr, expired_attr, m3_name]

        # Get the attributes for the SKUs
        attributes = await self.es.getScrollObject(
            f"systemair_ds_attributes_{lang}",
            query_sku_attributes(attributes, sku_ids),
            10000,
            "1m"
        )

        # Initialize result dictionary with default values
        result = {sku_id: {'market': False, 'expired': False, 'm3-item-name': ''} for sku_id in sku_ids}

        # Process the attributes
        for att in attributes:
            src = att.get("_source", {})
            name = src.get("name")
            sku_id = src.get("parentId")
            values = src.get("values", [])

            if not values:
                continue

            value = values[0].get("value")

            if name == market_attr:
                result[sku_id]['market'] = bool(value)
            elif name == expired_attr:
                result[sku_id]['expired'] = bool(value)
            elif name == m3_name:
                result[sku_id]['m3-item-name'] = value

        return result

    async def get_successors_ids(self, sku: Optional[dict], refsku: Optional[dict], lang, brand, market) -> List[
        "RelationShop"]:
        """
        Fetches related SKUs/accessories for a given SKU identifier.
        Returns a list of Relation dicts.
        """

        # identifiers = [i for i in [sku.get("epimId"), refsku.get("epimId")] if i is not None]
        resolved_epim_ids = []
        successorsIds = []

        # Collect from refSku
        for obj in (refsku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            if assignment_type == "Replacement":
                for o in obj.get("objects", []):
                    object_type = o.get("objectType")
                    object_id = o.get("objectId")
                    if assignment_type and object_type and object_id is not None:
                        resolved_epim_ids.append(object_id)

        # Collect from sku
        for obj in (sku or {}).get("relationAssignments", []):
            assignment_type = obj.get("assignmentType")
            if assignment_type == "Replacement":
                for o in obj.get("objects", []):
                    object_type = o.get("objectType")
                    object_id = o.get("objectId")
                    if assignment_type and object_type and object_id is not None:
                        resolved_epim_ids.append(object_id)

        index = f"systemair_ds_products_{lang}"
        scroll_results = await self.es.agetScrollObject(index, query_sku_relations(resolved_epim_ids), 10000, "1m")
        # must be flaged on the market and not expired
        successorsIds = [str(hit["_source"].get("epimId")) for hit in scroll_results if
                         hit.get("_source", {}).get("epimId") is not None]
        return successorsIds
