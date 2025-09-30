import logging
from typing import Optional, List, Dict, Union, Any
from models.operating_mode import OperatingMode, Certification, Attribute, Section, Price, Buttons,SectionContent
from queries.operating_mode_queries import query_operating_mode_by_id, query_attributes, query_texts, query_images,query_price,query_attr_buttons,query_attr_definitions,query_operating_mode_attributes,query_cert_definitions,query_certifications,query_image_byId,query_wiringSection
from services.elasticsearch_service import ESConnection
from services.database_service import DBConnection
import asyncio
import re
import json
import locale
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


class OperatingModeBuilder:
    def __init__(self, es_client: ESConnection, db_client: DBConnection):
        self.es = es_client
        self.db = db_client

    async def build_operating_mode(self, identifier: str, lang: str, brand: str, market:str) -> Optional[OperatingMode]:
        try:
            operating_mode = await self.get_operating_mode(identifier, lang)
            if not operating_mode:
                return None

            operating_mode_id = identifier
            ref_operating_mode_id = await self.get_ref_id(operating_mode)
            #if ref is not there do not proceed
            ref_operating_mode = await self.get_operating_mode(ref_operating_mode_id, lang) if ref_operating_mode_id else None
            identifiers = [i for i in [operating_mode_id, ref_operating_mode_id] if i is not None]
            index = f"systemair_ds_attributes_{lang}"
            raw_attributes  = list(self.es.getScrollObject(index, query_attributes(identifiers), 10000, "1m"))
            #print(attributes)
            get_texts_ids = await self.get_texts_ids(ref_operating_mode, operating_mode)
            index = f"systemair_ds_elements_{lang}"
            texts_res = self.es.search(index, query_texts(get_texts_ids))
            texts = texts_res.get("hits", {}).get("hits", [])

            (
                parent_id,
                price,
                certifications,
                parsed_attributes,
                sections,
                expired,
                vendor_id,
                name,
                short_name,
                description,
                specification,
                tagline,
                active,
                release_date,
                selectionTool,
                magicadBim,
                default,
                approved,
                sort,
                images,
                buttons,
                skuId
            ) = await asyncio.gather(
                self.get_parent_id(operating_mode),
                self.parse_price_async(operating_mode, market),
                self.parse_certifications_async(identifiers, lang),
                self.get_additional_attributes(operating_mode,ref_operating_mode,lang,brand),
                self.parse_sections_async(operating_mode,ref_operating_mode,lang,brand),
                self.get_expired_status(raw_attributes,market),
                self.get_vendor_id(raw_attributes),
                self.get_name(raw_attributes, operating_mode),
                self.get_short_name(operating_mode),
                self.get_description(texts),
                self.get_specification(texts),
                self.get_tagline(raw_attributes),
                self.get_active_status(ref_operating_mode_id),
                self.get_release_date(raw_attributes),
                self.get_selection_tool(raw_attributes),
                self.get_magicadBim(raw_attributes),
                self.get_default_operating_mode_id(raw_attributes),
                self.get_approved_status(operating_mode),
                self.get_sort_order(operating_mode),
                self.get_images(ref_operating_mode,operating_mode,lang),
                self.get_buttons(identifiers, lang),
                self.get_sku_id(operating_mode)
            )

            return OperatingMode(
                id=str(operating_mode_id),
                parentId=parent_id,
                vendorId=vendor_id,
                name=name,
                shortName=short_name,
                description=description,
                specificationText=specification,
                tagline=tagline,
                active=active,
                expired=expired,
                approved=approved,
                releaseDate=release_date,
                selectionTool=selectionTool,
                designTool=operating_mode.get("designTool", False),
                magicadBim=magicadBim,
                sort=sort,
                price=price,
                default=default,
                certifications=certifications,
                images=images,
                attributes=parsed_attributes,
                sections=sections,
                skuId=skuId,
                buttons=buttons
            )

        except Exception as e:
            logger.exception(f"Failed to build operating_mode {identifier}: {str(e)}")
            return None

    async def get_operating_mode(self, identifier: str, lang: str) -> Optional[dict]:
        index = f"systemair_ds_variants_{lang}"
        try:
            response = self.es.search(index, query_operating_mode_by_id(identifier))
            hits = response.get("hits", {}).get("hits", [])
            return hits[0]["_source"] if hits else None
        except Exception as e:
            logger.exception(f"Error fetching operating_mode {identifier}: {e}")
            return None

    async def get_ref_id(self, operating_mode: dict) -> Union[str, int, None]:
        return operating_mode.get("referenceId")
    async def get_sku_id(self, operating_mode: dict) -> Union[str, int, None]:
        return str(operating_mode.get("parentId"))

    async def get_parent_id(self, operating_mode: dict) -> Union[str, int, None]:
        return operating_mode.get("parentHierarchy")

    async def get_vendor_id(self, attributes: List[dict]) -> Union[str, int, None]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "M3-item-name":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None

    async def get_default_operating_mode_id(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "default-variant":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None

    async def get_name(self, attributes: List[dict], operating_mode: dict) -> Optional[str]:
        """
        Look for an attribute whose name exactly equals "M3-ITEM-NAME"
        and return its first value; otherwise fall back to operating_mode["name"].
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
        return operating_mode.get("name")

    async def get_short_name(self, operating_mode: dict) -> Optional[str]:

        return operating_mode.get("name")

    async def get_texts_ids(self, ref_operating_mode: Optional[dict], operating_mode: Optional[dict]) -> List[int]:
        resolved_epim_ids = []
        for obj in (ref_operating_mode or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])
        for obj in (operating_mode or {}).get("textAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])
        return resolved_epim_ids

    async def get_description(self, texts: List[dict]) -> Optional[str]:
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") ==6 for cat in categories):
                temp=await self.transform_xml_to_json(text["_source"].get("xmlText"))
                return temp
                #return text["_source"].get("text")
        return None

    async def get_tagline(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "M3-ITEM-DESCRIPTION":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None

    async def get_specification(self, texts: List[dict]) -> Optional[str]:
        for text in texts:
            categories = text.get("_source", {}).get("categories", [])
            if any(cat.get("id") == 7 for cat in categories):
                return text["_source"].get("text")
        return None

    async def get_sort_order(self, operating_mode: dict) -> int:
        return operating_mode.get("seqorderNr", 0)

    async def get_active_status(self, ref_operating_mode_id: Union[str, int]) -> bool:
        if ref_operating_mode_id:
            return True
        return False

    async def get_expired_status(self,attributes: List[dict],market: str) -> bool:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == str("market-expired-"+market):
                values = src.get("values", [])
                if values:
                    return values[0].get("value")
        return False

    async def get_approved_status(self, operating_mode: dict) -> bool:
        try:
            workflows = operating_mode.get("workflows", [])
            return any(
                wf.get("workflow") == "SKU_STATE" and wf.get("stateName") in ["WfState_42"]
                for wf in workflows
            )
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
    async def get_selection_tool(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "export-promaster-systemair-fan-selector":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None
    async def get_magicadBim(self, attributes: List[dict]) -> Optional[str]:
        for att in attributes:
            src = att.get("_source", {})
            if src.get("name") == "BIM-enabled":
                values = src.get("values", [])
                if values:
                    return str(values[0].get("value"))
        return None


    async def get_additional_attributes_old(self, attributes: List[dict]) -> Dict[str, Attribute]:
        result = {}
        try:
            for att in attributes:
                # assume well‐formed hits: faster indexing rather than .get
                src = att["_source"]
                name = src["name"]
                vals = src.get("values")
                if not name or not vals:
                    continue

                # Avoid building an intermediate list unless needed
                if len(vals) == 1:
                    value = vals[0]["value"]
                else:
                    # comprehension with local lookup
                    value = [v["value"] for v in vals if v["value"] is not None]

                unit = vals[0].get("unit")
                # pack into your Pydantic model
                result[name] = Attribute(
                    name=name,
                    attribute=name,
                    value=value,
                    unit=unit,
                )

            return result
        except Exception as e:
            logger.exception(f"Failed to fetch additional attributes: {e}")
            return result
    async def get_images_ids(self, ref_operating_mode: Optional[dict], operating_mode: Optional[dict], lang: str) -> List[str]:
        resolved_epim_ids = []
        for obj in (ref_operating_mode or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])
        for obj in (operating_mode or {}).get("imageAssignments", []):
            for o in obj.get("objects", []):
                #if o.get("isResolved"):
                resolved_epim_ids.append(o["epimId"])

        return resolved_epim_ids
    async def get_images(self, ref_operating_mode: Optional[dict], operating_mode: Optional[dict], lang: str) -> List[str]:
        resolved_epim_ids = await self.get_images_ids(ref_operating_mode, operating_mode, lang)

        index = f"systemair_ds_elements_{lang}"
        res = []
        try:
            response = self.es.search(index, query_images(resolved_epim_ids))
            for hit in response.get("hits", {}).get("hits", []):
                res.append(hit["_source"].get("phyPreviewFile"))
            return res
        except Exception as e:
            logger.exception(f"Failed to fetch images {resolved_epim_ids} from ES: {e}")
            return []


    async def parse_price_async(self, operating_mode: dict, market:str) -> Price:
        """
        Pulls the latest price row from your DB (via execute_query),
        then formats it as: { ondemand, string, float, currency }.
        Returns on-demand price if price is less than 1 or if price cannot be converted to a number.
        """
        rows = self.db.execute_query(query_price(), {"productnr": operating_mode.get("productNr")})
        if not rows:
            # no price → on-demand
            return Price(
                ondemand=True,
                string="On demand",
                float=None,
                currency=None,
            )

        row = rows[0]
        raw = row[str(market+"_PRICE")]
        curr = row[str(market+"_CURRENCY")]

        # numeric value
        p = float(raw)
        # If price is less than 1, return on-demand
        if p < 1:
            return Price(
                ondemand=True,
                string="On demand",
                float=None,
                currency=curr,
            )

        # -- format "11.700,00 €" --
        # 1) produce "11,700.00"
        s = f"{p:,.2f}"
        # 2) swap , → temporary, .→ ,, temp → .
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        s = f"{s} {curr}"   # non-breaking space before € 

        # if you really want an integer when no cents:
        fv = int(p) if p.is_integer() else p

        return Price(
            ondemand=False,
            string=s,
            float=fv,
            currency=curr,
        )
        
    async def parse_certifications_async(self, identifiers: List[int],lang: str) -> List[Certification]:
        """
        From a list of ES attribute hits, return only those contain  "-CERT-"
        whose first 'value' == 1, as Certification objects.
        """
        cert_table = self.es.search(f"systemair_ds_producttables_{lang}", query_cert_definitions())
        hits = cert_table.get("hits", {}).get("hits", [])
        table_rows = hits[0]["_source"].get("table", [])[0].get("rows", [])
        # Build seq → label mapping from row 0
        label_by_seq = {}
        for cell in table_rows[0]["cells"]:
            seq = cell.get("seqorderNr")
            if seq is not None:
                label_by_seq[seq] = cell.get("label", [])[0].get("content","")

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
            response = self.es.search(f"systemair_ds_attributes_{lang}", query_certifications(identifiers))
            for att in response.get("hits", {}).get("hits", []):
                # Some callers pass the raw dict or an ES hit—normalize both
                src = att.get("_source", att)

                name = src.get("name", "")
                cert = Certification(
                    id=str(src.get("attributeId", "")),
                    name=name,
                    label= merged_dict[name]["label"],
                    image= await self.get_image_byId(str(src.get("flag1ObjeId", "")),lang),
                    text=merged_dict[name]["nullFallbackText"]
                )
                result.append(cert)

        except Exception:
            logger.exception("Error parsing certifications")

        return result

    async def parse_attributes_async(self, data: Dict[str, dict]) -> Dict[str, Attribute]:
        return self.parse_attributes(data)

    async def parse_sections_async(self, ref_operating_mode: Optional[dict], operating_mode: Optional[dict], lang: str, brand: str) -> List[Section]:

        sections = []
        contents = []
        variants = []
        num_columns: int = 3
        if operating_mode:
            variants.append(operating_mode["epimId"])
        if ref_operating_mode:
            variants.append(ref_operating_mode["epimId"])
        #description
        #wiring
        texts_ids=await self.get_texts_ids(ref_operating_mode, operating_mode)
        wiringText=self.es.search([f"systemair_ds_elements_{lang}",f"systemair_ds_elements_eng_glo"],query_wiringSection(texts_ids))
        hits = wiringText.get("hits", {}).get("hits", [])
        for hit in hits:
            xmlText=hit["_source"].get("xmlText")
            jsonText=await self.transform_xml_to_json(xmlText)
            contents.append({
                "type": "text",
                "content": jsonText
            })
        images_ids= await self.get_images_ids(ref_operating_mode, operating_mode, lang)
        wiringImages=self.es.search(f"systemair_ds_elements_{lang}",query_wiringSection(images_ids))
        image=""
        for hit in wiringImages.get("hits", {}).get("hits", []):
            image=hit["_source"].get("phyPreviewFile")

        contents.append({
            "type": "image",
            "content": image
        })

        sections.append({
            "section": "ecom- -data",
            "name": "Anschlussplan",
            "contents": contents

        })

        #technical parameters
        prodtables_ids = await self.get_prodtable_ids(ref_operating_mode, operating_mode)
        index = f"systemair_ds_producttables_{lang}"

        response = self.es.search(index, query_attr_definitions(prodtables_ids, brand))
        hits = response.get("hits", {}).get("hits", [])
        techs={}
        for hit in hits:
            table = hit["_source"].get("table", [])
            att_definitions = await  self.parse_att_definitions(table)
            for seq, att in att_definitions.items():
                if att["attribute"][0]=="dummy-tab":
                    sec=att["label"][0]
                    techs[sec] = []
                techs[sec].append(att)

        for secName,tech in techs.items():

            unique_attributes = list(set(
                attr for data in tech for attr in data["attribute"]
            ))

            att_index = f"systemair_ds_attributes_{lang}"
            att_response = list(
                self.es.getScrollObject(att_index, query_operating_mode_attributes(unique_attributes, variants), 10000,
                                        "1m"))
            # for hit in att_response.get("hits", {}).get("hits", []):
            rows = []
            for entry in tech:
                attrs = entry["attribute"]  # the list of attribute keys for this tech-group
                label_txt = entry["label"][0]  # the display label for this group
                # 1) Dummy-header rows
                if "dummy-table-header" in attrs:
                    rows.append({
                        "tr": [
                            {"th": label_txt},
                            {"th": ""},
                            {"th": ""}
                        ]
                    })
                    continue

                # 2) Now for each real attribute key in this group, find its hit
                for hit in att_response:
                    src = hit["_source"]
                    name = src.get("name")
                    if name not in attrs:
                        continue

                    # extract unit + value(s)
                    vals = src.get("values", [])
                    if not vals:
                        continue

                    unit = vals[0].get("unit", "")
                    if len(vals) == 1:
                        value = vals[0].get("value")
                    else:
                        value = [v["value"] for v in vals if v.get("value") is not None]

                    if value is None:
                        continue  # skip empties

                    # build the row
                    rows.append({
                        "tr": [
                            {"td": label_txt},
                            {"td": value},
                            {"td": unit}
                        ]
                    })
                    # once matched, break out to the next entry
                    break

            # then serialize
            finalRows = self.clean_and_serialize_rows(rows)
            contents = []
            contents.append({
                "type": "table",
                "content": finalRows
            })
            sections.append({
                "section": "Data-Technical-parameter",
                "name": secName,
                "contents": contents

            })




        return self.parse_sections(sections)

    def clean_and_serialize_rows(self,rows: List[Dict[str, List[Dict[str, str]]]]) -> str:
        """
        Process table rows to clean up headers without data and serialize to JSON.

        Args:
            rows: List of rows where each row is a dict with a 'tr' key containing
                  a list of cells. Each cell is a dict with either 'th', 'td', or 'tdi' as the key.

        Returns:
            JSON string of the cleaned rows with proper escaping.

        Example:
            >>> rows = [{"tr": [{"th": "Header"}, {"th": ""}, {"th": ""}]}]
            >>> clean_and_serialize_rows(rows)
            '[]'
        """

        def is_header_row(cells: List[Dict[str, str]]) -> bool:
            """Check if all cells in the row are header cells."""
            return all('th' in cell for cell in cells)

        def has_data(cells: List[Dict[str, str]]) -> bool:
            """Check if any cell in the row contains data (non-empty td)."""
            return any(
                cell.get('td') not in (None, '')
                for cell in cells
                if 'td' in cell
            )

        cleaned_rows: List[Dict[str, List[Dict[str, str]]]] = []
        i = 0
        total_rows = len(rows)

        while i < total_rows:
            current_cells = rows[i]['tr']

            if is_header_row(current_cells):
                # Look ahead to see if the next row has data
                if i + 1 < total_rows:
                    next_cells = rows[i + 1]['tr']
                    if has_data(next_cells):
                        cleaned_rows.append(rows[i])  # Keep header if followed by data
                i += 1  # Always move past the header
            else:
                cleaned_rows.append(rows[i])
                i += 1

        # Serialize with minimal whitespace and proper escaping
        return json.dumps(cleaned_rows, separators=(',', ':'), ensure_ascii=True)
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
    async def get_image_byId(self, id:str,lang: str) -> Optional[str]:
        response = self.es.search(f"systemair_ds_elements_{lang}", query_image_byId(id))
        hits = response.get("hits", {}).get("hits", [])
        if hits:
            return hits[0]["_source"].get("phyPreviewFile")
        return ""
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
            response = self.es.search(index, query_attr_buttons(identifiers))
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
                    "Kompaktlüftungsgeräte Configurator"
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
    async def parse_att_definitions(self, table: list) -> Dict[int, Dict[str, list]]:
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
        #sort with sequence decending before return
        return cleaned_definitions
    async def get_additional_attributes(self, sku: Optional[dict], ref_sku: Optional[dict], lang: str, brand: str) -> Dict[str, Attribute]:
        # Initialize result dict to return
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
            response = self.es.search(index, query_attr_definitions(prodtables_ids,brand))
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return []
            for hit in hits:
                table = hit["_source"].get("table", [])

                # Run the parser
                sku_att_definitions = await  self.parse_att_definitions(table)

                unique_attributes = list(set(
                    attr for data in sku_att_definitions.values() for attr in data["attribute"]
                ))

                att_index = f"systemair_ds_attributes_{lang}"
                att_response = list(
                    self.es.getScrollObject(att_index, query_operating_mode_attributes(unique_attributes, skus), 10000,
                                            "1m"))
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