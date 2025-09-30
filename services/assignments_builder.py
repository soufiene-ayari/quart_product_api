import logging
from typing import Optional, List
from models.sku import Certification

from queries.assignments_queries import (query_certifications,query_cert_definitions,query_image_byId)
from services.elasticsearch_service import ESConnection
from services.database_service import DBConnection


logger = logging.getLogger(__name__)


class AssignmentsBuilder:
    def __init__(self, es_client: ESConnection, db_client: DBConnection):
        self.es = es_client
        self.db = db_client


    async def parse_certifications_async(self, lang: str) -> List[Certification]:
        """
        From a list of ES attribute hits, return only those contain  "-CERT-"
        whose first 'value' == 1, as Certification objects.
        """
        cert_table = self.es.search(f"systemair_ds_producttables_{lang}", query_cert_definitions())
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
            response = self.es.search(f"systemair_ds_attributes_{lang}", query_certifications())
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
        response = self.es.search(f"systemair_ds_elements_{lang}", query_image_byId(id))
        hits = response.get("hits", {}).get("hits", [])
        if hits:
            return hits[0]["_source"].get("dsElementPreviewFile")
        return ""

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

