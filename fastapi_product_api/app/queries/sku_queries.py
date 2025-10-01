from typing import Optional, List, Dict, Union
# This module should define raw Elasticsearch queries

def query_sku_by_id(identifier: str, brand: str) -> dict:
    filters = [
        {"term": {"epimId": identifier}}
    ]
    if brand is not None:
        filters.append(
            {
                "nested": {
                    "path": "hierarchies",
                    "query": {
                        "script": {
                            "script": {
                                "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{brand}')",
                                "lang": "painless"
                            }
                        }
                    }
                }
            }
        )
    return {
        "query": {
            "bool": {
                "filter": filters
            }
        }
    }


def query_sku_by_refrence_id(identifier: str, brand: str) -> dict:
    filters = [
        {"term": {"referenceId": identifier}}
    ]
    if brand is not None:
        filters.append(
            {
                "nested": {
                    "path": "hierarchies",
                    "query": {
                        "script": {
                            "script": {
                                "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{brand}')",
                                "lang": "painless"
                            }
                        }
                    }
                }
            }
        )
    return {
        "query": {
            "bool": {
                "filter": filters
            }
        }
    }


def query_sku_by_vendor_id(vendor_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"productNr": vendor_id}}
                ]
            }
        }
    }
def query_sku_relations(identifiers: List[int]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "objectId": identifiers
              }
            }
          ]
        }
      }
    }
def query_images_old(identifiers: List[int]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "epimId": identifiers
              }
            },
            {
              "nested": {
                "path": "categories",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "term": {
                          "categories.id": 119
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
def query_documents(identifiers: List[int]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "parentElement": identifiers
              }
            },
            {
              "nested": {
                "path": "categories",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "terms": {
                          "categories.id": [19,20,22,23,24,25,26,27,28,29,31,32,70,71]
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
def query_texts(identifiers: List[int]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "parentElement": identifiers
              }
            },
            {
              "nested": {
                "path": "categories",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "terms": {
                          "categories.id": [6,7]
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }    
def query_attributes(identifiers: List[int]) -> dict:
    return {"size":"10000",
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "product"}},
                    {
                      "nested": {
                        "path": "values",
                        "query": {
                          "exists": {
                            "field": "values.value"
                          }
                        }
                      }
                    }
                ]
            }
        }
    }     
def query_skus(sku_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"epimId": sku_id}}
                ]
            }
        }
    }
def query_default_operating_mode(identifiers: List[int]) -> dict:
    return {"size":10000,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "variant"}},
                    {"term": {"name": "default-variant"}},
                    {
                        "nested": {
                            "path": "values",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "values.value": 1
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },"_source":["parentId","name","values"]
    } 
def query_certifications(identifiers: List[int]) -> dict:
    return {
      "query": {
        "bool": {
          "filter": [
            {
              "wildcard": {
                "name": "*-CERT-*"
              }
            },
            {
              "terms": {
                "parentId": identifiers
              }
            },
            {
              "nested": {
                "path": "values",
                "query": {
                  "bool": {
                    "filter": [
                      {
                        "term": {
                          "values.value": 1
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
def query_cert_definitions() -> dict:
    return {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "name": "Certification_and_Compliance_Dictionary"
              }
            }
          ]
        }
      }
    }

def query_sections(sku_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"skuId": sku_id}},
                    {"term": {"objectType": "section"}}
                ]
            }
        }
    }

def query_images(sku_ids: List[str]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "parentElement": sku_ids
              }
            },
            {
              "nested": {
                "path": "categories",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "terms": {
                          "categories.id": [125,126,120,119]
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
def query_images(sku_ids: List[str],cat_ids: List[str]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "parentElement": sku_ids
              }
            },
            {
              "nested": {
                "path": "categories",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "terms": {
                          "categories.id": cat_ids
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
def query_image_byId(id: str) -> dict:
    return {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "parentElement": id
              }
            }
          ]
        }
      }
    }
def query_editorial_assets(sku_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"skuId": sku_id}},
                    {"term": {"category": "editorial"}}
                ]
            }
        }
    }  


def query_price() -> str:
    """
    Returns the SQL template for fetching the latest price.
    Uses a named parameter :productnr.
    """
    return (
        "SELECT TOP 1 * "
        "FROM vmps_ERP_prices "
        "WHERE PRODUCT_NUMBER = :productnr "
        "ORDER BY id DESC"
    )
def query_uom() -> str:
    """
    Returns the SQL template for fetching the mapping_units_of_measurements .
    Uses a named parameter :division.
    """
    return (
        "SELECT * "
        "FROM vmps_mapping_units_of_measurements "
        "WHERE DIVISION = :division "
    )
# Add more query builders as needed...

def query_sku_attributes(attributes: List[str],skus: List[int]) -> dict:
    return {"size":10000,
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "name": attributes
                        }
                    },
                    {
                        "terms": {
                            "parentId": skus
                        }
                    },

                    {
                      "nested": {
                        "path": "values",
                        "query": {
                          "exists": {
                            "field": "values.value"
                          }
                        }
                      }
                    }
                ]
            }
        },"_source": ["values.value","values.dictId","values.unit","values.unitList","values.seqorderNr", "name", "type", "parentId", "attributeId","datatype"]
    }
def query_shopSku_market(attributes: List[str],skus: List[int]) -> dict:
    return {
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "name": attributes
                        }
                    },
                    {
                        "terms": {
                            "parentId": skus
                        }
                    },
                    {
                        "term": {
                            "parentType": "product"
                        }
                    }
                ]
            }
        },"_source": ["values.value","values.dictId","values.unit", "name", "type", "parentId", "attributeId","datatype"]
    }
def query_attr_definitions(identifiers: List[int], brand: str) -> dict:
    return {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "wildcard": {
                                        "name": {
                                            "value": f"*{brand}"
                                        }
                                    }
                                },
                                {
                                    "terms": {
                                        "epimId": identifiers
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "term": {
                            "name": "DataStore_M3_Attributes"
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
def query_attr_TP_definitions(identifiers: List[int], brand: str) -> dict:
    return {
      "query": {
        "bool": {
          "must": [
            {
              "wildcard": {
                "name": {
                  "value": str("*"+brand)
                }
              }
            },
            {
              "terms": {
                "epimId": identifiers
              }
            }
          ]
        }
      }
    }
def query_shop_attr_definitions() -> dict:
    return {
      "query": {
        "bool": {
          "must": [
              {
              "term": {
                "name": "DataStore_Shop_Attributes"
              }
            }
          ]
        }
      }
    }

def query_attr_buttons(identifiers: List[int]) -> dict:
    return {
    "query": {
        "bool": {
            "filter": [
                {
                    "wildcard": {
                        "name": "BTN*"
                    }
                },
                {
                    "terms": {
                        "parentId": identifiers
                    }
                }
            ],
            "must_not": [
                {
                    "term": {
                        "datatype": "COLLECTION"
                    }
                }
            ]
        }
    }
}
def query_elements_attributes(identifiers: List[int]) -> dict:
    return {
  "query": {
    "bool": {
      "filter": [
        {
          "terms": {
            "name": ["inactive", "internal"]
          }
        },
        {
          "terms": {
            "parentId": identifiers
          }
        }
      ],
      "must_not": [
        {
          "nested": {
            "path": "values",
            "query": {
              "term": {
                "values.value": 1
              }
            }
          }
        }
      ]
    }
  }
}