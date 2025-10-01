from typing import Optional, List, Dict, Union
# This module should define raw Elasticsearch queries

def query_operating_mode_by_id(identifier: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"epimId": identifier}}
                ]
            }
        }
    }
def query_images(identifiers: List[int]) -> dict:
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
    return {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "variant"}},
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
def query_operating_modes(operating_mode_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"epimId": operating_mode_id}}
                ]
            }
        }
    }
def query_default_operating_mode(identifiers: List[int]) -> dict:
    return {
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
                          "exists": {
                            "field": "values.value"
                          }
                        }
                      }
                    }
                ]
            }
        },"_source":["parentId","name","values"]
    } 
def query_certifications(operating_mode_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"operating_modeId": operating_mode_id}},
                    {"term": {"category": "certification"}}
                ]
            }
        }
    }

def query_sections(operating_mode_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"operating_modeId": operating_mode_id}},
                    {"term": {"objectType": "section"}}
                ]
            }
        }
    }

def query_images(operating_mode_ids: List[str]) -> dict:
    return {"size":10000,
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "parentElement": operating_mode_ids
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
def query_editorial_assets(operating_mode_id: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"operating_modeId": operating_mode_id}},
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
# Add more query builders as needed...
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
def query_attr_definitions(identifiers: List[int], brand: str) -> dict:
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
def query_operating_mode_attributes(attributes: List[str],skus: List[int]) -> dict:
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
                            "parentType": "variant"
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
        },"_source": ["values.value","values.dictId","values.unit", "name", "type", "parentId", "attributeId","datatype"]
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

def query_wiringSection(identifiers: List[int]) -> dict:
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
                          "categories.id": [49,18,55]
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