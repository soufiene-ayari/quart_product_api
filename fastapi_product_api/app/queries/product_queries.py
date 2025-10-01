from typing import Optional, List, Dict, Union
# This module should define raw Elasticsearch queries

def query_product_by_id(identifier: str, brand: str) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {"epimId": identifier}}
                ]
            }
        }
    }
def query_secondaryParents(category: str) -> dict:
    return {
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "referenceId": category

                        }
                    },
                    {
                        "term": {
                            "planningLevel": "Alias"
                        }
                    }
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
                        "terms": {
                          "categories.id": [6]
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
def query_attributes_old(identifiers: List[int]) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "hierarchy"}},
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
def query_attributes(identifiers: List[int]) -> dict:
    return {"size":10000,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "hierarchy"}},
                    {
                        "nested": {
                            "path": "values",
                            "query": {
                                "bool": {
                                    "must": [{"exists": {"field": "values.value"}}]
                                }
                            }
                        }
                    }
                ]
            }
        }
    }

def query_attributes_old2(identifiers: List[int]) -> dict:
    return {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"parentId": identifiers}},
                    {"term": {"parentType": "hierarchy"}},
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
                ],
                "must_not": [
                    {"terms": {"name": [
                        "ecom-SKU-options",
                        "ecom-old-matching-ids-products",
                        "is-hidden-level-6",
                        "keywords-maintenance-header-COL",
                        "table-dimensions-COL",
                        "table-dimensions-Imp-COL",
                        "table-etim-class-EC011013-COL"
                    ]}}
                ]
            }
        }
    }    
def query_sku_options_definitions(identifiers: List[int]) -> dict:
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
                        "terms": {
                          "categories.id": [251]
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
    
def query_child_objects(identifier: str) -> dict:
    return {
      "query": {
        "nested": {
          "path": "hierarchies",
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "hierarchies.id": identifier
                  }
                }
              ]
            }
          }
        }
      },"_source":["epimId","referenceId","productNr"]
    }
def query_child_objects_attributes(attributes: List[str],skus: List[int]) -> dict:
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
                        "term": {
                            "parentType": "product"
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
def query_productNrs(identifiers: List[int]) -> dict:
    return {
      "query": {
        "bool": {
          "filter": [
            {
              "terms": {
                "epimId": identifiers
              }
            }
          ]
        }
      },"_source": ["epimId","productNr"]
    }
# Add more query builders as needed...
