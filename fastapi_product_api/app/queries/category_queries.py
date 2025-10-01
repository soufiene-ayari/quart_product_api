from typing import Optional, List, Dict, Union

def query_category_by_id(identifier: str) -> dict:
    """
    Query to get a single category by its ID
    """
    return {
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "epimId": identifier
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "planningLevel": "Product Range"
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {
                                                "field": "planningLevel"
                                            }
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {
                        "nested": {
                            "path": "hierarchies",
                            "query": {
                                "term": {
                                    "hierarchies.hierarchy": "ECOM NG"
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
                    {"term": {"parentType": "hierarchy"}},

                ]
            }
        }
    }
def query_categories(offset: int = 0, limit: int = 10, brand: str = 10) -> dict:
    """
    Query to get a paginated list of categories
    """
    return {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "planningLevel": "Product Range"
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {
                                                "field": "planningLevel"
                                            }
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    },
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
                ]
            }
        },
        "sort": [
            {
                "seqorderNr": "asc"
            }
        ]
    }
def query_categories_by_parentId(offset: int = 0, limit: int = 10, brand: str = 10,parentId: str="") -> dict:
    """
    Query to get a paginated list of categories
    """
    return {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "planningLevel": "Product Range"
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {
                                                "field": "planningLevel"
                                            }
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }

                    },
                    {
                        "term": {
                            "parentHierarchy": parentId

                        }
                    },
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
                ]
            }
        },
        "sort": [
            {
                "seqorderNr": "asc"
            }
        ]
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
                        "terms": {
                          "categories.id": [39,121]
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