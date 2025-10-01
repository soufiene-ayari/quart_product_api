

def query_certifications() -> dict:
    return {
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {
                        "wildcard": {
                            "name": "*-CERT-*"
                        }
                    },
                    {
                        "exists": {
                            "field": "flag1ObjeId"
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
        },
        "collapse": {
            "field": "attributeId"
        },
        "sort": [
            {
                "attributeId": "asc"
            }
        ]
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