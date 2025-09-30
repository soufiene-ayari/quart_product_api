def apply_filters(products, params):
    filtered = products
    for key, val in params.items():
        if key in ("source", "sort", "sortDirection", "page", "pageSize"):
            continue
        filtered = [p for p in filtered if str(p.get(key)) == val]
    return filtered
