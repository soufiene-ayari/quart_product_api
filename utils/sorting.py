def apply_sort(products, sort_by, direction):
    reverse = direction == "desc"
    return sorted(products, key=lambda x: x.get(sort_by, ""), reverse=reverse)
