def paginate(items, page, page_size):
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end]
def extract_pagination(args):
    try:
        page = max(1, int(args.get("page", 1)))
        page_size = max(1, min(100, int(args.get("pageSize", 10))))
    except ValueError:
        page, page_size = 1, 10

    offset = (page - 1) * page_size
    return offset, page, page_size