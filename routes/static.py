import os
from quart import send_file

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

async def load_static_json(filename: str):
    file_path = os.path.join(STATIC_DIR, filename)
    if not os.path.isfile(file_path):
        return None, f"File {filename} not found"

    try:
        async with await asyncio.to_thread(open, file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data, None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON format in {filename}: {str(e)}"
    except Exception as e:
        return None, str(e)

@app.route("/rest/shops")
async def get_shops():
    data, error = await load_static_json("systemair_shops.json")
    if error:
        return jsonify({"error": error}), 500
    return jsonify(data)

@app.route("/rest/statistics")
async def get_statistics():
    data, error = await load_static_json("systemair_statistics.json")
    if error:
        return jsonify({"error": error}), 500
    return jsonify(data)
