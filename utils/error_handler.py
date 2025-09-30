from quart import jsonify, request
import traceback
import logging

logger = logging.getLogger("utils.error")

async def register_error_handlers(app):
    @app.errorhandler(Exception)
    async def handle_exception(e):
        logger.exception(f"Unhandled exception during request: {request.method} {request.path}")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

    @app.errorhandler(404)
    async def not_found(e):
        return jsonify({"error": "Not Found"}), 404

    @app.errorhandler(400)
    async def bad_request(e):
        return jsonify({"error": "Bad Request"}), 400
