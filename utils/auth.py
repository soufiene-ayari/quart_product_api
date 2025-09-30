from functools import wraps
from quart import request, jsonify, g
import logging
from core.environment import env

logger = logging.getLogger(__name__)

# Load token-user map from config
VALID_TOKENS = env.getConfig().get("auth_tokens", {})

def require_auth(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization")
        if not auth or not auth.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid token"}), 401

        token = auth.replace("Bearer ", "").strip()
        user = VALID_TOKENS.get(token)
        if not user:
            return jsonify({"error": "Unauthorized"}), 403

        g.auth_user = user  # store user info in request context
        logger.info(f"Authenticated as {user}")
        return await func(*args, **kwargs)
    return wrapper
