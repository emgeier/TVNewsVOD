
import time
import hmac
import hashlib
import re
import base64
import os

SECRET_KEY = b"SECRET_KEY" 
# THIS IS A PLACEHOLDER: TODO WHEN COOKIE GENERATION IS SET UP 
def lambda_handler(event, context):
    request = event["Records"][0]["cf"]["request"]
    headers = request.get("headers", {})
    uri = request.get("uri", "")

    # Extract segment_id from URI: /segments/<segment_id>/...
    match = re.match(r"^/stream/([^/]+)/", uri)
    if not match:
        return deny("Invalid segment path")

    segment_id = match.group(1)

    # Extract cookies from headers
    cookies = parse_cookies(headers.get("cookie", []))
    cookie_value = cookies.get("segment_access")
    if not cookie_value:
        return deny("Missing segment_access cookie")

    try:
        cookie_segment_id, expiry_str, signature = cookie_value.split(".")
        expiry = int(expiry_str)
    except Exception:
        return deny("Malformed cookie")

    # Check expiration
    if expiry < int(time.time()):
        return deny("Cookie expired")

    # Validate signature
    payload = f"{cookie_segment_id}.{expiry}"
    expected_sig = hmac.new(SECRET_KEY, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    if expected_sig != signature:
        return deny("Invalid signature")

    # Check path match
    if cookie_segment_id != segment_id:
        return deny("Segment mismatch")

    # All good — allow request
    return request


def parse_cookies(cookie_header_list):
    cookies = {}
    for header in cookie_header_list:
        parts = header["value"].split(";")
        for part in parts:
            if "=" in part:
                name, value = part.strip().split("=", 1)
                cookies[name] = value
    return cookies


def deny(reason):
    print(f"Denied access: {reason}")
    return {
        "status": "403",
        "statusDescription": "Forbidden",
        "body": f"Access denied: {reason}",
        "headers": {
            "cache-control": [{"key": "Cache-Control", "value": "no-cache"}],
            "content-type": [{"key": "Content-Type", "value": "text/plain"}]
        }
    }
