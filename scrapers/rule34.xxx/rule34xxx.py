#!/usr/bin/env python3
"""
Rule34.xxx Scraper for Stashapp
================================

Scrapes metadata from rule34.xxx by matching file md5 hash to posts.

HOW IT WORKS:
1. Stashapp passes file path via fragment input
2. Extract md5 from filename (e.g., "abc123.jpg" -> "abc123")
3. Query rule34.xxx API: https://api.rule34.xxx/index.php?page=dapi&s=post&q=index&tags=md5:{hash}
4. Parse XML response and extract tags by type
5. Map to Stashapp fields and return JSON

TAG MAPPING:
- character tags -> Performers
- artist tags -> Studio only (not tagged)
- copyright tags -> Tags
- general tags -> Tags
- meta tags -> Tags

CONTINUATION NOTES:
- API endpoint: https://api.rule34.xxx/index.php?page=dapi&s=post&q=index
- Response format: XML with <post> elements
- Tag format in XML: space-separated string with type prefixes
- If stuck, check rule34.xxx API docs or test with: curl "https://api.rule34.xxx/index.php?page=dapi&s=post&q=index&limit=1"
"""

import json
import sys
import os
import re
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

# API Configuration
API_BASE = "https://api.rule34.xxx/index.php"
API_PARAMS = {
    "page": "dapi",
    "s": "post",
    "q": "index"
}

def log(message):
    """Log to stderr so it doesn't interfere with JSON output"""
    print(f"[Rule34.xxx] {message}", file=sys.stderr)

def load_credentials():
    """
    Load API credentials from environment variables or config file.

    Priority:
    1. Environment variables: R34_API_KEY and R34_USER_ID
    2. Config file: rule34xxx_config.json in same directory as script
    3. Config file: rule34xxx_config.json in current working directory

    Config file format:
    {
        "api_key": "your_key",
        "user_id": "your_id"
    }

    Returns: tuple (api_key, user_id) or (None, None) if not found
    """
    # Try environment variables first
    api_key = os.environ.get("R34_API_KEY")
    user_id = os.environ.get("R34_USER_ID")

    if api_key and user_id:
        log(f"Loaded credentials from environment variables (user_id: {user_id})")
        return api_key, user_id

    # Try config file in script directory
    script_dir = Path(__file__).parent
    config_paths = [
        script_dir / "rule34xxx_config.json",
        Path.cwd() / "rule34xxx_config.json"
    ]

    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    api_key = config.get("api_key")
                    user_id = config.get("user_id")
                    if api_key and user_id:
                        log(f"Loaded credentials from {config_path} (user_id: {user_id})")
                        return api_key, user_id
            except Exception as e:
                log(f"Failed to load config from {config_path}: {e}")

    log("WARNING: No credentials found. Set R34_API_KEY and R34_USER_ID environment variables or create rule34xxx_config.json")
    return None, None

def extract_md5_from_path(file_path):
    """
    Extract md5 hash from filename.

    Assumes filename IS the md5 hash (with or without extension).
    Examples:
        /path/to/abc123def456.jpg -> abc123def456
        /path/to/abc123def456 -> abc123def456
        abc123def456.mp4 -> abc123def456

    Returns: md5 hash string (lowercase, no extension)
    """
    filename = Path(file_path).stem  # Gets filename without extension
    # Remove any non-alphanumeric characters just in case
    md5_hash = re.sub(r'[^a-fA-F0-9]', '', filename)
    return md5_hash.lower()

def query_rule34_api(md5_hash, api_key=None, user_id=None):
    """
    Query rule34.xxx API for post with matching md5.

    API URL: https://api.rule34.xxx/index.php?page=dapi&s=post&q=index&tags=md5:HASH&api_key=KEY&user_id=ID

    Args:
        md5_hash: MD5 hash to search for
        api_key: API key for authentication (required)
        user_id: User ID for authentication (required)

    Returns: XML string or None if request fails
    """
    params = API_PARAMS.copy()
    params["tags"] = f"md5:{md5_hash}"

    # Add authentication if provided
    if api_key:
        params["api_key"] = api_key
    if user_id:
        params["user_id"] = user_id

    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    # Don't log the full URL with API key for security
    log(f"Querying API for md5:{md5_hash} (authenticated: {bool(api_key)})")

    try:
        # Create request with User-Agent header
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "stashapp/stash scraper (rule34.xxx)"}
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read().decode('utf-8')

    except Exception as e:
        log(f"API request failed: {e}")
        return None

def parse_tags_string(tags_string):
    """
    Parse space-separated tags string and categorize by type.

    Rule34.xxx tag format examples:
        - "1girl solo" (general tags, no prefix)
        - "character:name" (character tag)
        - "artist:name" (artist tag)
        - "copyright:series" (copyright/series tag)
        - "meta:tagme" (meta tag)

    Returns: dict with keys: characters, artists, copyrights, general, meta
    """
    tags = {
        "characters": [],
        "artists": [],
        "copyrights": [],
        "general": [],
        "meta": []
    }

    if not tags_string:
        return tags

    for tag in tags_string.split():
        tag = tag.strip()
        if not tag:
            continue

        # Check for type prefix
        if ":" in tag:
            tag_type, tag_name = tag.split(":", 1)
            tag_name = tag_name.replace("_", " ")  # Convert underscores to spaces

            if tag_type == "character":
                tags["characters"].append(tag_name)
            elif tag_type == "artist":
                tags["artists"].append(tag_name)
            elif tag_type == "copyright":
                tags["copyrights"].append(tag_name)
            elif tag_type == "meta":
                tags["meta"].append(tag_name)
            else:
                # Unknown prefix, treat as general
                tags["general"].append(tag.replace("_", " "))
        else:
            # No prefix = general tag
            tags["general"].append(tag.replace("_", " "))

    return tags

def parse_api_response(xml_string):
    """
    Parse XML response from rule34.xxx API.

    Expected XML format:
    <posts count="1" offset="0">
        <post id="12345" tags="tag1 tag2 artist:name" file_url="..."
              score="10" rating="s" width="1920" height="1080" ... />
    </posts>

    Returns: dict with post data or None if no results
    """
    try:
        root = ET.fromstring(xml_string)

        # Check if we got any posts
        posts = root.findall('post')
        if not posts:
            log("No posts found in API response")
            return None

        # Get first post (should only be one for md5 search)
        post = posts[0]

        return {
            "id": post.get("id"),
            "tags": post.get("tags", ""),
            "file_url": post.get("file_url"),
            "score": post.get("score"),
            "rating": post.get("rating"),
            "width": post.get("width"),
            "height": post.get("height"),
            "title": post.get("title", "")
        }

    except Exception as e:
        log(f"Failed to parse XML: {e}")
        return None

def map_to_stashapp(post_data, categorized_tags, md5_hash=None):
    """
    Map rule34.xxx post data to Stashapp scraper output format.

    Stashapp expects JSON with fields:
        - title: string
        - url: string (link to post)
        - performers: [{"name": string}]
        - studio: {"name": string}
        - tags: [{"name": string}]
        - details: string

    Returns: dict in Stashapp format
    """
    result = {}

    # Title (if available)
    if post_data.get("title"):
        result["title"] = post_data["title"]

    # URL - reconstruct post URL from ID or provide md5 search URL
    if post_data.get("id"):
        result["url"] = f"https://rule34.xxx/index.php?page=post&s=view&id={post_data['id']}"
    elif md5_hash:
        result["url"] = f"https://rule34.xxx/index.php?page=post&s=list&tags=md5:{md5_hash}"

    # Performers from character tags
    if categorized_tags["characters"]:
        result["performers"] = [{"name": str(char)} for char in categorized_tags["characters"]]

    # Studio from first artist
    if categorized_tags["artists"]:
        result["studio"] = {"name": str(categorized_tags["artists"][0])}

    # Tags - combine all types
    all_tags = []

    # General tags
    for tag in categorized_tags["general"]:
        all_tags.append({"name": str(tag)})

    # Copyright/series tags
    for series in categorized_tags["copyrights"]:
        all_tags.append({"name": str(series)})

    # Meta tags
    for meta in categorized_tags["meta"]:
        all_tags.append({"name": str(meta)})

    # Add rating as tag
    if post_data.get("rating"):
        rating_map = {"s": "safe", "q": "questionable", "e": "explicit"}
        rating = rating_map.get(post_data["rating"], post_data["rating"])
        all_tags.append({"name": rating})

    if all_tags:
        result["tags"] = all_tags

    # Details - include score and dimensions
    details = []
    if post_data.get("score"):
        details.append(f"Score: {post_data['score']}")
    if post_data.get("width") and post_data.get("height"):
        details.append(f"Dimensions: {post_data['width']}x{post_data['height']}")

    if details:
        result["details"] = " | ".join(details)

    return result

def main():
    """
    Main scraper entry point.

    Stashapp passes JSON input via stdin with structure:
    {
        "path": "/path/to/file.jpg",  # For fragment scraping
        "url": "https://..."           # For URL scraping (not used yet)
    }

    Returns: JSON output to stdout with scraped metadata
    """
    try:
        # Load API credentials
        api_key, user_id = load_credentials()
        if not api_key or not user_id:
            log("ERROR: Missing API credentials. Cannot query rule34.xxx")
            print(json.dumps({}))
            return

        # Read input from Stashapp
        input_data = json.load(sys.stdin)
        log(f"Received input: {input_data}")

        # Extract file path from different input formats
        file_path = None
        
        # Try different input fields in order of preference
        if input_data.get("files") and len(input_data["files"]) > 0:
            file_path = input_data["files"][0].get("path")
        if not file_path:
            file_path = input_data.get("path")
        if not file_path:
            file_path = input_data.get("url")
        if not file_path:
            file_path = input_data.get("title")
        
        if not file_path:
            log("No filename/path/url/title in input")
            print(json.dumps({}))
            return

        # Extract md5 from filename
        md5_hash = extract_md5_from_path(file_path)
        log(f"Extracted md5: {md5_hash}")

        if not md5_hash:
            log("Could not extract md5 from filename")
            print(json.dumps({}))
            return

        # Query API with authentication
        xml_response = query_rule34_api(md5_hash, api_key, user_id)
        if not xml_response:
            log("API query failed")
            print(json.dumps({}))
            return

        # Parse response
        post_data = parse_api_response(xml_response)
        if not post_data:
            log("No matching post found - returning md5 search URL")
            result = {"url": f"https://rule34.xxx/index.php?page=post&s=list&tags=md5:{md5_hash}"}
            print(json.dumps(result))
            return

        # Categorize tags
        categorized_tags = parse_tags_string(post_data["tags"])
        log(f"Categorized tags: {categorized_tags}")

        # Map to Stashapp format
        result = map_to_stashapp(post_data, categorized_tags, md5_hash)

        # Output JSON to stdout
        print(json.dumps(result))
        log("Scrape successful!")

    except Exception as e:
        log(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Return minimal data on error
        print(json.dumps({}))

if __name__ == "__main__":
    main()
