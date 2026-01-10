#!/usr/bin/env python3
"""
Rule34.xxx HTML Scraper for Stashapp
=====================================

Hybrid approach:
1. Use API to find post ID by md5 hash (fast, requires auth)
2. Fetch HTML page to extract categorized tags (no auth needed)

This allows proper tag categorization:
- Characters → Performers
- Artists → Studio + r34:artist: tags
- Copyright → r34:series: tags
- General → r34: tags
- Meta → r34:meta: tags
"""

import json
import sys
import os
import re
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
import time

# API Configuration
API_BASE = "https://api.rule34.xxx/index.php"
API_PARAMS = {"page": "dapi", "s": "post", "q": "index"}

def log(message):
    """Log to stderr so it doesn't interfere with JSON output"""
    print(f"[Rule34.xxx HTML] {message}", file=sys.stderr)

def load_credentials():
    """Load API credentials from environment variables or config file"""
    api_key = os.environ.get("R34_API_KEY")
    user_id = os.environ.get("R34_USER_ID")

    if api_key and user_id:
        log(f"Loaded credentials from environment (user_id: {user_id})")
        return api_key, user_id

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
                        log(f"Loaded credentials from {config_path}")
                        return api_key, user_id
            except Exception as e:
                log(f"Failed to load config: {e}")

    return None, None

def extract_post_id_from_filename(file_path):
    """
    Extract post ID from filename if it follows r34_{POST_ID}_* format.

    Supports formats like:
    - r34_12345_artist.jpg
    - r34_12345_01.png
    - r34_12345.mp4

    Returns: post_id (string) or None
    """
    filename = Path(file_path).stem

    # Match r34_{digits}_{anything} or r34_{digits}
    match = re.match(r'^r34_(\d+)', filename)
    if match:
        post_id = match.group(1)
        log(f"Extracted post ID from filename: {post_id}")
        return post_id

    return None

def extract_md5_from_path(file_path):
    """Extract md5 hash from filename"""
    filename = Path(file_path).stem
    md5_hash = re.sub(r'[^a-fA-F0-9]', '', filename)
    return md5_hash.lower()

def get_post_id_from_md5(md5_hash, api_key, user_id):
    """Use API to get post ID from md5 hash"""
    params = API_PARAMS.copy()
    params["tags"] = f"md5:{md5_hash}"
    params["api_key"] = api_key
    params["user_id"] = user_id

    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    log(f"Querying API for post ID (md5:{md5_hash})")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "stashapp/stash scraper"})
        with urllib.request.urlopen(req, timeout=10) as response:
            xml_response = response.read().decode('utf-8')
            root = ET.fromstring(xml_response)
            posts = root.findall('post')
            if not posts:
                return None, None

            post = posts[0]
            post_id = post.get("id")
            score = post.get("score")
            width = post.get("width")
            height = post.get("height")
            rating = post.get("rating")

            log(f"Found post ID: {post_id}")
            return post_id, {
                "score": score,
                "width": width,
                "height": height,
                "rating": rating
            }
    except Exception as e:
        log(f"API query failed: {e}")
        return None, None

class Rule34TagParser(HTMLParser):
    """Parse rule34.xxx HTML to extract categorized tags"""

    def __init__(self):
        super().__init__()
        self.tags = {
            "characters": [],
            "artists": [],
            "copyrights": [],
            "general": [],
            "meta": []
        }
        self.current_tag_type = None
        self.current_tag_link = False
        self.current_tag_name = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)

        # Look for tag list items with class indicating type
        if tag == "li":
            class_name = attrs_dict.get("class", "")

            # Map class names to tag types
            if "tag-type-character" in class_name or "character" in class_name:
                self.current_tag_type = "characters"
            elif "tag-type-artist" in class_name or "artist" in class_name:
                self.current_tag_type = "artists"
            elif "tag-type-copyright" in class_name or "copyright" in class_name or "series" in class_name:
                self.current_tag_type = "copyrights"
            elif "tag-type-metadata" in class_name or "meta" in class_name:
                self.current_tag_type = "meta"
            elif "tag-type-general" in class_name or "tag" in class_name:
                self.current_tag_type = "general"

        # Track when we're inside a tag link
        if tag == "a" and self.current_tag_type:
            self.current_tag_link = True
            self.current_tag_name = ""

    def handle_data(self, data):
        # Capture tag name from link text
        if self.current_tag_link:
            self.current_tag_name += data.strip()

    def handle_endtag(self, tag):
        # When link ends, save the tag
        if tag == "a" and self.current_tag_link and self.current_tag_name:
            tag_name = self.current_tag_name.replace("_", " ").strip()
            # Filter out empty, "?", and invalid tags
            if tag_name and tag_name != "?" and len(tag_name) > 1 and self.current_tag_type:
                # Avoid duplicates
                if tag_name not in self.tags[self.current_tag_type]:
                    self.tags[self.current_tag_type].append(tag_name)
            self.current_tag_link = False
            self.current_tag_name = ""

        # Reset tag type when list item ends
        if tag == "li":
            self.current_tag_type = None

def scrape_html_tags(post_id, retry_count=3):
    """
    Fetch HTML page and extract categorized tags with retry logic.

    Returns:
        - dict with tags if successful
        - None if temporary failure (rate limit, timeout, server error)
        - False if permanent failure (404, post doesn't exist)
    """
    url = f"https://rule34.xxx/index.php?page=post&s=view&id={post_id}"
    log(f"Fetching HTML from {url}")

    for attempt in range(retry_count):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "stashapp/stash scraper",
                "Accept": "text/html"
            })
            with urllib.request.urlopen(req, timeout=15) as response:
                html = response.read().decode('utf-8')

            # Check if post actually exists (404 page might return 200)
            if "Nobody here but us chickens" in html or "Post not found" in html:
                log(f"Post {post_id} does not exist (404)")
                return False  # Permanent failure

            parser = Rule34TagParser()
            parser.feed(html)

            # Verify we got some tags (sanity check)
            total_tags = sum(len(tags) for tags in parser.tags.values())
            if total_tags == 0:
                log(f"Warning: No tags extracted, might be parsing issue")

            log(f"Extracted tags: {len(parser.tags['characters'])} characters, "
                f"{len(parser.tags['artists'])} artists, "
                f"{len(parser.tags['copyrights'])} copyrights, "
                f"{len(parser.tags['general'])} general, "
                f"{len(parser.tags['meta'])} meta")

            return parser.tags

        except urllib.error.HTTPError as e:
            if e.code == 404:
                log(f"Post {post_id} not found (404)")
                return False  # Permanent failure
            elif e.code == 429:
                log(f"Rate limited (429), attempt {attempt + 1}/{retry_count}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    continue
                else:
                    log("Rate limit exceeded, giving up")
                    return None  # Temporary failure - don't tag
            elif e.code >= 500:
                log(f"Server error ({e.code}), attempt {attempt + 1}/{retry_count}")
                if attempt < retry_count - 1:
                    time.sleep(1)
                    continue
                else:
                    log("Server errors persist, giving up")
                    return None  # Temporary failure
            else:
                log(f"HTTP error {e.code}: {e}")
                return None  # Other HTTP error - temporary failure

        except urllib.error.URLError as e:
            log(f"Network error, attempt {attempt + 1}/{retry_count}: {e}")
            if attempt < retry_count - 1:
                time.sleep(1)
                continue
            else:
                log("Network errors persist, giving up")
                return None  # Temporary failure

        except Exception as e:
            log(f"Unexpected error: {e}")
            return None  # Temporary failure

    return None  # All retries failed

def map_to_stashapp(post_id, metadata, categorized_tags):
    """Map scraped data to Stashapp format"""
    result = {}

    # URL
    result["url"] = f"https://rule34.xxx/index.php?page=post&s=view&id={post_id}"

    # Performers from characters
    if categorized_tags["characters"]:
        result["performers"] = [{"name": char} for char in categorized_tags["characters"]]
        log(f"Mapped {len(categorized_tags['characters'])} performers")

    # Studio from first artist
    if categorized_tags["artists"]:
        result["studio"] = {"name": categorized_tags["artists"][0]}
        log(f"Mapped studio: {categorized_tags['artists'][0]}")

    # Tags
    all_tags = []

    # General tags
    for tag in categorized_tags["general"]:
        all_tags.append({"name": f"r34:{tag}"})

    # Artist tags
    for artist in categorized_tags["artists"]:
        all_tags.append({"name": f"r34:artist:{artist}"})

    # Copyright/series tags
    for series in categorized_tags["copyrights"]:
        all_tags.append({"name": f"r34:series:{series}"})

    # Meta tags
    for meta in categorized_tags["meta"]:
        all_tags.append({"name": f"r34:meta:{meta}"})

    # Rating tag
    if metadata.get("rating"):
        rating_map = {"s": "safe", "q": "questionable", "e": "explicit"}
        rating = rating_map.get(metadata["rating"], metadata["rating"])
        all_tags.append({"name": f"r34:rating:{rating}"})

    if all_tags:
        result["tags"] = all_tags
        log(f"Mapped {len(all_tags)} total tags")

    # Details
    details = []
    if metadata.get("score"):
        details.append(f"Score: {metadata['score']}")
    if metadata.get("width") and metadata.get("height"):
        details.append(f"Dimensions: {metadata['width']}x{metadata['height']}")

    if details:
        result["details"] = " | ".join(details)

    return result

def main():
    """Main scraper entry point"""
    try:
        # Read input
        input_data = json.load(sys.stdin)
        log(f"Received input: {input_data}")

        # Extract file path from different input formats
        file_path = input_data.get("path") or input_data.get("url")
        
        # For sceneByFragment/imageByFragment, path is in files array
        if not file_path and "files" in input_data and input_data["files"]:
            file_path = input_data["files"][0].get("path")
        
        if not file_path:
            log("No path provided - returning empty")
            print(json.dumps({}))
            return

        # Try to extract post ID from filename first (r34_* format)
        post_id = extract_post_id_from_filename(file_path)
        metadata = {}

        if post_id:
            # Direct scraping from post ID - no API key needed!
            log(f"Using post ID from filename: {post_id}")
        else:
            # Fall back to md5 lookup - requires API credentials
            log("No post ID in filename, trying md5 lookup")

            # Load credentials for API lookup
            api_key, user_id = load_credentials()
            if not api_key or not user_id:
                log("ERROR: Missing API credentials (required for md5 lookup)")
                log("TIP: Use r34_{POST_ID}_* filename format to skip API requirement")
                # Return empty - user needs to set up credentials or rename files
                print(json.dumps({}))
                return

            # Extract md5
            md5_hash = extract_md5_from_path(file_path)
            log(f"Extracted md5: {md5_hash}")

            if not md5_hash:
                log("Could not extract md5 - returning empty")
                print(json.dumps({}))
                return

            # Get post ID from API
            post_id, metadata = get_post_id_from_md5(md5_hash, api_key, user_id)
            if not post_id:
                log("No matching post found in API - returning empty")
                print(json.dumps({}))
                return

        # Scrape HTML for categorized tags (works with or without API)
        categorized_tags = scrape_html_tags(post_id)

        if categorized_tags is None:
            # Temporary failure (rate limit, timeout, server error)
            # Return empty result - don't pollute library with error tags
            log("Temporary failure (rate limit or server error) - returning empty")
            print(json.dumps({}))
            return
        elif categorized_tags is False:
            # Permanent failure (404, post doesn't exist)
            log("Post does not exist (404) - returning empty")
            print(json.dumps({}))
            return
        elif not categorized_tags:
            # Empty dict or other falsy value
            log("No tags extracted - returning empty")
            print(json.dumps({}))
            return

        # Map to Stashapp format
        result = map_to_stashapp(post_id, metadata, categorized_tags)

        print(json.dumps(result))
        log("Scrape successful!")

    except Exception as e:
        log(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Don't tag with error - return empty to avoid polluting library
        print(json.dumps({}))

if __name__ == "__main__":
    main()
