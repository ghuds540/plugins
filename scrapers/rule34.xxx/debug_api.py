#!/usr/bin/env python3
"""
Debug script to inspect raw rule34.xxx API responses.

Usage:
    python debug_api.py <md5_hash>

Example:
    python debug_api.py 87cd73705967e775ed4d5dfc4b9bbbf4
"""

import sys
import json
import urllib.request
import urllib.parse
from pathlib import Path

def load_credentials():
    """Load credentials from config file"""
    config_path = Path(__file__).parent / "rule34xxx_config.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get("api_key"), config.get("user_id")
    return None, None

def main():
    if len(sys.argv) < 2:
        print("Usage: python debug_api.py <md5_hash>")
        print("Example: python debug_api.py 87cd73705967e775ed4d5dfc4b9bbbf4")
        sys.exit(1)

    md5_hash = sys.argv[1]
    api_key, user_id = load_credentials()

    if not api_key or not user_id:
        print("ERROR: No credentials found in rule34xxx_config.json")
        sys.exit(1)

    # Build API URL
    params = {
        "page": "dapi",
        "s": "post",
        "q": "index",
        "tags": f"md5:{md5_hash}",
        "api_key": api_key,
        "user_id": user_id
    }

    url = f"https://api.rule34.xxx/index.php?{urllib.parse.urlencode(params)}"
    print(f"Querying: {url}\n")
    print("=" * 80)

    # Make request
    req = urllib.request.Request(url, headers={"User-Agent": "debug script"})
    with urllib.request.urlopen(req, timeout=10) as response:
        xml_response = response.read().decode('utf-8')
        print(xml_response)
        print("=" * 80)

    # Also try to parse and show structure
    print("\nParsing XML structure...\n")
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_response)

    for post in root.findall('post'):
        print("POST ATTRIBUTES:")
        for key, value in sorted(post.attrib.items()):
            # Truncate long values
            if len(value) > 200:
                value = value[:200] + "..."
            print(f"  {key}: {value}")

if __name__ == "__main__":
    main()
