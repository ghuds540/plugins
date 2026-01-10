#!/usr/bin/env python3
"""
Rule34 Wiki to Stash Tag Description Sync Tool

Scrapes tag descriptions from Rule34 wiki and updates matching tags in Stash.
"""

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_STASH_URL = "http://localhost:9999"
DEFAULT_RULE34_URL = "https://rule34.xxx"
DEFAULT_RATE_LIMIT = 1.0  # seconds between requests
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 2  # exponential backoff multiplier

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(verbose: bool, debug: bool) -> logging.Logger:
    """Configure logging based on verbosity level."""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger("rule34_stash_tagger")
    logger.setLevel(level)
    logger.addHandler(handler)
    
    return logger

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class StashTag:
    """Represents a tag from Stash."""
    id: str
    name: str
    description: Optional[str] = None
    
    @property
    def has_description(self) -> bool:
        return bool(self.description and self.description.strip())

@dataclass
class WikiResult:
    """Result from wiki scrape attempt."""
    tag_name: str
    description: Optional[str] = None
    success: bool = False
    error: Optional[str] = None

@dataclass
class UpdateResult:
    """Result from a tag update attempt."""
    tag: StashTag
    wiki_result: WikiResult
    updated: bool = False
    skipped: bool = False
    skip_reason: Optional[str] = None
    error: Optional[str] = None

# ============================================================================
# HTTP Session with Retry Logic
# ============================================================================

def create_session(max_retries: int = MAX_RETRIES, backoff_factor: float = BACKOFF_FACTOR) -> requests.Session:
    """Create a requests session with retry logic for transient errors."""
    session = requests.Session()
    
    # Retry on these status codes
    status_forcelist = [
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    ]
    
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,  # Don't raise, let us handle status codes
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set a reasonable user agent
    session.headers.update({
        "User-Agent": "Rule34-Stash-Tagger/1.0 (Tag Description Sync Tool)"
    })
    
    return session

# ============================================================================
# Stash API Client
# ============================================================================

class StashClient:
    """Client for interacting with Stash GraphQL API."""
    
    def __init__(self, base_url: str, api_key: Optional[str], session: requests.Session, 
                 logger: logging.Logger, timeout: int = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.graphql_url = f"{self.base_url}/graphql"
        self.session = session
        self.logger = logger
        self.timeout = timeout
        
        if api_key:
            self.session.headers["ApiKey"] = api_key
    
    def _execute_query(self, query: str, variables: Optional[dict] = None) -> dict:
        """Execute a GraphQL query against Stash."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        self.logger.debug(f"Executing GraphQL query: {query[:100]}...")
        
        try:
            response = self.session.post(
                self.graphql_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                raise Exception(f"GraphQL errors: {data['errors']}")
            
            return data.get("data", {})
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Stash API request failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test connection to Stash."""
        try:
            query = "query { systemStatus { databaseSchema } }"
            self._execute_query(query)
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Stash: {e}")
            return False
    
    def get_all_tags(self) -> list[StashTag]:
        """Fetch all tags from Stash."""
        query = """
        query FindTags($filter: FindFilterType) {
            findTags(filter: $filter) {
                count
                tags {
                    id
                    name
                    description
                }
            }
        }
        """
        
        # Fetch all tags (use a high per_page value)
        variables = {
            "filter": {
                "per_page": -1  # -1 means all
            }
        }
        
        data = self._execute_query(query, variables)
        tags_data = data.get("findTags", {}).get("tags", [])
        
        tags = [
            StashTag(
                id=t["id"],
                name=t["name"],
                description=t.get("description")
            )
            for t in tags_data
        ]
        
        self.logger.info(f"Fetched {len(tags)} tags from Stash")
        return tags
    
    def get_tags_by_names(self, names: list[str]) -> list[StashTag]:
        """Fetch specific tags by name."""
        all_tags = self.get_all_tags()
        name_set = {n.lower() for n in names}
        return [t for t in all_tags if t.name.lower() in name_set]
    
    def update_tag_description(self, tag_id: str, description: str) -> bool:
        """Update a tag's description in Stash."""
        mutation = """
        mutation TagUpdate($input: TagUpdateInput!) {
            tagUpdate(input: $input) {
                id
                description
            }
        }
        """
        
        variables = {
            "input": {
                "id": tag_id,
                "description": description
            }
        }
        
        try:
            self._execute_query(mutation, variables)
            return True
        except Exception as e:
            self.logger.error(f"Failed to update tag {tag_id}: {e}")
            return False

# ============================================================================
# Rule34 Wiki Scraper
# ============================================================================

class Rule34WikiScraper:
    """Scraper for Rule34 wiki tag descriptions."""
    
    def __init__(self, base_url: str, session: requests.Session, logger: logging.Logger,
                 rate_limit: float = DEFAULT_RATE_LIMIT, timeout: int = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.logger = logger
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.last_request_time = 0.0
    
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _normalize_tag_name(self, tag_name: str) -> str:
        """Normalize tag name for wiki lookup (spaces to underscores, lowercase)."""
        return tag_name.strip().lower().replace(" ", "_")
    
    def _clean_description(self, text: str) -> str:
        """Clean up extracted description text."""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        # Remove "edit" links and similar artifacts
        text = re.sub(r'\[edit\]', '', text, flags=re.IGNORECASE)
        
        # Remove Rule34 wiki page header: "Now Viewing: tagname Tag type: Type"
        text = re.sub(r'^Now Viewing:\s*\S+\s*Tag type:\s*\w+\s*', '', text, flags=re.IGNORECASE)
        
        # Remove wiki footer: "Other Wiki Information Last updated: ..." to end
        text = re.sub(r'\s*Other Wiki Information\s*Last updated:.*$', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Also handle "This entry is locked..." which sometimes appears
        text = re.sub(r'\s*This entry is locked.*$', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove "View more »" if present
        text = re.sub(r'\s*View more\s*[»>]?\s*$', '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def scrape_tag(self, tag_name: str) -> WikiResult:
        """Scrape description for a single tag from Rule34 wiki."""
        normalized_name = self._normalize_tag_name(tag_name)
        
        # Step 1: Search for the wiki page to find its ID
        search_url = f"{self.base_url}/index.php?page=wiki&s=list&search={quote(normalized_name)}"
        
        self.logger.debug(f"Searching wiki for tag '{tag_name}': {search_url}")
        self._wait_for_rate_limit()
        
        try:
            response = self.session.get(search_url, timeout=self.timeout)
            
            if response.status_code == 429:
                self.logger.warning(f"Rate limited while searching '{tag_name}'")
                return WikiResult(tag_name=tag_name, success=False, error="Rate limited")
            
            if response.status_code >= 500:
                return WikiResult(tag_name=tag_name, success=False, 
                                error=f"Server error: {response.status_code}")
            
            if response.status_code != 200:
                return WikiResult(tag_name=tag_name, success=False,
                                error=f"HTTP {response.status_code}")
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find the wiki page link for this exact tag
            wiki_id = self._find_wiki_id(soup, normalized_name)
            
            if not wiki_id:
                self.logger.debug(f"No wiki page found for tag '{tag_name}'")
                return WikiResult(tag_name=tag_name, success=False, error="No wiki page found for tag")
            
            # Step 2: Fetch the actual wiki page by ID
            self._wait_for_rate_limit()
            wiki_url = f"{self.base_url}/index.php?page=wiki&s=view&id={wiki_id}"
            self.logger.debug(f"Fetching wiki page: {wiki_url}")
            
            response = self.session.get(wiki_url, timeout=self.timeout)
            
            if response.status_code != 200:
                return WikiResult(tag_name=tag_name, success=False,
                                error=f"HTTP {response.status_code} fetching wiki page")
            
            soup = BeautifulSoup(response.text, "html.parser")
            description = self._extract_description(soup, tag_name)
            
            if description:
                self.logger.debug(f"Found description for '{tag_name}': {description[:100]}...")
                return WikiResult(tag_name=tag_name, description=description, success=True)
            else:
                self.logger.debug(f"No description content found for '{tag_name}'")
                return WikiResult(tag_name=tag_name, success=False, 
                                error="No description found on wiki page")
                
        except requests.exceptions.Timeout:
            self.logger.warning(f"Timeout while fetching wiki for '{tag_name}'")
            return WikiResult(tag_name=tag_name, success=False, error="Request timeout")
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for '{tag_name}': {e}")
            return WikiResult(tag_name=tag_name, success=False, error=str(e))
    
    def _find_wiki_id(self, soup: BeautifulSoup, normalized_tag: str) -> Optional[str]:
        """Find the wiki page ID for a tag from search results."""
        # Look for links to wiki pages in the search results
        # Format: index.php?page=wiki&s=view&id=XXXXX
        
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            link_text = link.get_text().strip().lower().replace(" ", "_")
            
            # Check if this is a wiki view link
            if "page=wiki" in href and "s=view" in href and "id=" in href:
                # Check if the link text matches our tag
                if link_text == normalized_tag:
                    # Extract the ID from the URL
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        return match.group(1)
        
        # If exact match not found, try to find partial match as fallback
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            link_text = link.get_text().strip().lower().replace(" ", "_")
            
            if "page=wiki" in href and "s=view" in href and "id=" in href:
                # Check for partial match (tag might have slightly different name)
                if normalized_tag in link_text or link_text in normalized_tag:
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        self.logger.debug(f"Using partial match: '{link_text}' for '{normalized_tag}'")
                        return match.group(1)
        
        return None
    
    def _extract_description(self, soup: BeautifulSoup, tag_name: str) -> Optional[str]:
        """Extract tag description from parsed wiki page."""
        
        # Remove known non-content elements first
        for elem in soup.select("script, style, #header, #navbar, #subnavbar, #paginator, .sidebar"):
            elem.decompose()
        
        # Method 1: Look for wiki body content - Rule34 uses table layouts
        # The description is usually in a td cell
        content_div = soup.find("div", {"id": "content"})
        if content_div:
            for td in content_div.find_all("td"):
                text = td.get_text().strip()
                
                # Skip metadata cells
                if len(text) < 30:
                    continue
                if text.startswith("Version"):
                    continue
                if "Last updated" in text:
                    continue
                if "Recent Changes" in text:
                    continue
                if "Reset cookie" in text:
                    continue
                    
                cleaned = self._clean_description(text)
                if len(cleaned) > 30:
                    return cleaned
        
        # Method 2: Look for paragraphs with real content
        for p in soup.find_all("p"):
            text = p.get_text().strip()
            if len(text) > 50:
                if "Reset cookie" not in text and "GDPR" not in text:
                    return self._clean_description(text)
        
        # Method 3: Look for any substantial text in divs
        for div in soup.find_all("div"):
            div_id = div.get("id", "")
            div_class = " ".join(div.get("class", []))
            
            # Skip navigation/chrome
            skip_ids = ["header", "navbar", "subnavbar", "sidebar", "paginator", "notice", "footer"]
            skip_classes = ["sidebar", "notice", "pagination"]
            
            if any(x in div_id.lower() for x in skip_ids):
                continue
            if any(x in div_class.lower() for x in skip_classes):
                continue
            
            text = div.get_text().strip()
            if 50 < len(text) < 3000:
                cleaned = self._clean_description(text)
                if "Recent Changes" not in cleaned and "Reset cookie" not in cleaned:
                    if len(cleaned.split()) > 10:
                        return cleaned
        
        return None

# ============================================================================
# Main Sync Logic
# ============================================================================

class TagSyncer:
    """Orchestrates the sync between Rule34 wiki and Stash."""
    
    def __init__(self, stash_client: StashClient, wiki_scraper: Rule34WikiScraper,
                 logger: logging.Logger, dry_run: bool = False, 
                 skip_existing: bool = True, force: bool = False):
        self.stash = stash_client
        self.wiki = wiki_scraper
        self.logger = logger
        self.dry_run = dry_run
        self.skip_existing = skip_existing
        self.force = force
        
        # Statistics
        self.stats = {
            "total": 0,
            "updated": 0,
            "skipped": 0,
            "not_found": 0,
            "errors": 0,
        }
    
    def sync_tag(self, tag: StashTag) -> UpdateResult:
        """Sync a single tag."""
        self.stats["total"] += 1
        
        # Check if we should skip existing descriptions
        if self.skip_existing and tag.has_description and not self.force:
            self.stats["skipped"] += 1
            self.logger.info(f"Skipping '{tag.name}' - already has description")
            return UpdateResult(
                tag=tag,
                wiki_result=WikiResult(tag_name=tag.name),
                skipped=True,
                skip_reason="Already has description"
            )
        
        # Scrape wiki
        wiki_result = self.wiki.scrape_tag(tag.name)
        
        if not wiki_result.success or not wiki_result.description:
            self.stats["not_found"] += 1
            self.logger.info(f"No wiki description found for '{tag.name}': {wiki_result.error}")
            return UpdateResult(
                tag=tag,
                wiki_result=wiki_result,
                skipped=True,
                skip_reason=wiki_result.error or "No description found"
            )
        
        # Dry run - don't actually update
        if self.dry_run:
            self.stats["updated"] += 1
            self.logger.info(f"[DRY RUN] Would update '{tag.name}' with: {wiki_result.description[:100]}...")
            return UpdateResult(
                tag=tag,
                wiki_result=wiki_result,
                updated=True
            )
        
        # Actually update
        success = self.stash.update_tag_description(tag.id, wiki_result.description)
        
        if success:
            self.stats["updated"] += 1
            self.logger.info(f"Updated '{tag.name}' with wiki description")
            return UpdateResult(tag=tag, wiki_result=wiki_result, updated=True)
        else:
            self.stats["errors"] += 1
            return UpdateResult(tag=tag, wiki_result=wiki_result, error="Failed to update in Stash")
    
    def sync_tags(self, tags: list[StashTag]) -> list[UpdateResult]:
        """Sync multiple tags."""
        results = []
        total = len(tags)
        
        for i, tag in enumerate(tags, 1):
            self.logger.info(f"Processing tag {i}/{total}: '{tag.name}'")
            result = self.sync_tag(tag)
            results.append(result)
        
        return results
    
    def print_summary(self):
        """Print sync statistics."""
        print("\n" + "=" * 60)
        print("SYNC SUMMARY")
        print("=" * 60)
        print(f"  Total tags processed: {self.stats['total']}")
        print(f"  Successfully updated: {self.stats['updated']}")
        print(f"  Skipped (existing):   {self.stats['skipped']}")
        print(f"  Not found in wiki:    {self.stats['not_found']}")
        print(f"  Errors:               {self.stats['errors']}")
        print("=" * 60)

# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync tag descriptions from Rule34 wiki to Stash",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for all tags (see what would be updated)
  %(prog)s --dry-run

  # Test with specific tags
  %(prog)s --tags "pokemon,naruto,original_character" --dry-run

  # Update all tags (skip those with existing descriptions)
  %(prog)s

  # Force update all tags (overwrite existing descriptions)
  %(prog)s --force

  # Update specific tags only
  %(prog)s --tags "tag1,tag2,tag3"

  # Verbose output for debugging
  %(prog)s --verbose --dry-run

  # Custom Stash URL and API key
  %(prog)s --stash-url http://192.168.1.100:9999 --api-key your_key_here
        """
    )
    
    # Connection settings
    conn_group = parser.add_argument_group("Connection Settings")
    conn_group.add_argument(
        "--stash-url",
        default=DEFAULT_STASH_URL,
        help=f"Stash server URL (default: {DEFAULT_STASH_URL})"
    )
    conn_group.add_argument(
        "--api-key",
        help="Stash API key (if authentication is enabled)"
    )
    conn_group.add_argument(
        "--rule34-url",
        default=DEFAULT_RULE34_URL,
        help=f"Rule34 base URL (default: {DEFAULT_RULE34_URL})"
    )
    
    # Tag selection
    tag_group = parser.add_argument_group("Tag Selection")
    tag_group.add_argument(
        "--tags", "-t",
        help="Comma-separated list of specific tags to process (for testing)"
    )
    tag_group.add_argument(
        "--limit", "-l",
        type=int,
        help="Limit number of tags to process (for testing)"
    )
    
    # Behavior settings
    behavior_group = parser.add_argument_group("Behavior")
    behavior_group.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Don't actually update tags, just show what would be done"
    )
    behavior_group.add_argument(
        "--force", "-f",
        action="store_true",
        help="Overwrite existing descriptions"
    )
    behavior_group.add_argument(
        "--include-existing",
        action="store_true",
        help="Process tags that already have descriptions (use with --force to overwrite)"
    )
    
    # Rate limiting
    rate_group = parser.add_argument_group("Rate Limiting")
    rate_group.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help=f"Seconds between wiki requests (default: {DEFAULT_RATE_LIMIT})"
    )
    rate_group.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT})"
    )
    rate_group.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help=f"Maximum retries for failed requests (default: {MAX_RETRIES})"
    )
    
    # Output settings
    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    output_group.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output (very verbose)"
    )
    output_group.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    
    # Utility
    util_group = parser.add_argument_group("Utility")
    util_group.add_argument(
        "--test-connection",
        action="store_true",
        help="Test connection to Stash and exit"
    )
    util_group.add_argument(
        "--test-wiki",
        metavar="TAG",
        help="Test wiki scraping for a specific tag and exit"
    )
    
    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose, args.debug)
    
    # Create HTTP session
    session = create_session(max_retries=args.max_retries)
    
    # Create clients
    stash = StashClient(
        base_url=args.stash_url,
        api_key=args.api_key,
        session=session,
        logger=logger,
        timeout=args.timeout
    )
    
    wiki = Rule34WikiScraper(
        base_url=args.rule34_url,
        session=session,
        logger=logger,
        rate_limit=args.rate_limit,
        timeout=args.timeout
    )
    
    # Handle utility modes
    if args.test_connection:
        print(f"Testing connection to Stash at {args.stash_url}...")
        if stash.test_connection():
            print("✓ Connection successful!")
            sys.exit(0)
        else:
            print("✗ Connection failed!")
            sys.exit(1)
    
    if args.test_wiki:
        print(f"Testing wiki scrape for tag: {args.test_wiki}")
        result = wiki.scrape_tag(args.test_wiki)
        print(f"\nResult:")
        print(f"  Success: {result.success}")
        if result.description:
            print(f"  Description: {result.description[:500]}...")
        if result.error:
            print(f"  Error: {result.error}")
        sys.exit(0 if result.success else 1)
    
    # Test Stash connection first
    print(f"Connecting to Stash at {args.stash_url}...")
    if not stash.test_connection():
        print("Failed to connect to Stash. Check URL and API key.")
        sys.exit(1)
    print("✓ Connected to Stash\n")
    
    # Get tags to process
    if args.tags:
        tag_names = [t.strip() for t in args.tags.split(",")]
        print(f"Fetching specified tags: {tag_names}")
        tags = stash.get_tags_by_names(tag_names)
        
        if not tags:
            print("No matching tags found in Stash!")
            sys.exit(1)
        
        # Warn about missing tags
        found_names = {t.name.lower() for t in tags}
        for name in tag_names:
            if name.lower() not in found_names:
                print(f"  Warning: Tag '{name}' not found in Stash")
    else:
        print("Fetching all tags from Stash...")
        tags = stash.get_all_tags()
    
    # Apply limit if specified
    if args.limit:
        tags = tags[:args.limit]
        print(f"Limited to first {args.limit} tags")
    
    print(f"Processing {len(tags)} tags\n")
    
    if args.dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("=" * 60 + "\n")
    
    # Create syncer and run
    syncer = TagSyncer(
        stash_client=stash,
        wiki_scraper=wiki,
        logger=logger,
        dry_run=args.dry_run,
        skip_existing=not args.include_existing,
        force=args.force
    )
    
    results = syncer.sync_tags(tags)
    
    # Output results
    if args.json:
        output = {
            "stats": syncer.stats,
            "results": [
                {
                    "tag_name": r.tag.name,
                    "tag_id": r.tag.id,
                    "updated": r.updated,
                    "skipped": r.skipped,
                    "skip_reason": r.skip_reason,
                    "error": r.error,
                    "wiki_description": r.wiki_result.description[:200] if r.wiki_result.description else None
                }
                for r in results
            ]
        }
        print(json.dumps(output, indent=2))
    else:
        syncer.print_summary()

if __name__ == "__main__":
    main()
