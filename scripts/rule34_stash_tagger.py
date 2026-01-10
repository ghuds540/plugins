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
        """Clean up extracted description text.
        
        Handles various Rule34 wiki formatting edge cases:
        - Page headers (Now Viewing, Tag type)
        - Wiki footers (Other Wiki Information, Last updated)
        - Boilerplate text (edit notices, locked/unlocked entry messages)
        - DText formatting artifacts (h4. headers, links)
        - Related tag dumps that follow descriptions
        - Site chrome (GDPR consent, pagination)
        """
        # First pass: normalize whitespace but preserve line breaks for later processing
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n', text)
        text = text.strip()
        
        # Remove "edit" links and similar artifacts
        text = re.sub(r'\[edit\]', '', text, flags=re.IGNORECASE)
        
        # ============================================================
        # Remove page headers
        # ============================================================
        # Format: "Now Viewing: tagname Tag type: TypeDescription..." 
        # We need to remove header but KEEP the description that follows
        # The description often starts with an article (A, An, The) or proper noun
        
        # First try combined pattern - match known tag types to avoid eating description
        header_match = re.match(
            r'^Now Viewing:\s*\S+\s*Tag type:\s*'
            r'(General|Character|Copyright|Artist|Meta|Ambiguous|Lore)\s*',
            text,
            flags=re.IGNORECASE
        )
        if header_match:
            text = text[header_match.end():]
        else:
            # Try separate patterns
            text = re.sub(r'^Now Viewing:\s*\S+\s*', '', text, flags=re.IGNORECASE)
            # For Tag type, only remove if followed by whitespace to avoid eating content
            text = re.sub(r'^Tag type:\s*(General|Character|Copyright|Artist|Meta|Ambiguous|Lore)\s+', '', text, flags=re.IGNORECASE)
            # If no match, try simpler pattern but require space after
            text = re.sub(r'^Tag type:\s*\w+\s+', '', text, flags=re.IGNORECASE)
        
        # ============================================================
        # Remove wiki footer sections (everything after these markers)
        # ============================================================
        # "Other Wiki Information Last updated: ... by user"
        text = re.sub(r'\s*Other Wiki Information\s*Last updated:.*$', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Standalone "Last updated" if Other Wiki Information was already removed
        text = re.sub(r'\s*Last updated:\s*[^.]+\.\s*by\s+\w+.*$', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # ============================================================
        # Remove boilerplate edit/lock notices
        # ============================================================
        # "This entry is not locked and you can edit it as you see fit."
        text = re.sub(r'\s*This entry is not locked and you can edit it as you see fit\.?\s*', '', text, flags=re.IGNORECASE)
        
        # "This entry is locked..." (various forms)
        text = re.sub(r'\s*This entry is locked[^.]*\.?\s*', '', text, flags=re.IGNORECASE)
        
        # ============================================================
        # Remove related content markers
        # ============================================================
        # "View more »" or "View more >" (related posts section)
        text = re.sub(r'\s*View more\s*[»>]?\s*$', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*View more\s*[»>]?\s*', ' ', text, flags=re.IGNORECASE)
        
        # "There are no images associated with this wiki entry."
        text = re.sub(r'\s*There are no images associated with this wiki entry\.?\s*', '', text, flags=re.IGNORECASE)
        
        # ============================================================
        # Remove site chrome and cookies
        # ============================================================
        # "Reset cookie / GDPR consent" and variations
        text = re.sub(r'\s*Reset cookie\s*/?\s*GDPR consent\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*GDPR consent\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*Reset cookie\s*', '', text, flags=re.IGNORECASE)
        
        # ============================================================
        # Remove h4 sections that are not content (navigational/reference sections)
        # These sections continue until the next h4 or end of text
        # ============================================================
        remove_sections = [
            r'h4\.\s*See also',
            r'h4\.\s*External links?',
            r'h4\.\s*Links?',
            r'h4\.\s*References?',
            r'h4\.\s*Typical Tags?',
            r'h4\.\s*Related Tags?',
        ]
        
        for section_pattern in remove_sections:
            # Remove from section header to next h4 or end
            text = re.sub(
                section_pattern + r'.*?(?=h4\.|$)',
                '',
                text,
                flags=re.IGNORECASE | re.DOTALL
            )
        
        # Convert remaining h4 headers to readable format (these are content headers)
        text = re.sub(r'\bh4\.\s*(Original characters?)\s*:?\s*', 'Original characters: ', text, flags=re.IGNORECASE)
        text = re.sub(r'\bh4\.\s*(Types?)\s*:?\s*', 'Types: ', text, flags=re.IGNORECASE)
        text = re.sub(r'\bh4\.\s*(\w+)\s*:?\s*', r'\1: ', text)  # Generic h4 -> "Header: "
        
        # ============================================================
        # Clean up DText link formatting
        # ============================================================
        # DText links: "display text":URL or "display text":[URL] or "text":/path?query
        # Keep only the display text
        text = re.sub(r'"([^"]+)":\[?(?:https?://)?[^\s\[\]"]+\]?', r'\1', text)
        text = re.sub(r'"([^"]+)":/[^\s"]+', r'\1', text)
        
        # Clean up wiki links: [[link]] or [[link|display]]
        text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', text)  # [[link|display]] -> display
        text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)  # [[link]] -> link
        
        # Clean up bare URLs and URL-like fragments
        text = re.sub(r'https?://[^\s<>"]+', '', text)
        text = re.sub(r'\b\w+\.(?:com|org|net|info|jp|co\.uk)[^\s]*', '', text)  # Domain names
        
        # Clean up query string fragments that might remain
        text = re.sub(r'\[[^\]]*\]', '', text)  # Remove anything in square brackets (likely URL params)
        text = re.sub(r'=[^\s&]*', '', text)  # Remove =value patterns
        text = re.sub(r'&\w+', '', text)  # Remove &param patterns
        text = re.sub(r'\?utf8', '', text)  # Specific cleanup
        
        # ============================================================
        # Remove tag dump sections (lists of related tags after description)
        # These appear as long strings of underscore_separated_words
        # ============================================================
        # Detect and remove sections that look like tag dumps
        text = self._remove_tag_dumps(text)
        
        # ============================================================
        # Clean up bullet points and list formatting
        # ============================================================
        # Convert asterisk bullets to proper bullets
        text = re.sub(r'\*\s+', '• ', text)  # "* item" -> "• item"
        text = re.sub(r'^\s*\*\s*', '• ', text, flags=re.MULTILINE)
        text = re.sub(r'\|\s*\*\s*', ' • ', text)
        text = re.sub(r'\s*\|\s*', ' ', text)
        
        # Clean up remaining asterisks used as separators
        text = re.sub(r'\*\s*', ', ', text)
        # Clean up multiple commas
        text = re.sub(r',\s*,', ',', text)
        text = re.sub(r':\s*,\s*', ': ', text)  # "Types: , item" -> "Types: item"
        
        # Clean up search wildcard patterns that are clearly not prose
        text = re.sub(r'\b\w+_,\s*', '', text)  # "fuwayu_," leftover
        text = re.sub(r',\s*_\w+\b', '', text)  # ",_fuwayu" leftover
        
        # ============================================================
        # Final cleanup
        # ============================================================
        # Add space before sentences that are mushed together
        text = re.sub(r'\.([A-Z])', r'. \1', text)  # ".The" -> ". The"
        
        # Add space between bullet items that are mushed together
        text = re.sub(r'•\s*(\w)', r'• \1', text)  # "•item" -> "• item"
        text = re.sub(r'(\w)•', r'\1 •', text)  # "item•" -> "item •"
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        # Remove empty parentheses, brackets that might remain
        text = re.sub(r'\(\s*\)', '', text)
        text = re.sub(r'\[\s*\]', '', text)
        text = re.sub(r'\]', '', text)  # Remove any remaining brackets
        
        # Clean up multiple punctuation
        text = re.sub(r'\.{2,}', '.', text)
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        
        # Clean up leading/trailing punctuation
        text = re.sub(r'^[,;:\s•*]+', '', text)
        text = re.sub(r'[,;:\s•*]+$', '', text)
        
        # Remove empty bullet point artifacts
        text = re.sub(r'•\s*•', '•', text)
        text = re.sub(r':\s*•\s*$', '', text)  # Remove trailing ": •"
        
        # Final trim
        return text.strip()
    
    def _remove_tag_dumps(self, text: str) -> str:
        """Remove sections that appear to be dumps of related tags.
        
        Tag dumps are characterized by:
        - Many words separated by underscores or spaces
        - Typically lowercase
        - Often contain common tag patterns (1girls, big_breasts, etc.)
        - Appear at the end of descriptions
        """
        # Common tag dump indicators - if we see these, truncate
        tag_dump_patterns = [
            # Common tag prefixes/patterns that indicate a tag dump started
            r'\b\d+girls?\b',
            r'\b\d+boys?\b', 
            r'\bbig_breasts\b',
            r'\blarge_breasts\b',
            r'\bhuge_breasts\b',
            r'\bsmall_breasts\b',
            r'\bmedium_breasts\b',
            r'\bblonde_hair\b',
            r'\bblack_hair\b',
            r'\bbrown_hair\b',
            r'\bblue_eyes\b',
            r'\bgreen_eyes\b',
            r'\bbrown_eyes\b',
            r'\bfemale_only\b',
            r'\bmale_only\b',
            r'\bsolo_female\b',
            r'\bsolo_male\b',
            r'\bnude_female\b',
            r'\bnude_male\b',
            r'\bcompletely_nude\b',
            r'\bhigh_resolution\b',
            r'\bhighres\b',
            r'\bhi_res\b',
            r'\bdigital_media\b',
            r'\bdigital_art\b',
            r'\boriginal_character\b',
            r'\bfemale_focus\b',
            r'\bmale_focus\b',
        ]
        
        # Build combined pattern
        combined_pattern = '|'.join(tag_dump_patterns)
        
        # Find first occurrence of what looks like a tag dump
        # Look for a sequence that starts with common tag patterns
        match = re.search(combined_pattern, text, flags=re.IGNORECASE)
        
        if match:
            # Check if this is actually in a tag dump context
            # A tag dump typically has multiple tag-like words in sequence
            pos = match.start()
            before = text[:pos]
            after = text[pos:]
            
            # Count tag-like patterns in the "after" section
            tag_count = len(re.findall(r'\b\w+_\w+\b', after[:200]))
            
            # If there are many underscore-separated words, it's likely a tag dump
            if tag_count >= 5:
                # Truncate at this point, but try to end at a sentence
                # Look backwards for a good stopping point
                sentence_end = max(
                    before.rfind('. '),
                    before.rfind('.\n'),
                    before.rfind('.)'),
                )
                if sentence_end > len(before) * 0.3:  # Only if we keep at least 30%
                    return before[:sentence_end + 1].strip()
                return before.strip()
        
        return text
    
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
        """Extract tag description from parsed wiki page.
        
        Rule34 wiki pages have a specific structure:
        - Header: "Now Viewing: tagname" and "Tag type: Type"
        - Body: The actual description (may include h4 sections, lists, links)
        - Footer: "Other Wiki Information Last updated: ..."
        - Below: "This entry is [not] locked..." message
        - Then: Related images with their tags
        
        We need to extract just the body content.
        """
        
        # Remove known non-content elements first
        for elem in soup.select("script, style, #header, #navbar, #subnavbar, #paginator, .sidebar, .notice"):
            elem.decompose()
        
        # First, try to find the content div which contains the wiki body
        content_div = soup.find("div", {"id": "content"})
        if not content_div:
            return None
        
        # The wiki content is typically in a table structure
        # Try to find the specific cell containing the description
        
        # Method 1: Look for the wiki body table cell
        # The structure often has the header info, then description, then footer
        best_candidate = None
        best_score = 0
        
        for td in content_div.find_all("td"):
            text = td.get_text(separator='\n').strip()
            
            # Skip very short cells
            if len(text) < 30:
                continue
            
            # Skip cells that are mostly metadata
            if text.startswith("Version"):
                continue
            if "Recent Changes" in text and len(text.split('\n')) < 5:
                continue
            
            # Score this candidate based on content quality
            score = self._score_description_candidate(text)
            
            if score > best_score:
                best_score = score
                best_candidate = text
        
        if best_candidate and best_score >= 10:
            cleaned = self._clean_description(best_candidate)
            if self._is_valid_description(cleaned):
                return cleaned
        
        # Method 2: Look for paragraphs with real content
        for p in content_div.find_all("p"):
            text = p.get_text().strip()
            if len(text) > 50:
                cleaned = self._clean_description(text)
                if self._is_valid_description(cleaned):
                    return cleaned
        
        # Method 3: Fall back to any substantial text in divs within content
        for div in content_div.find_all("div"):
            div_id = div.get("id", "")
            div_class = " ".join(div.get("class", []))
            
            # Skip navigation/chrome
            skip_patterns = ["header", "navbar", "subnavbar", "sidebar", "paginator", "notice", "footer", "pagination"]
            
            if any(x in div_id.lower() for x in skip_patterns):
                continue
            if any(x in div_class.lower() for x in skip_patterns):
                continue
            
            text = div.get_text(separator='\n').strip()
            if 50 < len(text) < 5000:
                cleaned = self._clean_description(text)
                if self._is_valid_description(cleaned):
                    return cleaned
        
        return None
    
    def _score_description_candidate(self, text: str) -> int:
        """Score a text block on how likely it is to be a real description.
        
        Higher score = more likely to be valid description content.
        """
        score = 0
        
        # Positive signals
        if "Tag type:" in text:  # Has the header, which means it's the main content
            score += 20
        if any(x in text for x in ["is a ", "are ", "refers to", "describes", "character", "series"]):
            score += 15
        if len(text.split('.')) >= 2:  # Has multiple sentences
            score += 10
        if re.search(r'\bh4\.\s*(See also|External links)', text, re.IGNORECASE):
            score += 5  # Has wiki formatting which indicates real content
        
        # Length-based scoring (prefer medium-length content)
        text_len = len(text)
        if 100 < text_len < 500:
            score += 15
        elif 500 < text_len < 2000:
            score += 10
        elif text_len > 2000:
            score += 5  # Might include tag dumps
        
        # Negative signals
        if "Reset cookie" in text:
            score -= 30
        if "GDPR consent" in text:
            score -= 30
        if "Recent Changes" in text:
            score -= 20
        
        # Heavily penalize if it looks like mostly tags
        tag_like_words = len(re.findall(r'\b\w+_\w+_\w+\b', text))  # Words with multiple underscores
        if tag_like_words > 20:
            score -= 30
        
        return score
    
    def _is_valid_description(self, cleaned_text: str) -> bool:
        """Check if cleaned text is a valid description worth saving.
        
        Returns False for garbage, boilerplate, or empty content.
        """
        if not cleaned_text:
            return False
        
        # Too short to be useful
        if len(cleaned_text) < 20:
            return False
        
        # Too few words
        word_count = len(cleaned_text.split())
        if word_count < 5:
            return False
        
        # Mostly numbers/symbols
        alpha_chars = sum(1 for c in cleaned_text if c.isalpha())
        if alpha_chars < len(cleaned_text) * 0.5:
            return False
        
        # Known garbage patterns
        garbage_patterns = [
            r'^This entry is (not )?locked',
            r'^Reset cookie',
            r'^GDPR consent',
            r'^Recent Changes',
            r'^Version \d+',
            r'^Last updated',
            r'^There are no images',
            r'^View more',
            # Pure tag dumps (multiple underscore-joined words with no prose)
            r'^(\w+_)+\w+(\s+(\w+_)+\w+){5,}$',
        ]
        
        for pattern in garbage_patterns:
            if re.match(pattern, cleaned_text, re.IGNORECASE):
                return False
        
        return True

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
