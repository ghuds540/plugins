#!/usr/bin/env python3
"""
Stash Bulk Scraper

Scrapes all images and/or scenes in Stash using installed fragment scrapers.
Handles tag creation, rate limiting, and errors gracefully.
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not installed. Progress bar disabled. Install with: pip install tqdm")

# ============================================================================
# Configuration
# ============================================================================

DEFAULT_STASH_URL = "http://localhost:9999"
DEFAULT_RATE_LIMIT = 2.0  # seconds between scrape requests
DEFAULT_RATE_LIMIT_LOCAL = 0.5  # faster rate limit for localhost
DEFAULT_TIMEOUT = 60
DEFAULT_TIMEOUT_LOCAL = 30  # shorter timeout for localhost
MAX_RETRIES = 3
MAX_RETRIES_LOCAL = 1  # fewer retries for localhost
BACKOFF_FACTOR = 2
BATCH_SIZE = 50  # Number of items to fetch per page

# ============================================================================
# Utility Functions
# ============================================================================

def is_localhost(url: str) -> bool:
    """Check if a URL points to localhost/local machine."""
    from urllib.parse import urlparse
    hostname = urlparse(url).hostname
    if not hostname:
        return False

    localhost_names = ['localhost', '127.0.0.1', '::1', '0.0.0.0']
    return hostname.lower() in localhost_names

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(verbose: bool, debug: bool, log_file: Optional[str] = None) -> logging.Logger:
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

    logger = logging.getLogger("bulk_scraper")
    logger.setLevel(level)
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# ============================================================================
# Date Utilities
# ============================================================================

def parse_date(date_string: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format '{date_string}'. Expected YYYY-MM-DD (e.g., 2024-01-15)")

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class StashItem:
    """Represents an image or scene in Stash."""
    id: str
    type: str  # "image" or "scene"
    title: Optional[str] = None
    path: Optional[str] = None
    checksum: Optional[str] = None
    tags: List[Dict[str, Any]] = field(default_factory=list)
    organized: bool = False
    file_timestamp: Optional[datetime] = None

@dataclass
class Scraper:
    """Represents a Stash scraper."""
    id: str
    name: str
    supported_scrapes: List[str]

@dataclass
class ScrapeResult:
    """Result from a scrape operation."""
    item: StashItem
    scraper_name: str
    success: bool = False
    scraped_data: Optional[Dict[str, Any]] = None
    tags_created: List[str] = field(default_factory=list)
    tags_added: List[str] = field(default_factory=list)
    performers_created: List[str] = field(default_factory=list)
    performers_added: List[str] = field(default_factory=list)
    studio_created: Optional[str] = None
    studio_added: Optional[str] = None
    metadata_updated: Dict[str, bool] = field(default_factory=dict)  # field_name: was_updated
    scrape_time_seconds: float = 0.0
    fallback_used: bool = False
    error: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class ProgressStats:
    """Statistics for progress tracking."""
    total: int = 0
    processed: int = 0
    successful: int = 0
    skipped: int = 0
    failed: int = 0
    tags_created: int = 0
    tags_added: int = 0
    performers_created: int = 0
    performers_added: int = 0
    studios_created: int = 0
    studios_added: int = 0
    fallback_used_count: int = 0
    metadata_fields_updated: Dict[str, int] = field(default_factory=dict)  # field_name: count
    scraper_success_count: Dict[str, int] = field(default_factory=dict)  # scraper_name: success_count
    scraper_failure_count: Dict[str, int] = field(default_factory=dict)  # scraper_name: failure_count
    start_time: float = field(default_factory=time.time)
    total_scrape_time: float = 0.0

    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    def items_per_second(self) -> float:
        elapsed = self.elapsed_seconds()
        return self.processed / elapsed if elapsed > 0 else 0

    def eta_seconds(self) -> Optional[float]:
        remaining = self.total - self.processed
        rate = self.items_per_second()
        return remaining / rate if rate > 0 else None

    def format_eta(self) -> str:
        eta = self.eta_seconds()
        if eta is None:
            return "unknown"
        return str(timedelta(seconds=int(eta)))

# ============================================================================
# HTTP Session with Retry Logic
# ============================================================================

def create_session(max_retries: int = MAX_RETRIES, backoff_factor: float = BACKOFF_FACTOR, is_local: bool = False) -> requests.Session:
    """Create a requests session with retry logic for transient errors."""
    session = requests.Session()

    # For localhost, use simpler retry logic
    if is_local:
        status_forcelist = [
            500,  # Internal Server Error
            503,  # Service Unavailable
        ]
    else:
        status_forcelist = [
            429,  # Too Many Requests
            500,  # Internal Server Error
            502,  # Bad Gateway
            503,  # Service Unavailable
            504,  # Gateway Timeout
        ]

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor if not is_local else 0.5,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Stash-Bulk-Scraper/1.0",
        "Content-Type": "application/json"
    })

    return session

# ============================================================================
# Stash API Client
# ============================================================================

class StashClient:
    """Client for interacting with Stash GraphQL API."""

    def __init__(self, base_url: str, api_key: Optional[str], session: requests.Session,
                 logger: logging.Logger, timeout: int = DEFAULT_TIMEOUT, timestamp_type: str = "mtime",
                 need_timestamps: bool = False):
        self.base_url = base_url.rstrip("/")
        self.graphql_url = f"{self.base_url}/graphql"
        self.session = session
        self.logger = logger
        self.timeout = timeout
        self.timestamp_type = timestamp_type
        self.need_timestamps = need_timestamps

        if api_key:
            self.session.headers["ApiKey"] = api_key

    def _get_file_timestamp(self, file_path: str) -> Optional[datetime]:
        """Get file timestamp based on configured timestamp type."""
        try:
            path = Path(file_path)
            if not path.exists():
                self.logger.debug(f"File does not exist: {file_path}")
                return None

            stat = path.stat()
            if self.timestamp_type == "ctime":
                timestamp = stat.st_ctime
            else:  # mtime (default)
                timestamp = stat.st_mtime

            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            self.logger.debug(f"Failed to get timestamp for {file_path}: {e}")
            return None

    def _execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute a GraphQL query against Stash."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        self.logger.debug(f"Executing GraphQL query: {query[:100]}...")
        if variables:
            self.logger.debug(f"Variables: {json.dumps(variables)[:200]}...")

        try:
            response = self.session.post(
                self.graphql_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                error_msg = f"GraphQL errors: {data['errors']}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

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

    def list_scrapers(self, scraper_type: str) -> List[Scraper]:
        """List available scrapers for a given type (SCENE or IMAGE)."""
        query = """
        query ListScrapers($types: [ScrapeContentType!]!) {
            listScrapers(types: $types) {
                id
                name
                scene {
                    supported_scrapes
                }
                image {
                    supported_scrapes
                }
            }
        }
        """

        variables = {"types": [scraper_type]}
        data = self._execute_query(query, variables)

        scrapers = []
        for scraper_data in data.get("listScrapers", []):
            type_key = "scene" if scraper_type == "SCENE" else "image"
            type_data = scraper_data.get(type_key, {})

            if type_data and type_data.get("supported_scrapes"):
                scrapers.append(Scraper(
                    id=scraper_data["id"],
                    name=scraper_data["name"],
                    supported_scrapes=type_data["supported_scrapes"]
                ))

        return scrapers

    def find_images(self, page: int = 1, per_page: int = BATCH_SIZE,
                   organized_filter: Optional[bool] = None) -> tuple[List[StashItem], int]:
        """Fetch images from Stash with pagination."""
        query = """
        query FindImages($filter: FindFilterType, $image_filter: ImageFilterType) {
            findImages(filter: $filter, image_filter: $image_filter) {
                count
                images {
                    id
                    title
                    files {
                        path
                        fingerprints {
                            type
                            value
                        }
                    }
                    tags {
                        id
                        name
                    }
                    organized
                }
            }
        }
        """

        filter_obj = {
            "page": page,
            "per_page": per_page,
            "sort": "id",
            "direction": "ASC"
        }

        image_filter = {}
        if organized_filter is not None:
            image_filter["organized"] = organized_filter

        variables = {
            "filter": filter_obj,
            "image_filter": image_filter if image_filter else None
        }

        data = self._execute_query(query, variables)
        find_images_data = data.get("findImages", {})

        images = []
        for img in find_images_data.get("images", []):
            # Get primary file path and MD5
            files = img.get("files", [])
            path = files[0].get("path") if files else None
            checksum = None

            if files:
                for fp in files[0].get("fingerprints", []):
                    if fp.get("type") == "md5":
                        checksum = fp.get("value")
                        break

            file_timestamp = None
            if self.need_timestamps and path:
                file_timestamp = self._get_file_timestamp(path)

            images.append(StashItem(
                id=img["id"],
                type="image",
                title=img.get("title"),
                path=path,
                checksum=checksum,
                tags=img.get("tags", []),
                organized=img.get("organized", False),
                file_timestamp=file_timestamp
            ))

        total_count = find_images_data.get("count", 0)
        return images, total_count

    def find_scenes(self, page: int = 1, per_page: int = BATCH_SIZE,
                   organized_filter: Optional[bool] = None) -> tuple[List[StashItem], int]:
        """Fetch scenes from Stash with pagination."""
        query = """
        query FindScenes($filter: FindFilterType, $scene_filter: SceneFilterType) {
            findScenes(filter: $filter, scene_filter: $scene_filter) {
                count
                scenes {
                    id
                    title
                    files {
                        path
                        fingerprints {
                            type
                            value
                        }
                    }
                    tags {
                        id
                        name
                    }
                    organized
                }
            }
        }
        """

        filter_obj = {
            "page": page,
            "per_page": per_page,
            "sort": "id",
            "direction": "ASC"
        }

        scene_filter = {}
        if organized_filter is not None:
            scene_filter["organized"] = organized_filter

        variables = {
            "filter": filter_obj,
            "scene_filter": scene_filter if scene_filter else None
        }

        data = self._execute_query(query, variables)
        find_scenes_data = data.get("findScenes", {})

        scenes = []
        for scene in find_scenes_data.get("scenes", []):
            # Get primary file path and MD5
            files = scene.get("files", [])
            path = files[0].get("path") if files else None
            checksum = None

            if files:
                for fp in files[0].get("fingerprints", []):
                    if fp.get("type") == "md5":
                        checksum = fp.get("value")
                        break

            file_timestamp = None
            if self.need_timestamps and path:
                file_timestamp = self._get_file_timestamp(path)

            scenes.append(StashItem(
                id=scene["id"],
                type="scene",
                title=scene.get("title"),
                path=path,
                checksum=checksum,
                tags=scene.get("tags", []),
                organized=scene.get("organized", False),
                file_timestamp=file_timestamp
            ))

        total_count = find_scenes_data.get("count", 0)
        return scenes, total_count

    def scrape_item(self, item: StashItem, scraper_id: str) -> Optional[Dict]:
        """Scrape a single item using a specific scraper."""
        if item.type == "scene":
            query = """
            query ScrapeSingleScene($source: ScraperSourceInput!, $input: ScrapeSingleSceneInput!) {
                scrapeSingleScene(source: $source, input: $input) {
                    title
                    code
                    details
                    director
                    urls
                    date
                    tags {
                        stored_id
                        name
                    }
                    performers {
                        stored_id
                        name
                    }
                    studio {
                        stored_id
                        name
                    }
                }
            }
            """

            variables = {
                "source": {"scraper_id": scraper_id},
                "input": {"scene_id": item.id}
            }

            data = self._execute_query(query, variables)
            result = data.get("scrapeSingleScene")
            # API returns array, take first result
            return result[0] if result and len(result) > 0 else None

        else:  # image
            query = """
            query ScrapeSingleImage($source: ScraperSourceInput!, $input: ScrapeSingleImageInput!) {
                scrapeSingleImage(source: $source, input: $input) {
                    title
                    code
                    details
                    photographer
                    urls
                    date
                    tags {
                        stored_id
                        name
                    }
                    performers {
                        stored_id
                        name
                    }
                    studio {
                        stored_id
                        name
                    }
                }
            }
            """

            variables = {
                "source": {"scraper_id": scraper_id},
                "input": {"image_id": item.id}
            }

            data = self._execute_query(query, variables)
            result = data.get("scrapeSingleImage")
            # API returns array, take first result
            return result[0] if result and len(result) > 0 else None

    def find_tag(self, name: str) -> Optional[str]:
        """Find a tag by name, returns tag ID if found."""
        query = """
        query FindTags($filter: FindFilterType) {
            findTags(filter: $filter) {
                tags {
                    id
                    name
                }
            }
        }
        """

        variables = {
            "filter": {
                "q": name,
                "per_page": 1
            }
        }

        data = self._execute_query(query, variables)
        tags = data.get("findTags", {}).get("tags", [])

        for tag in tags:
            if tag["name"].lower() == name.lower():
                return tag["id"]

        return None

    def create_tag(self, name: str) -> Optional[str]:
        """Create a new tag, returns tag ID."""
        mutation = """
        mutation TagCreate($input: TagCreateInput!) {
            tagCreate(input: $input) {
                id
                name
            }
        }
        """

        variables = {
            "input": {"name": name}
        }

        try:
            data = self._execute_query(mutation, variables)
            tag_data = data.get("tagCreate")
            if tag_data:
                return tag_data["id"]
        except Exception as e:
            self.logger.debug(f"Failed to create tag '{name}': {e}")

        return None

    def get_or_create_tag(self, name: str) -> Optional[str]:
        """Get tag ID, creating it if it doesn't exist."""
        # Try to find existing tag
        tag_id = self.find_tag(name)
        if tag_id:
            return tag_id

        # Create new tag
        self.logger.debug(f"Creating new tag: {name}")
        return self.create_tag(name)

    def find_performer(self, name: str) -> Optional[str]:
        """Find a performer by name, returns performer ID if found."""
        query = """
        query FindPerformers($filter: FindFilterType) {
            findPerformers(filter: $filter) {
                performers {
                    id
                    name
                }
            }
        }
        """

        variables = {
            "filter": {
                "q": name,
                "per_page": 1
            }
        }

        try:
            data = self._execute_query(query, variables)
            performers = data.get("findPerformers", {}).get("performers", [])

            for performer in performers:
                if performer["name"].lower() == name.lower():
                    return performer["id"]
        except Exception as e:
            self.logger.debug(f"Error finding performer '{name}': {e}")

        return None

    def create_performer(self, name: str) -> Optional[str]:
        """Create a new performer, returns performer ID."""
        mutation = """
        mutation PerformerCreate($input: PerformerCreateInput!) {
            performerCreate(input: $input) {
                id
                name
            }
        }
        """

        variables = {
            "input": {"name": name}
        }

        try:
            data = self._execute_query(mutation, variables)
            performer_data = data.get("performerCreate")
            if performer_data:
                return performer_data["id"]
        except Exception as e:
            self.logger.debug(f"Failed to create performer '{name}': {e}")

        return None

    def get_or_create_performer(self, name: str) -> Optional[str]:
        """Get performer ID, creating it if it doesn't exist."""
        # Try to find existing performer
        performer_id = self.find_performer(name)
        if performer_id:
            return performer_id

        # Create new performer
        self.logger.debug(f"Creating new performer: {name}")
        return self.create_performer(name)

    def find_studio(self, name: str) -> Optional[str]:
        """Find a studio by name, returns studio ID if found."""
        query = """
        query FindStudios($filter: FindFilterType) {
            findStudios(filter: $filter) {
                studios {
                    id
                    name
                }
            }
        }
        """

        variables = {
            "filter": {
                "q": name,
                "per_page": 1
            }
        }

        try:
            data = self._execute_query(query, variables)
            studios = data.get("findStudios", {}).get("studios", [])

            for studio in studios:
                if studio["name"].lower() == name.lower():
                    return studio["id"]
        except Exception as e:
            self.logger.debug(f"Error finding studio '{name}': {e}")

        return None

    def create_studio(self, name: str) -> Optional[str]:
        """Create a new studio, returns studio ID."""
        mutation = """
        mutation StudioCreate($input: StudioCreateInput!) {
            studioCreate(input: $input) {
                id
                name
            }
        }
        """

        variables = {
            "input": {"name": name}
        }

        try:
            data = self._execute_query(mutation, variables)
            studio_data = data.get("studioCreate")
            if studio_data:
                return studio_data["id"]
        except Exception as e:
            self.logger.debug(f"Failed to create studio '{name}': {e}")

        return None

    def get_or_create_studio(self, name: str) -> Optional[str]:
        """Get studio ID, creating it if it doesn't exist."""
        # Try to find existing studio
        studio_id = self.find_studio(name)
        if studio_id:
            return studio_id

        # Create new studio
        self.logger.debug(f"Creating new studio: {name}")
        return self.create_studio(name)

    def update_item_metadata(self, item: StashItem, updates: Dict[str, Any]) -> tuple[bool, Dict[str, bool]]:
        """Update metadata for an item. Returns (success, fields_updated_map)."""
        fields_updated = {}

        # Merge tags with existing (deduplicate)
        if "tag_ids" in updates:
            current_tag_ids = [tag["id"] for tag in item.tags]
            updates["tag_ids"] = list(set(current_tag_ids + updates["tag_ids"]))
            fields_updated["tags"] = True

        # Track which fields we're updating
        for field in ["title", "details", "date", "photographer", "director", "code", "urls", "studio_id", "performer_ids"]:
            if field in updates and updates[field]:
                fields_updated[field] = True

        if item.type == "scene":
            mutation = """
            mutation SceneUpdate($input: SceneUpdateInput!) {
                sceneUpdate(input: $input) {
                    id
                }
            }
            """

            # Build scene update input
            input_data = {"id": item.id}
            input_data.update(updates)

            variables = {"input": input_data}
        else:  # image
            mutation = """
            mutation ImageUpdate($input: ImageUpdateInput!) {
                imageUpdate(input: $input) {
                    id
                }
            }
            """

            # Build image update input
            input_data = {"id": item.id}
            input_data.update(updates)

            variables = {"input": input_data}

        try:
            self._execute_query(mutation, variables)
            return True, fields_updated
        except Exception as e:
            self.logger.error(f"Failed to update {item.type} {item.id}: {e}")
            return False, {}

# ============================================================================
# Bulk Scraper
# ============================================================================

class BulkScraper:
    """Orchestrates bulk scraping of Stash items."""

    def __init__(self, stash_client: StashClient, logger: logging.Logger,
                 rate_limit: float = DEFAULT_RATE_LIMIT, dry_run: bool = False,
                 skip_organized: bool = False, skip_tagged: bool = False,
                 try_all_scrapers: bool = False, skip_if_has_tags: Optional[List[str]] = None,
                 date_since: Optional[datetime] = None, date_before: Optional[datetime] = None):
        self.stash = stash_client
        self.logger = logger
        self.rate_limit = rate_limit
        self.dry_run = dry_run
        self.skip_organized = skip_organized
        self.skip_tagged = skip_tagged
        self.try_all_scrapers = try_all_scrapers
        self.skip_if_has_tags = [tag.lower() for tag in (skip_if_has_tags or [])]
        self.date_since = date_since
        self.date_before = date_before
        self.last_request_time = 0.0
        self.stats = ProgressStats()

    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limit."""
        if self.rate_limit <= 0:
            return

        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _should_skip_item(self, item: StashItem) -> tuple[bool, Optional[str]]:
        """Determine if an item should be skipped."""
        if self.skip_organized and item.organized:
            return True, "already organized"

        if self.skip_tagged and len(item.tags) > 0:
            return True, "already has tags"

        # Check if item has any of the exclusion tags
        if self.skip_if_has_tags and item.tags:
            item_tag_names = [tag["name"].lower() for tag in item.tags]
            for skip_tag in self.skip_if_has_tags:
                if skip_tag in item_tag_names:
                    return True, f"has exclusion tag: {skip_tag}"

        if not item.path:
            return True, "no file path"

        # Check date filters
        if self.date_since or self.date_before:
            if not item.file_timestamp:
                return True, "no file timestamp available"

            # Apply --since filter (inclusive)
            if self.date_since and item.file_timestamp < self.date_since:
                return True, f"before date filter ({self.date_since.strftime('%Y-%m-%d')})"

            # Apply --before filter (inclusive, need to check end of day)
            if self.date_before:
                # Add one day and compare (to make the date inclusive)
                end_of_day = self.date_before + timedelta(days=1)
                if item.file_timestamp >= end_of_day:
                    return True, f"after date filter ({self.date_before.strftime('%Y-%m-%d')})"

        return False, None

    def scrape_item(self, item: StashItem, scrapers: List[Scraper]) -> ScrapeResult:
        """Scrape a single item, trying multiple scrapers if enabled."""
        scrape_start_time = time.time()

        # Start with first scraper
        primary_scraper = scrapers[0]
        result = ScrapeResult(item=item, scraper_name=primary_scraper.name)

        # Check if should skip
        should_skip, skip_reason = self._should_skip_item(item)
        if should_skip:
            result.skipped = True
            result.skip_reason = skip_reason
            result.scrape_time_seconds = time.time() - scrape_start_time
            return result

        # Try each scraper until one succeeds
        scrapers_to_try = scrapers if self.try_all_scrapers else [primary_scraper]

        for scraper_index, scraper in enumerate(scrapers_to_try):
            # Rate limit
            self._wait_for_rate_limit()

            try:
                # Scrape the item
                self.logger.debug(f"Scraping {item.type} {item.id} with {scraper.name}")
                scraped_data = self.stash.scrape_item(item, scraper.id)

                if not scraped_data:
                    self.logger.debug(f"{scraper.name} returned no data")
                    if scraper == scrapers_to_try[-1]:  # Last scraper
                        result.error = "No data returned from any scraper"
                    continue

                # Check if we got any useful data
                has_useful_data = any([
                    scraped_data.get("tags"),
                    scraped_data.get("performers"),
                    scraped_data.get("studio"),
                    scraped_data.get("title"),
                    scraped_data.get("details"),
                    scraped_data.get("date"),
                    scraped_data.get("urls"),
                ])

                if not has_useful_data:
                    self.logger.debug(f"{scraper.name} returned no useful data")
                    if scraper == scrapers_to_try[-1]:  # Last scraper
                        result.skipped = True
                        result.skip_reason = "no useful data from any scraper"
                    continue

                # Success! Update result with successful scraper name
                result.scraper_name = scraper.name
                result.scraped_data = scraped_data
                result.fallback_used = (scraper != primary_scraper)

                if result.fallback_used:
                    self.logger.info(f"Fallback scraper {scraper.name} succeeded for {item.type} {item.id}")

                # Build metadata updates dictionary
                updates = {}

                # Process tags
                scraped_tags = scraped_data.get("tags", [])
                if scraped_tags:
                    tag_ids = []
                    for tag in scraped_tags:
                        tag_name = tag["name"]

                        if tag.get("stored_id"):
                            tag_ids.append(tag["stored_id"])
                            result.tags_added.append(tag_name)
                        else:
                            if not self.dry_run:
                                tag_id = self.stash.get_or_create_tag(tag_name)
                                if tag_id:
                                    tag_ids.append(tag_id)
                                    result.tags_created.append(tag_name)
                                    result.tags_added.append(tag_name)
                                    self.logger.debug(f"Created tag: {tag_name}")
                                else:
                                    self.logger.warning(f"Failed to create tag: {tag_name}")
                            else:
                                result.tags_created.append(tag_name)
                                result.tags_added.append(tag_name)

                    if tag_ids or self.dry_run:
                        updates["tag_ids"] = tag_ids

                # Process performers
                scraped_performers = scraped_data.get("performers", [])
                if scraped_performers:
                    performer_ids = []
                    for performer in scraped_performers:
                        performer_name = performer["name"]

                        if performer.get("stored_id"):
                            performer_ids.append(performer["stored_id"])
                            result.performers_added.append(performer_name)
                        else:
                            if not self.dry_run:
                                performer_id = self.stash.get_or_create_performer(performer_name)
                                if performer_id:
                                    performer_ids.append(performer_id)
                                    result.performers_created.append(performer_name)
                                    result.performers_added.append(performer_name)
                                    self.logger.debug(f"Created performer: {performer_name}")
                                else:
                                    self.logger.warning(f"Failed to create performer: {performer_name}")
                            else:
                                result.performers_created.append(performer_name)
                                result.performers_added.append(performer_name)

                    if performer_ids or self.dry_run:
                        updates["performer_ids"] = performer_ids

                # Process studio
                scraped_studio = scraped_data.get("studio")
                if scraped_studio:
                    studio_name = scraped_studio["name"]

                    if scraped_studio.get("stored_id"):
                        updates["studio_id"] = scraped_studio["stored_id"]
                        result.studio_added = studio_name
                    else:
                        if not self.dry_run:
                            studio_id = self.stash.get_or_create_studio(studio_name)
                            if studio_id:
                                updates["studio_id"] = studio_id
                                result.studio_created = studio_name
                                result.studio_added = studio_name
                                self.logger.debug(f"Created studio: {studio_name}")
                            else:
                                self.logger.warning(f"Failed to create studio: {studio_name}")
                        else:
                            result.studio_created = studio_name
                            result.studio_added = studio_name

                # Process other metadata fields
                if scraped_data.get("title"):
                    updates["title"] = scraped_data["title"]

                if scraped_data.get("details"):
                    updates["details"] = scraped_data["details"]

                if scraped_data.get("date"):
                    updates["date"] = scraped_data["date"]

                if scraped_data.get("urls"):
                    updates["urls"] = scraped_data["urls"]

                if scraped_data.get("code"):
                    updates["code"] = scraped_data["code"]

                # Scene-specific fields
                if item.type == "scene" and scraped_data.get("director"):
                    updates["director"] = scraped_data["director"]

                # Image-specific fields
                if item.type == "image" and scraped_data.get("photographer"):
                    updates["photographer"] = scraped_data["photographer"]

                if self.dry_run:
                    # Dry run - just report what would happen
                    result.success = True
                    result.metadata_updated = {k: True for k in updates.keys()}
                    update_summary = []
                    if result.tags_added:
                        update_summary.append(f"{len(result.tags_added)} tags")
                    if result.performers_added:
                        update_summary.append(f"{len(result.performers_added)} performers")
                    if result.studio_added:
                        update_summary.append(f"studio: {result.studio_added}")
                    if "title" in updates:
                        update_summary.append("title")
                    if "details" in updates:
                        update_summary.append("details")

                    self.logger.info(
                        f"[DRY RUN] Would update {item.type} {item.id} from {scraper.name}: "
                        f"{', '.join(update_summary)}"
                    )
                    result.scrape_time_seconds = time.time() - scrape_start_time
                    return result

                # Actually update the item
                if not updates:
                    self.logger.warning(f"No valid metadata to update from {scraper.name}")
                    if scraper == scrapers_to_try[-1]:
                        result.error = "No valid metadata from any scraper"
                    continue

                success, fields_updated = self.stash.update_item_metadata(item, updates)
                if success:
                    result.success = True
                    result.metadata_updated = fields_updated

                    update_summary = []
                    if result.tags_added:
                        update_summary.append(f"{len(result.tags_added)} tags")
                    if result.performers_added:
                        update_summary.append(f"{len(result.performers_added)} performers")
                    if result.studio_added:
                        update_summary.append(f"studio: {result.studio_added}")
                    if "title" in fields_updated:
                        update_summary.append("title")
                    if "details" in fields_updated:
                        update_summary.append("details")

                    self.logger.info(
                        f"Updated {item.type} {item.id} from {scraper.name}: {', '.join(update_summary)}"
                    )
                    result.scrape_time_seconds = time.time() - scrape_start_time
                    return result
                else:
                    result.error = "Failed to update item"
                    result.scrape_time_seconds = time.time() - scrape_start_time
                    return result

            except Exception as e:
                self.logger.error(f"Error scraping {item.type} {item.id} with {scraper.name}: {e}")
                if scraper == scrapers_to_try[-1]:  # Last scraper
                    result.error = str(e)
                    result.scrape_time_seconds = time.time() - scrape_start_time
                continue

        result.scrape_time_seconds = time.time() - scrape_start_time
        return result

    def scrape_all(self, item_type: str, scraper_name: Optional[str] = None,
                  limit: Optional[int] = None) -> List[ScrapeResult]:
        """Scrape all items of a given type."""
        results = []

        # List available scrapers
        scraper_type = "SCENE" if item_type == "scene" else "IMAGE"
        all_scrapers = self.stash.list_scrapers(scraper_type)

        if not all_scrapers:
            self.logger.error(f"No scrapers found for type {scraper_type}")
            return results

        # Order scrapers - primary first, then others, generic/auto scrapers last
        # Also filter to only include scrapers that support FRAGMENT scraping
        def is_generic_scraper(scraper: Scraper) -> bool:
            """Check if scraper is a generic/fallback type that should run last."""
            generic_keywords = ['auto', 'generic', 'fallback', 'default', 'universal']
            return any(keyword in scraper.name.lower() for keyword in generic_keywords)

        # Filter to only scrapers that support fragment scraping
        fragment_scrapers = [s for s in all_scrapers if "FRAGMENT" in s.supported_scrapes]

        if not fragment_scrapers:
            self.logger.error(f"No scrapers support FRAGMENT scraping for type {scraper_type}")
            return results

        if scraper_name:
            primary_scraper = None
            specific_scrapers = []
            generic_scrapers = []

            for s in fragment_scrapers:
                if s.name.lower() == scraper_name.lower():
                    primary_scraper = s
                elif is_generic_scraper(s):
                    generic_scrapers.append(s)
                else:
                    specific_scrapers.append(s)

            if not primary_scraper:
                self.logger.error(f"Scraper '{scraper_name}' not found or doesn't support FRAGMENT scraping. Available: {[s.name for s in fragment_scrapers]}")
                return results

            # Put primary first, then specific scrapers, then generic ones last
            ordered_scrapers = [primary_scraper] + specific_scrapers + generic_scrapers
        else:
            # Separate specific and generic scrapers
            specific_scrapers = [s for s in fragment_scrapers if not is_generic_scraper(s)]
            generic_scrapers = [s for s in fragment_scrapers if is_generic_scraper(s)]
            # Specific scrapers first, generic last
            ordered_scrapers = specific_scrapers + generic_scrapers

        self.logger.info(f"Primary scraper: {ordered_scrapers[0].name}")
        self.logger.info(f"Supported scrapes: {ordered_scrapers[0].supported_scrapes}")

        if self.try_all_scrapers and len(ordered_scrapers) > 1:
            fallback_names = [s.name for s in ordered_scrapers[1:]]
            self.logger.info(f"Fallback scrapers enabled (in order): {fallback_names}")

            # Highlight if generic scrapers are last
            generic_fallbacks = [s.name for s in ordered_scrapers[1:] if is_generic_scraper(s)]
            if generic_fallbacks:
                self.logger.info(f"Generic scrapers deprioritized to end: {generic_fallbacks}")

        # Fetch items with pagination
        page = 1
        total_items = None
        items_fetched = 0

        # Determine organized filter
        organized_filter = False if self.skip_organized else None

        # Setup progress bar
        pbar = None

        while True:
            if item_type == "scene":
                items, total_count = self.stash.find_scenes(
                    page=page,
                    per_page=BATCH_SIZE,
                    organized_filter=organized_filter
                )
            else:
                items, total_count = self.stash.find_images(
                    page=page,
                    per_page=BATCH_SIZE,
                    organized_filter=organized_filter
                )

            if total_items is None:
                total_items = min(total_count, limit) if limit else total_count
                self.stats.total = total_items
                self.logger.info(f"Found {total_count} {item_type}s, will process {total_items}")

                # Initialize progress bar
                if TQDM_AVAILABLE:
                    pbar = tqdm(
                        total=total_items,
                        desc=f"Scraping {item_type}s",
                        unit="items",
                        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                    )

            if not items:
                break

            for item in items:
                if limit and items_fetched >= limit:
                    break

                items_fetched += 1
                self.stats.processed += 1

                # Update progress bar or log
                if pbar:
                    pbar.update(1)
                else:
                    percent = (self.stats.processed / self.stats.total * 100) if self.stats.total > 0 else 0
                    eta = self.stats.format_eta()
                    self.logger.info(
                        f"Processing {item.type} {self.stats.processed}/{self.stats.total} "
                        f"({percent:.1f}%) - ETA: {eta}"
                    )

                result = self.scrape_item(item, ordered_scrapers)
                results.append(result)

                # Update stats
                self.stats.total_scrape_time += result.scrape_time_seconds

                if result.success:
                    self.stats.successful += 1
                    self.stats.tags_created += len(result.tags_created)
                    self.stats.tags_added += len(result.tags_added)
                    self.stats.performers_created += len(result.performers_created)
                    self.stats.performers_added += len(result.performers_added)
                    if result.studio_created:
                        self.stats.studios_created += 1
                    if result.studio_added:
                        self.stats.studios_added += 1
                    if result.fallback_used:
                        self.stats.fallback_used_count += 1

                    # Track metadata fields
                    for field in result.metadata_updated:
                        self.stats.metadata_fields_updated[field] = \
                            self.stats.metadata_fields_updated.get(field, 0) + 1

                    # Track scraper success
                    self.stats.scraper_success_count[result.scraper_name] = \
                        self.stats.scraper_success_count.get(result.scraper_name, 0) + 1

                elif result.skipped:
                    self.stats.skipped += 1
                else:
                    self.stats.failed += 1
                    # Track scraper failure
                    self.stats.scraper_failure_count[result.scraper_name] = \
                        self.stats.scraper_failure_count.get(result.scraper_name, 0) + 1

            if limit and items_fetched >= limit:
                break

            if len(items) < BATCH_SIZE:
                break

            page += 1

        # Close progress bar
        if pbar:
            pbar.close()

        return results

    def print_summary(self):
        """Print scraping statistics."""
        elapsed = self.stats.elapsed_seconds()
        rate = self.stats.items_per_second()
        avg_scrape_time = self.stats.total_scrape_time / self.stats.processed if self.stats.processed > 0 else 0

        print("\n" + "=" * 80)
        print("BULK SCRAPE SUMMARY")
        print("=" * 80)

        # Basic stats
        print(f"  Total items:                 {self.stats.total}")
        print(f"  Processed:                   {self.stats.processed}")
        print(f"  Successful:                  {self.stats.successful} ({self.stats.successful/self.stats.processed*100:.1f}%)" if self.stats.processed > 0 else "  Successful:                  0")
        print(f"  Skipped:                     {self.stats.skipped}")
        print(f"  Failed:                      {self.stats.failed}")
        print()

        # Metadata stats
        print("Metadata Created:")
        print(f"  Tags:                        {self.stats.tags_created}")
        print(f"  Performers:                  {self.stats.performers_created}")
        print(f"  Studios:                     {self.stats.studios_created}")
        print()

        print("Metadata Added:")
        print(f"  Tags (total):                {self.stats.tags_added}")
        print(f"  Performers (total):          {self.stats.performers_added}")
        print(f"  Studios (total):             {self.stats.studios_added}")
        print()

        # Metadata fields updated
        if self.stats.metadata_fields_updated:
            print("Metadata Fields Updated:")
            for field, count in sorted(self.stats.metadata_fields_updated.items()):
                print(f"  {field:28s} {count}")
            print()

        # Scraper performance
        if self.stats.scraper_success_count:
            print("Scraper Success Rates:")
            for scraper in sorted(self.stats.scraper_success_count.keys()):
                success = self.stats.scraper_success_count.get(scraper, 0)
                failure = self.stats.scraper_failure_count.get(scraper, 0)
                total = success + failure
                success_rate = (success / total * 100) if total > 0 else 0
                print(f"  {scraper:28s} {success}/{total} ({success_rate:.1f}%)")
            print()

        # Fallback stats
        if self.stats.fallback_used_count > 0:
            print(f"Fallback scraper used:       {self.stats.fallback_used_count} times")
            print()

        # Timing stats
        print("Performance:")
        print(f"  Total time:                  {timedelta(seconds=int(elapsed))}")
        print(f"  Scraping time:               {timedelta(seconds=int(self.stats.total_scrape_time))}")
        print(f"  Overhead time:               {timedelta(seconds=int(elapsed - self.stats.total_scrape_time))}")
        print(f"  Rate:                        {rate:.2f} items/sec")
        print(f"  Avg scrape time:             {avg_scrape_time:.2f} sec/item")
        print("=" * 80)

# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Bulk scrape all images/scenes in Stash using installed scrapers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run - see what would happen
  %(prog)s --type image --dry-run

  # Scrape all images with a specific scraper
  %(prog)s --type image --scraper "Rule34.xxx"

  # Scrape all scenes, skip already organized
  %(prog)s --type scene --skip-organized

  # Test with first 10 images
  %(prog)s --type image --limit 10 --verbose

  # Scrape both images and scenes
  %(prog)s --type both --scraper "Rule34.xxx"

  # Process only items added since a specific date
  %(prog)s --type image --since 2024-01-15

  # Process items added during a specific time period
  %(prog)s --type image --between 2024-01-01 2024-01-31

  # Process old items not yet scraped
  %(prog)s --type image --before 2023-12-31 --skip-tagged

  # Use creation time instead of modification time
  %(prog)s --type image --since 2024-01-01 --timestamp-type ctime

  # Save detailed log to file
  %(prog)s --type image --log-file scrape.log --verbose
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

    # Scraping options
    scrape_group = parser.add_argument_group("Scraping Options")
    scrape_group.add_argument(
        "--type", "-t",
        choices=["image", "scene", "both"],
        required=True,
        help="Type of items to scrape"
    )
    scrape_group.add_argument(
        "--scraper", "-s",
        help="Specific scraper to use (by name). If not specified, uses first available."
    )
    scrape_group.add_argument(
        "--limit", "-l",
        type=int,
        help="Limit number of items to process (for testing)"
    )
    scrape_group.add_argument(
        "--try-all-scrapers",
        action="store_true",
        help="If primary scraper returns no results, try other available scrapers as fallback"
    )

    # Filtering
    filter_group = parser.add_argument_group("Filtering")
    filter_group.add_argument(
        "--skip-organized",
        action="store_true",
        help="Skip items that are already marked as organized"
    )
    filter_group.add_argument(
        "--skip-tagged",
        action="store_true",
        help="Skip items that already have tags"
    )
    filter_group.add_argument(
        "--skip-if-has-tag",
        action="append",
        metavar="TAG",
        help="Skip items that have this tag (can be used multiple times). Example: --skip-if-has-tag '[scraped]' --skip-if-has-tag 'auto-tagged'"
    )
    filter_group.add_argument(
        "--timestamp-type",
        choices=["mtime", "ctime"],
        default="mtime",
        help="Which file timestamp to use for date filtering: mtime (modification time, default) or ctime (creation time)"
    )
    filter_group.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="Only process items added to disk on or after this date (inclusive). Example: --since 2024-01-15"
    )
    filter_group.add_argument(
        "--before",
        metavar="YYYY-MM-DD",
        help="Only process items added to disk on or before this date (inclusive). Example: --before 2024-12-31"
    )
    filter_group.add_argument(
        "--between",
        nargs=2,
        metavar=("START", "END"),
        help="Only process items added between two dates (inclusive). Format: --between YYYY-MM-DD YYYY-MM-DD. Example: --between 2024-01-01 2024-01-31"
    )

    # Behavior
    behavior_group = parser.add_argument_group("Behavior")
    behavior_group.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Don't actually update anything, just show what would be done"
    )
    behavior_group.add_argument(
        "--rate-limit",
        type=float,
        default=None,
        help=f"Seconds between scrape requests (default: {DEFAULT_RATE_LIMIT} for remote, {DEFAULT_RATE_LIMIT_LOCAL} for localhost, use 0 to disable)"
    )
    behavior_group.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT})"
    )
    behavior_group.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help=f"Maximum retries for failed requests (default: {MAX_RETRIES})"
    )

    # Output
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
        "--log-file",
        help="Write logs to file"
    )
    output_group.add_argument(
        "--json-output",
        help="Write results to JSON file"
    )

    # Utility
    util_group = parser.add_argument_group("Utility")
    util_group.add_argument(
        "--test-connection",
        action="store_true",
        help="Test connection to Stash and exit"
    )
    util_group.add_argument(
        "--list-scrapers",
        action="store_true",
        help="List available scrapers and exit"
    )

    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_args()

    # Setup logging
    logger = setup_logging(args.verbose, args.debug, args.log_file)

    # Detect if connecting to localhost and apply optimized defaults
    is_local = is_localhost(args.stash_url)
    if is_local:
        logger.debug("Detected localhost connection - using optimized settings")

    # Apply optimized defaults for localhost if not explicitly set by user
    if args.rate_limit is None:
        args.rate_limit = DEFAULT_RATE_LIMIT_LOCAL if is_local else DEFAULT_RATE_LIMIT
        if is_local:
            logger.debug(f"Using optimized rate limit for localhost: {args.rate_limit}s")

    if args.timeout == DEFAULT_TIMEOUT and is_local:
        args.timeout = DEFAULT_TIMEOUT_LOCAL
        logger.debug(f"Using optimized timeout for localhost: {args.timeout}s")

    if args.max_retries == MAX_RETRIES and is_local:
        args.max_retries = MAX_RETRIES_LOCAL
        logger.debug(f"Using optimized retries for localhost: {args.max_retries}")

    # Parse and validate date filters
    date_since = None
    date_before = None

    if args.between:
        try:
            date_since = parse_date(args.between[0])
            date_before = parse_date(args.between[1])
            if date_since > date_before:
                logger.error(f"--between start date ({args.between[0]}) must be before or equal to end date ({args.between[1]})")
                sys.exit(1)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)

    if args.since:
        try:
            since_date = parse_date(args.since)
            # Combine with --between start date if both specified (AND logic)
            if date_since:
                date_since = max(date_since, since_date)
            else:
                date_since = since_date
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)

    if args.before:
        try:
            before_date = parse_date(args.before)
            # Combine with --between end date if both specified (AND logic)
            if date_before:
                date_before = min(date_before, before_date)
            else:
                date_before = before_date
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)

    # Validate combined date range
    if date_since and date_before and date_since > date_before:
        logger.error(f"Date filter conflict: --since {date_since.strftime('%Y-%m-%d')} is after --before {date_before.strftime('%Y-%m-%d')}")
        sys.exit(1)

    # Create HTTP session
    session = create_session(max_retries=args.max_retries, is_local=is_local)

    # Create Stash client
    stash = StashClient(
        base_url=args.stash_url,
        api_key=args.api_key,
        session=session,
        logger=logger,
        timeout=args.timeout,
        timestamp_type=args.timestamp_type,
        need_timestamps=bool(date_since or date_before)
    )

    # Test connection
    if args.test_connection:
        print(f"Testing connection to Stash at {args.stash_url}...")
        if stash.test_connection():
            print("✓ Connection successful!")
            sys.exit(0)
        else:
            print("✗ Connection failed!")
            sys.exit(1)

    # List scrapers
    if args.list_scrapers:
        print("Available scrapers:\n")

        if args.type in ["image", "both"]:
            image_scrapers = stash.list_scrapers("IMAGE")
            print("IMAGE scrapers:")
            for scraper in image_scrapers:
                print(f"  - {scraper.name} (supports: {', '.join(scraper.supported_scrapes)})")
            print()

        if args.type in ["scene", "both"]:
            scene_scrapers = stash.list_scrapers("SCENE")
            print("SCENE scrapers:")
            for scraper in scene_scrapers:
                print(f"  - {scraper.name} (supports: {', '.join(scraper.supported_scrapes)})")

        sys.exit(0)

    # Test Stash connection
    print(f"Connecting to Stash at {args.stash_url}...")
    if not stash.test_connection():
        print("Failed to connect to Stash. Check URL and API key.")
        sys.exit(1)
    print("✓ Connected to Stash")
    if is_local:
        print(f"  Localhost detected - optimized settings: {args.rate_limit}s rate limit, {args.max_retries} retries")
    print()

    if args.dry_run:
        print("=" * 70)
        print("DRY RUN MODE - No changes will be made")
        print("=" * 70 + "\n")

    # Show filtering options
    if args.skip_if_has_tag:
        logger.info(f"Will skip items with these tags: {args.skip_if_has_tag}")

    if date_since or date_before:
        logger.info(f"Using {args.timestamp_type} for date filtering")
        if date_since and date_before:
            logger.info(f"Will process items from {date_since.strftime('%Y-%m-%d')} to {date_before.strftime('%Y-%m-%d')} (inclusive)")
        elif date_since:
            logger.info(f"Will process items since {date_since.strftime('%Y-%m-%d')} (inclusive)")
        elif date_before:
            logger.info(f"Will process items before {date_before.strftime('%Y-%m-%d')} (inclusive)")

    # Create bulk scraper
    scraper = BulkScraper(
        stash_client=stash,
        logger=logger,
        rate_limit=args.rate_limit,
        dry_run=args.dry_run,
        skip_organized=args.skip_organized,
        skip_tagged=args.skip_tagged,
        try_all_scrapers=args.try_all_scrapers,
        skip_if_has_tags=args.skip_if_has_tag,
        date_since=date_since,
        date_before=date_before
    )

    # Run scraping
    all_results = []

    if args.type in ["image", "both"]:
        logger.info("Starting image scraping...")
        results = scraper.scrape_all("image", scraper_name=args.scraper, limit=args.limit)
        all_results.extend(results)

    if args.type in ["scene", "both"]:
        logger.info("Starting scene scraping...")
        # Reset stats if we already did images
        if args.type == "both":
            scraper.stats = ProgressStats()
        results = scraper.scrape_all("scene", scraper_name=args.scraper, limit=args.limit)
        all_results.extend(results)

    # Print summary
    scraper.print_summary()

    # Save JSON output
    if args.json_output:
        output_data = {
            "summary": {
                "total": scraper.stats.total,
                "processed": scraper.stats.processed,
                "successful": scraper.stats.successful,
                "success_rate": scraper.stats.successful / scraper.stats.processed if scraper.stats.processed > 0 else 0,
                "skipped": scraper.stats.skipped,
                "failed": scraper.stats.failed,
                "elapsed_seconds": scraper.stats.elapsed_seconds(),
                "total_scrape_time_seconds": scraper.stats.total_scrape_time,
                "rate_items_per_second": scraper.stats.items_per_second(),
                "avg_scrape_time_seconds": scraper.stats.total_scrape_time / scraper.stats.processed if scraper.stats.processed > 0 else 0,
            },
            "metadata_created": {
                "tags": scraper.stats.tags_created,
                "performers": scraper.stats.performers_created,
                "studios": scraper.stats.studios_created,
            },
            "metadata_added": {
                "tags": scraper.stats.tags_added,
                "performers": scraper.stats.performers_added,
                "studios": scraper.stats.studios_added,
            },
            "metadata_fields_updated": scraper.stats.metadata_fields_updated,
            "scraper_performance": {
                "success_counts": scraper.stats.scraper_success_count,
                "failure_counts": scraper.stats.scraper_failure_count,
                "success_rates": {
                    scraper_name: scraper.stats.scraper_success_count.get(scraper_name, 0) /
                                  (scraper.stats.scraper_success_count.get(scraper_name, 0) +
                                   scraper.stats.scraper_failure_count.get(scraper_name, 0))
                    for scraper_name in set(list(scraper.stats.scraper_success_count.keys()) +
                                           list(scraper.stats.scraper_failure_count.keys()))
                }
            },
            "fallback_used_count": scraper.stats.fallback_used_count,
            "results": [
                {
                    "timestamp": r.timestamp,
                    "item_id": r.item.id,
                    "item_type": r.item.type,
                    "item_path": r.item.path,
                    "item_title": r.item.title,
                    "scraper_used": r.scraper_name,
                    "fallback_used": r.fallback_used,
                    "success": r.success,
                    "skipped": r.skipped,
                    "skip_reason": r.skip_reason,
                    "error": r.error,
                    "scrape_time_seconds": r.scrape_time_seconds,
                    "tags_created": r.tags_created,
                    "tags_added": r.tags_added,
                    "performers_created": r.performers_created,
                    "performers_added": r.performers_added,
                    "studio_created": r.studio_created,
                    "studio_added": r.studio_added,
                    "metadata_fields_updated": list(r.metadata_updated.keys())
                }
                for r in all_results
            ]
        }

        with open(args.json_output, "w") as f:
            json.dump(output_data, f, indent=2)

        print(f"\nDetailed results saved to: {args.json_output}")

if __name__ == "__main__":
    main()
