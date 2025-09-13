"""
Enhanced Intelligent Playwright-Based Scraping Agent for Complete Tulire Listings
Scrapes ALL available listings with focus on specific keys: title, address, price, bedroom, bathroom, description, rental terms, amenities, pet_friendly
NOW SUPPORTS: Complete data overwrite option for fresh scraping runs
FIXED: Unicode encoding errors and timeout issues
"""

import asyncio
import json
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging
from urllib.parse import urljoin, urlparse
import random
import os
import sys
from dataclasses import dataclass, asdict
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pymongo
import certifi
from dotenv import load_dotenv
from db.connection import MongoDBClient

# Load environment variables
load_dotenv()

DATABASE_NAME = "rental_database"
MONGODB_URL = os.getenv("MONGODB_URL_KEY")
ca = certifi.where()

@dataclass
class ListingData:
    """Enhanced data class for rental listing information with specific keys"""
    title: str = ""
    address: str = ""
    price: Optional[int] = None
    bedroom: Optional[int] = None
    bathroom: Optional[float] = None
    description: str = ""
    rental_terms: Dict[str, Any] = None
    amenities: Dict[str, Any] = None
    pet_friendly: Optional[str] = None
    listing_url: str = ""
    scraped_at: str = ""
    
    def __post_init__(self):
        if self.rental_terms is None:
            self.rental_terms = {}
        if self.amenities is None:
            self.amenities = {
                "appliances": [],
                "utilities_included": [],
                "other_amenities": []
            }

class EnhancedPlaywrightScrapingAgent:
    """Enhanced Playwright-based scraping agent for complete Tulire data extraction"""
    
    def __init__(self, 
                 headless: bool = True,
                 max_listings: Optional[int] = None,
                 delay_range: tuple = (2, 4),
                 save_to_db: bool = True,
                 batch_size: int = 10,
                 overwrite_data: bool = False):
        
        self.headless = headless
        self.max_listings = max_listings
        self.delay_range = delay_range
        self.save_to_db = save_to_db
        self.batch_size = batch_size
        self.overwrite_data = overwrite_data
        
        # URLs and selectors
        self.base_url = "https://tulirealty.appfolio.com"
        self.listings_url = f"{self.base_url}/listings/listings"
        
        # Browser instances
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        
        # MongoDB setup
        self.mongo_client = None
        self.collection = None
        self.mongodb_connected = False
        self.data_cleared = False
        
        if self.save_to_db:
            try:
                self.mongo_client = MongoDBClient()
                self.collection = self.mongo_client.database['tulire_listings']
                self.collection.count_documents({})
                self.mongodb_connected = True
            except Exception as e:
                print(f"MongoDB connection failed: {e}")
                self.save_to_db = False
        
        # Setup logging with UTF-8 encoding fix
        self.logger = self._setup_logging()
        
        # Enhanced selectors based on Tulire page structure
        self.selectors = {
            'listing_links': 'a[href*="/listings/detail/"]',
            'title': [
                'h1',
                '.listing-title', 
                '.property-title',
                'title',
                '[data-testid="listing-title"]'
            ],
            'address': [
                '.address',
                '.property-address',
                '[data-testid="address"]',
                '.listing-address',
                'address'
            ],
            'price': [
                '.rent-price',
                '.price',
                '[class*="price"]',
                '[data-testid="price"]',
                '.rental-price'
            ],
            'bed_bath_info': [
                '.bed-bath',
                '.property-details',
                '.listing-details',
                '[class*="bed"]',
                '[class*="bath"]'
            ],
            'description': [
                '.property-description',
                '.listing-description',
                '.description',
                '[class*="description"]',
                'p'
            ],
            'utilities_section': [
                '.utilities',
                '[class*="utilities"]',
                'h3:has-text("Utilities"), h4:has-text("Utilities")',
                'strong:has-text("Utilities")'
            ],
            'appliances_section': [
                '.appliances',
                '[class*="appliances"]',
                'h3:has-text("Appliances"), h4:has-text("Appliances")',
                'strong:has-text("Appliances")'
            ],
            'rental_terms_section': [
                '.rental-terms',
                '.lease-terms',
                '[class*="terms"]',
                'h3:has-text("Rental Terms"), h4:has-text("Rental Terms")'
            ]
        }

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration with UTF-8 encoding fix for Windows"""
        # Ensure logs directory exists
        log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"complete_tulire_scraper_{timestamp}.log"
        log_path = os.path.join(log_dir, log_filename)

        # Create a custom handler that handles Unicode properly
        class SafeStreamHandler(logging.StreamHandler):
            def emit(self, record):
                try:
                    msg = self.format(record)
                    # Remove emojis and special Unicode characters for console output
                    safe_msg = re.sub(r'[^\x00-\x7F]+', '', msg)
                    stream = self.stream
                    stream.write(safe_msg + self.terminator)
                    self.flush()
                except Exception:
                    self.handleError(record)

        # Configure logging with safe handlers
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        logger.handlers.clear()
        
        # File handler with UTF-8 encoding
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Safe console handler
        console_handler = SafeStreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    async def clear_existing_data(self) -> bool:
        """Clear all existing Tulire data from MongoDB collection"""
        if not self.mongodb_connected or self.data_cleared:
            return False
        
        try:
            current_count = self.collection.count_documents({})
            
            if current_count > 0:
                self.logger.info(f"OVERWRITE MODE: Clearing {current_count} existing listings from MongoDB...")
                
                result = self.collection.delete_many({})
                
                self.logger.info(f"Successfully cleared {result.deleted_count} existing listings")
                self.data_cleared = True
                return True
            else:
                self.logger.info("No existing data found in MongoDB")
                self.data_cleared = True
                return True
                
        except Exception as e:
            self.logger.error(f"Error clearing existing data: {e}")
            return False
    
    async def initialize_browser(self) -> None:
        """Initialize Playwright browser with enhanced settings for difficult sites"""
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-bgsync',
                '--disable-extensions',
                '--disable-default-apps',
                '--no-first-run',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        self.logger.info("Browser initialized successfully")

    async def close_browser(self) -> None:
        """Clean up browser resources"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()

    async def random_delay(self) -> None:
        """Add random delay to appear more human-like"""
        delay = random.uniform(*self.delay_range)
        await asyncio.sleep(delay)

    async def safe_navigate_to_page(self, page: Page, url: str, max_retries: int = 3) -> bool:
        """Safely navigate to a page with retries and different wait strategies"""
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1}/{max_retries}: Navigating to {url}")
                
                # Try different wait strategies
                wait_strategies = ['networkidle', 'load', 'domcontentloaded']
                wait_strategy = wait_strategies[attempt % len(wait_strategies)]
                
                # Increase timeout progressively
                timeout = 30000 + (attempt * 15000)  # 30s, 45s, 60s
                
                await page.goto(url, wait_until=wait_strategy, timeout=timeout)
                
                # Wait a bit more and check if page loaded
                await asyncio.sleep(2)
                
                # Check if we can find some content
                title = await page.title()
                if title and len(title) > 0:
                    self.logger.info(f"Successfully navigated to page: {title}")
                    return True
                
            except PlaywrightTimeoutError as e:
                self.logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                else:
                    self.logger.error(f"Failed to navigate to {url} after {max_retries} attempts")
                    return False
            except Exception as e:
                self.logger.error(f"Navigation error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                else:
                    return False
        
        return False

    async def safe_get_text(self, page: Page, selectors: List[str], default: str = "") -> str:
        """Safely get text from multiple possible selectors"""
        for selector in selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            except Exception:
                continue
        return default

    async def extract_listing_urls_with_pagination(self, page: Page) -> List[str]:
        """Extract all listing URLs handling pagination with improved error handling"""
        all_urls = []
        page_num = 1
        max_pages = 20
        
        try:
            while page_num <= max_pages:
                self.logger.info(f"Processing page {page_num}...")
                
                # Wait for listings to load with multiple attempts
                listings_found = False
                wait_attempts = 0
                max_wait_attempts = 3
                
                while not listings_found and wait_attempts < max_wait_attempts:
                    try:
                        await page.wait_for_selector(self.selectors['listing_links'], timeout=20000)
                        listings_found = True
                    except PlaywrightTimeoutError:
                        wait_attempts += 1
                        self.logger.warning(f"Waiting for listings, attempt {wait_attempts}/{max_wait_attempts}")
                        if wait_attempts < max_wait_attempts:
                            await asyncio.sleep(5)
                            # Try to refresh the page
                            await page.reload()
                            await asyncio.sleep(3)
                        continue
                
                if not listings_found:
                    self.logger.warning(f"No listings found on page {page_num} after {max_wait_attempts} attempts")
                    break
                
                # Extract URLs from current page
                links = await page.query_selector_all(self.selectors['listing_links'])
                page_urls = []
                
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in all_urls:
                            page_urls.append(full_url)
                            all_urls.append(full_url)
                
                self.logger.info(f"Found {len(page_urls)} new listings on page {page_num} (Total: {len(all_urls)})")
                
                if not page_urls:
                    self.logger.info("No more listings found")
                    break
                
                # Try to find and click next page button
                next_button = None
                next_selectors = [
                    'a:has-text("Next")',
                    'a[aria-label="Next"]',
                    '.pagination a:last-child',
                    'a[href*="page="]:last-child',
                    '.next-page',
                    '[class*="next"]'
                ]
                
                for selector in next_selectors:
                    try:
                        next_button = await page.query_selector(selector)
                        if next_button:
                            is_disabled = await next_button.get_attribute('disabled')
                            aria_disabled = await next_button.get_attribute('aria_disabled')
                            
                            if is_disabled == 'true' or aria_disabled == 'true':
                                next_button = None
                                continue
                            break
                    except Exception:
                        continue
                
                if not next_button:
                    self.logger.info("No more pages found")
                    break
                
                # Click next page and wait
                try:
                    await next_button.click()
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    await self.random_delay()
                    page_num += 1
                except Exception as e:
                    self.logger.warning(f"Could not navigate to next page: {e}")
                    break
            
            self.logger.info(f"Total listings found across all pages: {len(all_urls)}")
            return all_urls
            
        except Exception as e:
            self.logger.error(f"Error extracting listing URLs: {e}")
            return all_urls
    
    def extract_price_from_text(self, price_text: str) -> Optional[int]:
        """Extract numeric price from price text"""
        if not price_text:
            return None
        
        # Remove $ and commas, extract numbers
        price_match = re.search(r'\$?([\d,]+)', price_text.replace(',', ''))
        if price_match:
            return int(price_match.group(1).replace(',', ''))
        return None

    def extract_bed_bath_from_text(self, text: str) -> Dict[str, Optional[float]]:
        """Extract bedroom and bathroom counts from text"""
        result = {'bedroom': None, 'bathroom': None}
        
        if not text:
            return result
        
        text_lower = text.lower()
        
        if 'studio' in text_lower:
            result['bedroom'] = 0
        
        bed_patterns = [
            r'(\d+)\s*(?:bd|bedroom|br)s?',
            r'(\d+)\s*bed',
            r'(\d+)\s*bd'
        ]
        
        for pattern in bed_patterns:
            match = re.search(pattern, text_lower)
            if match and result['bedroom'] is None:
                result['bedroom'] = int(match.group(1))
                break
        
        bath_patterns = [
            r'(\d+(?:\.\d+)?)\s*(?:ba|bath|bathroom)s?',
            r'(\d+(?:\.\d+)?)\s*bath',
            r'(\d+(?:\.\d+)?)\s*ba'
        ]
        
        for pattern in bath_patterns:
            match = re.search(pattern, text_lower)
            if match:
                result['bathroom'] = float(match.group(1))
                break
        
        return result

    async def extract_section_content(self, page: Page, section_name: str) -> List[str]:
        """Extract content from a specific section"""
        content = []
        
        try:
            headers = await page.query_selector_all(f'h3:has-text("{section_name}"), h4:has-text("{section_name}"), strong:has-text("{section_name}")')
            
            for header in headers:
                parent = await header.query_selector('..')
                if parent:
                    items = await parent.query_selector_all('li, p, div')
                    for item in items:
                        text = await item.text_content()
                        if text and text.strip() and text.strip() != section_name:
                            content.append(text.strip())
                
                next_element = await page.evaluate('(element) => element.nextElementSibling', header)
                if next_element:
                    text = await page.evaluate('(element) => element.textContent', next_element)
                    if text and text.strip():
                        content.append(text.strip())
        
        except Exception as e:
            self.logger.debug(f"Error extracting {section_name} section: {e}")
        
        return content

    async def extract_rental_terms(self, page: Page) -> Dict[str, Any]:
        """Extract rental terms information"""
        rental_terms = {}
        
        try:
            page_text = await page.text_content('body')
            
            rent_match = re.search(r'rent[:\s]*\$?(\d{1,3}(?:,\d{3})*)', page_text, re.IGNORECASE)
            if rent_match:
                rental_terms['rent'] = f"${rent_match.group(1)}"
            
            app_fee_match = re.search(r'application\s+fee[:\s]*\$?(\d+)', page_text, re.IGNORECASE)
            if app_fee_match:
                rental_terms['application_fee'] = f"${app_fee_match.group(1)}"
            
            deposit_match = re.search(r'security\s+deposit[:\s]*\$?([\d,]+)', page_text, re.IGNORECASE)
            if deposit_match:
                rental_terms['security_deposit'] = f"${deposit_match.group(1)}"
            
            avail_match = re.search(r'available[:\s]*([^<\n\r]+)', page_text, re.IGNORECASE)
            if avail_match:
                rental_terms['availability'] = avail_match.group(1).strip()
            
            lease_match = re.search(r'lease[:\s]*([^<\n\r.]+)', page_text, re.IGNORECASE)
            if lease_match:
                rental_terms['lease_terms'] = lease_match.group(1).strip()
            
        except Exception as e:
            self.logger.debug(f"Error extracting rental terms: {e}")
        
        return rental_terms

    async def check_pet_friendly(self, page: Page) -> Optional[str]:
        """Check if the property is pet friendly"""
        try:
            page_text = await page.text_content('body')
            page_text_lower = page_text.lower()
            
            pet_positive = [
                'pets allowed',
                'pet friendly', 
                'pet-friendly',
                'pets welcome',
                'pets ok',
                'pets: yes'
            ]
            
            pet_negative = [
                'no pets',
                'pets not allowed',
                'no pets allowed',
                'pets: no'
            ]
            
            for positive in pet_positive:
                if positive in page_text_lower:
                    return "Yes"
            
            for negative in pet_negative:
                if negative in page_text_lower:
                    return "No"
            
            if 'pet' in page_text_lower:
                pet_match = re.search(r'pet[s]?[:\s]*([^<\n\r.]{1,50})', page_text_lower)
                if pet_match:
                    return pet_match.group(1).strip().title()
            
        except Exception as e:
            self.logger.debug(f"Error checking pet policy: {e}")
        
        return None

    async def scrape_listing_details(self, page: Page, listing_url: str) -> ListingData:
        """Scrape detailed information from a single listing page"""
        try:
            # Navigate with retry logic
            if not await self.safe_navigate_to_page(page, listing_url):
                self.logger.error(f"Failed to navigate to {listing_url}")
                return ListingData(listing_url=listing_url, scraped_at=datetime.now().isoformat())
            
            await self.random_delay()
            
            listing = ListingData(
                listing_url=listing_url,
                scraped_at=datetime.now().isoformat()
            )
            
            page_text = await page.text_content('body')
            
            # Extract TITLE
            listing.title = await self.safe_get_text(page, self.selectors['title'])
            if not listing.title:
                page_title = await page.title()
                listing.title = re.sub(r'\s*-\s*.*$', '', page_title)
            
            # Extract ADDRESS
            listing.address = await self.safe_get_text(page, self.selectors['address'])
            if not listing.address and page_text:
                address_patterns = [
                    r'(\d+[^,\n]*(?:Avenue|Street|Boulevard|Road|Place|Lane|Drive|Way|Court|St|Ave|Blvd|Rd|Pl|Ln|Dr|Ct)[^,\n]*,\s*[^,\n]*(?:,\s*[A-Z]{2})?)',
                    r'Management\s+(\d+[^,\n]*.+?(?:NJ|NY)\s*\d{5})',
                ]
                for pattern in address_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        listing.address = match.group(1).strip()
                        break
            
            # Extract PRICE
            price_text = await self.safe_get_text(page, self.selectors['price'])
            if not price_text and page_text:
                price_match = re.search(r'\$[\d,]+', page_text)
                if price_match:
                    price_text = price_match.group()
            listing.price = self.extract_price_from_text(price_text if price_text else page_text)
            
            # Extract BEDROOM and BATHROOM
            bed_bath_text = await self.safe_get_text(page, self.selectors['bed_bath_info'])
            if not bed_bath_text:
                bed_bath_text = page_text
            
            bed_bath_info = self.extract_bed_bath_from_text(bed_bath_text)
            listing.bedroom = bed_bath_info['bedroom']
            listing.bathroom = bed_bath_info['bathroom']
            
            # Extract DESCRIPTION
            description_text = await self.safe_get_text(page, self.selectors['description'])
            if not description_text:
                paragraphs = await page.query_selector_all('p')
                for p in paragraphs:
                    text = await p.text_content()
                    if text and len(text) > 100:
                        description_text = text
                        break
            listing.description = description_text
            
            # Extract RENTAL TERMS
            listing.rental_terms = await self.extract_rental_terms(page)
            
            # Extract AMENITIES
            utilities = await self.extract_section_content(page, "Utilities")
            if not utilities and page_text:
                util_patterns = [
                    r'heat\s*\([^)]*\)',
                    r'water\s*\([^)]*\)', 
                    r'electric\s*\([^)]*\)',
                    r'gas\s*\([^)]*\)'
                ]
                for pattern in util_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    utilities.extend(matches)
            
            appliances = await self.extract_section_content(page, "Appliances")
            if not appliances and page_text:
                appliance_keywords = ['refrigerator', 'stove', 'dishwasher', 'microwave', 'washer', 'dryer']
                for keyword in appliance_keywords:
                    if keyword in page_text.lower():
                        appliances.append(keyword.title())
            
            listing.amenities = {
                "appliances": list(set(appliances)),
                "utilities_included": list(set(utilities)),
                "other_amenities": []
            }
            
            # Extract PET_FRIENDLY
            listing.pet_friendly = await self.check_pet_friendly(page)
            
            return listing
            
        except Exception as e:
            self.logger.error(f"Error scraping {listing_url}: {e}")
            return ListingData(listing_url=listing_url, scraped_at=datetime.now().isoformat())

    async def save_to_mongodb_batch(self, listings: List[ListingData]) -> bool:
        """Save listings to MongoDB in batch"""
        if not self.mongodb_connected or not listings:
            return False
        
        try:
            bulk_operations = []
            for listing in listings:
                listing_dict = asdict(listing)
                listing_dict['_id'] = f"tulire_{hash(listing.listing_url)}"
                listing_dict['source'] = 'tulire_realty'
                listing_dict['last_updated'] = datetime.now()
                
                if self.overwrite_data:
                    bulk_operations.append(
                        pymongo.ReplaceOne(
                            {'_id': listing_dict['_id']},
                            listing_dict,
                            upsert=True
                        )
                    )
                else:
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            {'_id': listing_dict['_id']},
                            {'$set': listing_dict},
                            upsert=True
                        )
                    )
            
            if bulk_operations:
                result = self.collection.bulk_write(bulk_operations)
                
                operation_type = "Replaced" if self.overwrite_data else "Inserted/Updated"
                self.logger.info(f"MongoDB Batch Save - {operation_type}: {result.upserted_count + result.modified_count}")
                return True
            
        except Exception as e:
            self.logger.error(f"Error saving batch to MongoDB: {e}")
            return False
        
        return False

    async def scrape_all_listings(self) -> List[ListingData]:
        """Main method to scrape ALL listings with enhanced error handling"""
        try:
            await self.initialize_browser()
            
            if self.overwrite_data and self.save_to_db:
                if not await self.clear_existing_data():
                    self.logger.warning("Failed to clear existing data, continuing anyway...")
            
            page = await self.context.new_page()
            
            self.logger.info("Navigating to listings page...")
            
            # Navigate with enhanced retry logic
            if not await self.safe_navigate_to_page(page, self.listings_url):
                self.logger.error("Failed to navigate to listings page after multiple attempts")
                return []
            
            # Extract ALL listing URLs with pagination
            listing_urls = await self.extract_listing_urls_with_pagination(page)
            
            if not listing_urls:
                self.logger.warning("No listing URLs found")
                return []
            
            if self.max_listings:
                listing_urls = listing_urls[:self.max_listings]
                self.logger.info(f"Limited to {len(listing_urls)} listings")
            else:
                self.logger.info(f"Processing ALL {len(listing_urls)} listings")
            
            all_listings = []
            current_batch = []
            
            for i, url in enumerate(listing_urls, 1):
                try:
                    self.logger.info(f"Scraping listing {i}/{len(listing_urls)}: {url}")
                    listing = await self.scrape_listing_details(page, url)
                    
                    if listing.title and listing.address:
                        current_batch.append(listing)
                        all_listings.append(listing)
                        
                        if len(current_batch) >= self.batch_size:
                            if self.save_to_db:
                                await self.save_to_mongodb_batch(current_batch)
                            current_batch = []
                    else:
                        self.logger.warning(f"Missing compulsory data for {url}")
                    
                    await self.random_delay()
                    
                    if i % 10 == 0:
                        self.logger.info(f"Progress: {i}/{len(listing_urls)} completed ({len(all_listings)} valid)")
                    
                except Exception as e:
                    self.logger.error(f"Error scraping listing {url}: {e}")
                    continue
            
            # Save remaining listings in final batch
            if current_batch and self.save_to_db:
                await self.save_to_mongodb_batch(current_batch)
            
            await page.close()
            
            self.logger.info(f"Successfully scraped {len(all_listings)} listings with complete data")
            return all_listings
            
        except Exception as e:
            self.logger.error(f"Error in scrape_all_listings: {e}")
            return []
        
        finally:
            await self.close_browser()

    def save_to_json(self, listings: List[ListingData], filename: str = None) -> str:
        """Save listings to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            mode_prefix = "fresh_" if self.overwrite_data else "complete_"
            filename = f"{mode_prefix}tulire_listings_{timestamp}.json"
        
        os.makedirs("scraped_data", exist_ok=True)
        filepath = os.path.join("scraped_data", filename)
        
        try:
            listings_dict = [asdict(listing) for listing in listings]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(listings_dict, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"Saved {len(listings)} listings to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
            return ""

    def generate_summary_report(self, listings: List[ListingData]) -> Dict[str, Any]:
        """Generate a comprehensive summary report"""
        if not listings:
            return {}
        
        total = len(listings)
        
        # Price analysis
        prices = [l.price for l in listings if l.price]
        price_stats = {}
        if prices:
            price_stats = {
                'min': min(prices),
                'max': max(prices),
                'avg': sum(prices) // len(prices),
                'median': sorted(prices)[len(prices)//2]
            }
        
        # Bedroom analysis
        bedrooms = [l.bedroom for l in listings if l.bedroom is not None]
        bedroom_counts = {}
        for bed in bedrooms:
            bedroom_counts[bed] = bedroom_counts.get(bed, 0) + 1
        
        # Pet friendly analysis
        pet_friendly_count = len([l for l in listings if l.pet_friendly == "Yes"])
        pet_not_allowed = len([l for l in listings if l.pet_friendly == "No"])
        pet_unknown = total - pet_friendly_count - pet_not_allowed
        
        return {
            'total_listings': total,
            'scrape_mode': 'OVERWRITE' if self.overwrite_data else 'UPDATE',
            'data_completeness': {
                'title': len([l for l in listings if l.title]) / total * 100,
                'price': len([l for l in listings if l.price]) / total * 100,
                'bedroom': len([l for l in listings if l.bedroom is not None]) / total * 100,
                'bathroom': len([l for l in listings if l.bathroom is not None]) / total * 100,
                'description': len([l for l in listings if l.description]) / total * 100,
                'rental_terms': len([l for l in listings if l.rental_terms]) / total * 100,
                'pet_info': len([l for l in listings if l.pet_friendly]) / total * 100,
            },
            'price_analysis': price_stats,
            'bedroom_distribution': bedroom_counts,
            'pet_policy': {
                'allowed': pet_friendly_count,
                'not_allowed': pet_not_allowed,
                'unknown': pet_unknown
            }
        }

async def main():
    """Main execution function for complete scraping with overwrite support"""
    print("ENHANCED TULIRE LISTINGS SCRAPING AGENT WITH DATA OVERWRITE SUPPORT")
    print("Target: ALL available listings with detailed data extraction")
    print("Focus: title, address, price, bedroom, bathroom, description, rental_terms, amenities, pet_friendly")
    print("=" * 100)
    
    # Configuration for COMPLETE scraping with OVERWRITE option
    MAX_LISTINGS = None     # Set to None for ALL listings, or set a number for testing
    HEADLESS = True         # Set to False to see browser in action  
    SAVE_TO_DB = True       # Save to MongoDB
    BATCH_SIZE = 20         # Process and save in batches of 20
    OVERWRITE_DATA = True   # Set to True to completely overwrite existing data
    
    print(f"Configuration:")
    print(f"  - Max Listings: {'ALL AVAILABLE' if MAX_LISTINGS is None else MAX_LISTINGS}")
    print(f"  - Headless Mode: {HEADLESS}")
    print(f"  - Save to MongoDB: {SAVE_TO_DB}")
    print(f"  - Batch Size: {BATCH_SIZE}")
    print(f"  - Data Mode: {'OVERWRITE (Replace All)' if OVERWRITE_DATA else 'UPDATE (Merge with existing)'}")
    print("-" * 100)
    
    if OVERWRITE_DATA:
        print("WARNING: OVERWRITE MODE ENABLED")
        print("   - All existing Tulire data in MongoDB will be DELETED")
        print("   - Fresh scraping will populate the database with new data")
        print("   - This ensures you have the most current listings without duplicates")
        
        if not HEADLESS:
            response = input("\nContinue with data overwrite? (y/N): ").lower().strip()
            if response != 'y' and response != 'yes':
                print("Operation cancelled by user")
                return
        
        print("-" * 100)
    
    agent = EnhancedPlaywrightScrapingAgent(
        headless=HEADLESS,
        max_listings=MAX_LISTINGS,
        save_to_db=SAVE_TO_DB,
        delay_range=(1.5, 3.5),
        batch_size=BATCH_SIZE,
        overwrite_data=OVERWRITE_DATA
    )
    
    try:
        start_time = datetime.now()
        print(f"Starting {'FRESH' if OVERWRITE_DATA else 'UPDATE'} scrape at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Scrape all listings
        listings = await agent.scrape_all_listings()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        if not listings:
            print("No listings found. Check the log file for details")
            return
        
        # Display results
        print(f"\nSCRAPING COMPLETED SUCCESSFULLY!")
        print("=" * 100)
        print(f"Mode: {'FRESH DATA OVERWRITE' if OVERWRITE_DATA else 'DATA UPDATE/MERGE'}")
        print(f"Total Time: {duration}")
        print(f"Total Listings Scraped: {len(listings)}")
        print(f"Average Time per Listing: {duration.total_seconds() / len(listings):.2f} seconds")
        
        # Generate and save comprehensive report
        summary = agent.generate_summary_report(listings)
        
        # Save data to files
        json_file = agent.save_to_json(listings)
        
        # Display comprehensive statistics
        print(f"\nCOMPREHENSIVE SUMMARY REPORT:")
        print("-" * 100)
        
        if summary:
            print(f"Total Listings: {summary['total_listings']}")
            print(f"Scrape Mode: {summary['scrape_mode']}")
            
            print(f"\nData Completeness:")
            for field, percentage in summary['data_completeness'].items():
                bar_length = int(percentage / 5)
                bar = "#" * bar_length + "-" * (20 - bar_length)
                print(f"  {field.replace('_', ' ').title():15} [{bar}] {percentage:.1f}%")
            
            if summary.get('price_analysis'):
                price = summary['price_analysis']
                print(f"\nPrice Analysis:")
                print(f"  Minimum Rent: ${price['min']:,}")
                print(f"  Maximum Rent: ${price['max']:,}")
                print(f"  Average Rent: ${price['avg']:,}")
                print(f"  Median Rent:  ${price['median']:,}")
            
            if summary.get('bedroom_distribution'):
                print(f"\nBedroom Distribution:")
                for bedrooms, count in sorted(summary['bedroom_distribution'].items()):
                    bedroom_text = "Studio" if bedrooms == 0 else f"{bedrooms} BR"
                    percentage = (count / summary['total_listings']) * 100
                    print(f"  {bedroom_text:10} {count:3d} listings ({percentage:.1f}%)")
            
            pet_policy = summary.get('pet_policy', {})
            print(f"\nPet Policy Distribution:")
            print(f"  Pets Allowed:     {pet_policy.get('allowed', 0):3d} listings")
            print(f"  Pets Not Allowed: {pet_policy.get('not_allowed', 0):3d} listings")  
            print(f"  Policy Unknown:   {pet_policy.get('unknown', 0):3d} listings")
        
        # Display sample listings
        print(f"\nSAMPLE LISTINGS:")
        print("-" * 100)
        
        complete_listings = [l for l in listings if l.title and l.address and l.price]
        sample_count = min(3, len(complete_listings))
        
        for i in range(sample_count):
            listing = complete_listings[i]
            print(f"\nSample Listing #{i+1}:")
            print(f"   Title: {listing.title}")
            print(f"   Address: {listing.address}")
            print(f"   Price: ${listing.price:,}" if listing.price else "   Price: Not specified")
            print(f"   Bedrooms: {listing.bedroom}")
            print(f"   Bathrooms: {listing.bathroom}")
            print(f"   Pet Friendly: {listing.pet_friendly or 'Not specified'}")
            
            if listing.rental_terms:
                terms_str = ", ".join([f"{k}: {v}" for k, v in listing.rental_terms.items()])
                print(f"   Rental Terms: {terms_str}")
            
            if listing.amenities and listing.amenities.get('appliances'):
                appliances_str = ", ".join(listing.amenities['appliances'][:3])
                print(f"   Appliances: {appliances_str}{'...' if len(listing.amenities['appliances']) > 3 else ''}")
            
            if listing.description:
                desc_preview = listing.description[:100] + "..." if len(listing.description) > 100 else listing.description
                print(f"   Description: {desc_preview}")
        
        # Save summary report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode_prefix = "fresh_" if OVERWRITE_DATA else "update_"
        summary_file = f"scraped_data/{mode_prefix}tulire_summary_report_{timestamp}.json"
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
            print(f"\nFiles Saved:")
            print(f"   Summary Report: {os.path.basename(summary_file)}")
        except Exception as e:
            print(f"   Could not save summary report: {e}")
        
        db_message = "rental_database.tulire_listings (FRESH DATA)" if OVERWRITE_DATA else "rental_database.tulire_listings (UPDATED DATA)"
        print(f"   MongoDB: {db_message}")
        if json_file:
            print(f"   Complete Data: {os.path.basename(json_file)}")
        
        # Final success message
        mode_text = "FRESH DATA OVERWRITE" if OVERWRITE_DATA else "DATA UPDATE"
        print(f"\nTULIRE SCRAPING WITH {mode_text} COMPLETED!")
        print(f"Successfully processed {len(listings)} listings in {duration}")
        
        if OVERWRITE_DATA:
            print(f"Database now contains FRESH data (old data was cleared)")
        else:
            print(f"Database has been updated with new/changed listings")
        
        # Performance metrics
        if len(listings) > 50:
            print(f"\nPERFORMANCE METRICS:")
            print(f"   Listings per minute: {(len(listings) / duration.total_seconds()) * 60:.1f}")
            print(f"   Success rate: {(len([l for l in listings if l.title and l.address]) / len(listings)) * 100:.1f}%")
        
        print(f"\nNEXT TIME:")
        print(f"   - Set OVERWRITE_DATA = True for fresh database replacement")
        print(f"   - Set OVERWRITE_DATA = False for incremental updates")
        
    except KeyboardInterrupt:
        print(f"\nScraping interrupted by user")
        print(f"Partial results may be available in MongoDB and log files")
        
    except Exception as e:
        print(f"Fatal Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":    
    asyncio.run(main())