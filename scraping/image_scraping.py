import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from typing import List, Dict, Optional
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib
import os
from dataclasses import dataclass

@dataclass
class ImageData:
    """Data class for storing image information"""
    url: str
    alt_text: str = ""
    title: str = ""
    size_info: str = ""
    is_main_image: bool = False

class PropertyImageScraper:
    """
    A comprehensive tool for scraping images from property listing URLs.
    Designed to work with real estate platforms and extract property photos.
    """
    
    def __init__(self, delay: float = 1.0, timeout: int = 30):
        """
        Initialize the scraper with configuration options.
        
        Args:
            delay: Delay between requests in seconds
            timeout: Request timeout in seconds
        """
        self.delay = delay
        self.timeout = timeout
        self.session = self._create_session()
        self.logger = self._setup_logger()
        
        # Common image selectors for different property sites
        self.image_selectors = {
            'appfolio': [
                '.photo-carousel img',
                '.property-photos img',
                '.listing-photos img',
                '.gallery img',
                '.image-gallery img',
                'img[src*="photo"]',
                'img[src*="image"]'
            ],
            'generic': [
                'img[src*="property"]',
                'img[src*="listing"]',
                'img[src*="photo"]',
                'img[src*="image"]',
                '.property-image img',
                '.listing-image img',
                '.gallery img',
                '.carousel img',
                '.slider img',
                'img[alt*="property"]',
                'img[alt*="room"]',
                'img[alt*="apartment"]',
                'img[alt*="house"]'
            ]
        }
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy and headers"""
        session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers to mimic a real browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for the scraper"""
        logger = logging.getLogger('PropertyImageScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _detect_platform(self, url: str) -> str:
        """Detect the property listing platform from URL"""
        if 'appfolio.com' in url:
            return 'appfolio'
        elif 'zillow.com' in url:
            return 'zillow'
        elif 'apartments.com' in url:
            return 'apartments'
        elif 'rent.com' in url:
            return 'rent'
        else:
            return 'generic'
    
    def _is_valid_image_url(self, url: str) -> bool:
        """Check if URL is likely a valid image"""
        if not url:
            return False
        
        # Check for image file extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp']
        url_lower = url.lower()
        
        # Skip common non-image files
        skip_patterns = [
            'logo', 'icon', 'button', 'arrow', 'social', 
            'favicon', 'sprite', 'background', 'banner',
            'ad', 'advertisement', 'tracking'
        ]
        
        if any(pattern in url_lower for pattern in skip_patterns):
            return False
        
        # Must have image extension or contain image-related keywords
        has_extension = any(ext in url_lower for ext in image_extensions)
        has_image_keywords = any(keyword in url_lower for keyword in ['photo', 'image', 'pic', 'property'])
        
        return has_extension or has_image_keywords
    
    def _extract_images_from_html(self, html: str, base_url: str, platform: str) -> List[ImageData]:
        """Extract image URLs and metadata from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        seen_urls = set()
        
        # Get platform-specific selectors
        selectors = self.image_selectors.get(platform, self.image_selectors['generic'])
        
        # Try each selector
        for selector in selectors:
            img_tags = soup.select(selector)
            for img in img_tags:
                # Get image URL
                img_url = img.get('src') or img.get('data-src') or img.get('data-original')
                if not img_url:
                    continue
                
                # Convert relative URLs to absolute
                img_url = urljoin(base_url, img_url)
                
                # Skip if already seen or invalid
                if img_url in seen_urls or not self._is_valid_image_url(img_url):
                    continue
                
                seen_urls.add(img_url)
                
                # Extract metadata
                alt_text = img.get('alt', '')
                title = img.get('title', '')
                
                # Determine if this is a main/hero image
                is_main = any(keyword in (alt_text + title + img.get('class', []).__str__()).lower() 
                            for keyword in ['main', 'hero', 'primary', 'featured'])
                
                images.append(ImageData(
                    url=img_url,
                    alt_text=alt_text,
                    title=title,
                    is_main_image=is_main
                ))
        
        # Also check for images in JavaScript/JSON (common in modern sites)
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string:
                # Look for image URLs in JSON or JavaScript
                img_urls = re.findall(r'https?://[^\s"\'<>]+\.(?:jpg|jpeg|png|webp|gif)', script.string)
                for img_url in img_urls:
                    if img_url not in seen_urls and self._is_valid_image_url(img_url):
                        seen_urls.add(img_url)
                        images.append(ImageData(url=img_url))
        
        return images
    
    def scrape_listing_images(self, listing: Dict) -> Dict:
        """
        Scrape images from a single property listing.
        
        Args:
            listing: Dictionary containing listing information with 'listing_url' key
            
        Returns:
            Dictionary with original listing data plus 'images' key containing image data
        """
        listing_url = listing.get('listing_url')
        if not listing_url:
            self.logger.warning("No listing_url found in listing")
            return {**listing, 'images': [], 'image_scrape_status': 'no_url'}
        
        try:
            self.logger.info(f"Scraping images from: {listing_url}")
            
            # Make request
            response = self.session.get(listing_url, timeout=self.timeout)
            response.raise_for_status()
            
            # Detect platform
            platform = self._detect_platform(listing_url)
            
            # Extract images
            images = self._extract_images_from_html(response.text, listing_url, platform)
            
            # Convert to dictionaries for JSON serialization
            image_data = [
                {
                    'url': img.url,
                    'alt_text': img.alt_text,
                    'title': img.title,
                    'is_main_image': img.is_main_image
                }
                for img in images
            ]
            
            self.logger.info(f"Found {len(image_data)} images for listing")
            
            # Add delay between requests
            time.sleep(self.delay)
            
            return {
                **listing,
                'images': image_data,
                'image_scrape_status': 'success',
                'images_found': len(image_data),
                'scraped_platform': platform
            }
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {listing_url}: {str(e)}")
            return {**listing, 'images': [], 'image_scrape_status': 'request_failed', 'error': str(e)}
        except Exception as e:
            self.logger.error(f"Unexpected error for {listing_url}: {str(e)}")
            return {**listing, 'images': [], 'image_scrape_status': 'error', 'error': str(e)}
    
    def scrape_multiple_listings(self, listings: List[Dict]) -> List[Dict]:
        """
        Scrape images from multiple property listings.
        
        Args:
            listings: List of listing dictionaries, each containing 'listing_url'
            
        Returns:
            List of listings with added image data
        """
        if not listings:
            self.logger.warning("No listings provided")
            return []
        
        self.logger.info(f"Starting to scrape images from {len(listings)} listings")
        
        results = []
        for i, listing in enumerate(listings, 1):
            self.logger.info(f"Processing listing {i}/{len(listings)}")
            result = self.scrape_listing_images(listing)
            results.append(result)
        
        # Summary statistics
        total_images = sum(r.get('images_found', 0) for r in results)
        successful_scrapes = sum(1 for r in results if r.get('image_scrape_status') == 'success')
        
        self.logger.info(f"Scraping complete: {successful_scrapes}/{len(listings)} successful, {total_images} total images found")
        
        return results
    
    def get_image_summary(self, listing_with_images: Dict) -> Dict:
        """
        Get a summary of images for a listing.
        
        Args:
            listing_with_images: Listing dictionary with 'images' key
            
        Returns:
            Dictionary with image summary statistics
        """
        images = listing_with_images.get('images', [])
        
        return {
            'total_images': len(images),
            'main_images': len([img for img in images if img.get('is_main_image', False)]),
            'has_images': len(images) > 0,
            'first_image_url': images[0]['url'] if images else None,
            'all_image_urls': [img['url'] for img in images]
        }

# Usage example and helper functions
def scrape_property_images(listings: List[Dict], delay: float = 1.0) -> List[Dict]:
    """
    Convenience function to scrape images from property listings.
    
    Args:
        listings: List of listing dictionaries with 'listing_url' keys
        delay: Delay between requests in seconds
        
    Returns:
        List of listings with added image data
    """
    scraper = PropertyImageScraper(delay=delay)
    return scraper.scrape_multiple_listings(listings)

def get_images_for_agent(listings_with_images: List[Dict]) -> Dict:
    """
    Format scraped image data for use by a main agent.
    
    Args:
        listings_with_images: List of listings with scraped image data
        
    Returns:
        Dictionary formatted for agent consumption
    """
    agent_data = {
        'total_listings': len(listings_with_images),
        'listings_with_images': 0,
        'total_images': 0,
        'listings': []
    }
    
    for listing in listings_with_images:
        images = listing.get('images', [])
        
        listing_summary = {
            'title': listing.get('title', 'Unknown Title'),
            'address': listing.get('address', 'Unknown Address'),
            'price': listing.get('price'),
            'listing_url': listing.get('listing_url'),
            'images': images,
            'image_count': len(images),
            'main_image': next((img['url'] for img in images if img.get('is_main_image')), 
                              images[0]['url'] if images else None)
        }
        
        agent_data['listings'].append(listing_summary)
        
        if images:
            agent_data['listings_with_images'] += 1
            agent_data['total_images'] += len(images)
    
    return agent_data

# Example usage:
if __name__ == "__main__":
    # Example with your sample listing
    sample_listings = [
        {
            "title": "3316-3318 New York Avenue - BSMNT, Apartment BSMNT, Union City, NJ 07087",
            "address": "Union City, NJ 07087",
            "price": 1850,
            "listing_url": "https://tulirealty.appfolio.com/listings/detail/40247f44-b111-43f2-95d3-d22b3f596f91"
        }
        # Add your other 3 listings here
    ]
    
    # Scrape images
    results = scrape_property_images(sample_listings)
    
    # Format for agent
    agent_data = get_images_for_agent(results)
    
    print(f"Scraped images for {agent_data['listings_with_images']} listings")
    print(f"Total images found: {agent_data['total_images']}")