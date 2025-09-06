import requests
from bs4 import BeautifulSoup
import json
import csv
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import os

class TulireListingsScraper:
    def __init__(self, max_listings=None, use_threading=True, max_workers=5):
        self.base_url = "https://tulirealty.appfolio.com"
        self.listings_url = f"{self.base_url}/listings/listings"
        self.max_listings = max_listings  # None means scrape all
        self.use_threading = use_threading
        self.max_workers = max_workers
        
        # Create scraped_data directory
        self.data_dir = "scraped_data"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Setup session with realistic headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        })
        
        # Setup logging - keep log file in root directory
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def extract_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from price text"""
        if not price_text:
            return None
        
        # Remove $ and commas, extract numbers
        price_match = re.search(r'\$?([\d,]+)', price_text.replace(',', ''))
        if price_match:
            return int(price_match.group(1).replace(',', ''))
        return None

    def extract_bed_bath_improved(self, text: str) -> Dict[str, Optional[float]]:
        """Improved extraction of bedroom and bathroom counts from various formats"""
        result = {'bedrooms': None, 'bathrooms': None}
        
        if not text:
            return result
        
        # Convert to lowercase for easier matching
        text_lower = text.lower()
        
        # Check for studio first
        if 'studio' in text_lower:
            result['bedrooms'] = 0
        
        # Enhanced patterns for bedrooms
        bedroom_patterns = [
            r'(\d+)\s*bd(?:room)?s?',           # "2 bd", "2 bdrm", "2 bedrooms"
            r'(\d+)\s*bedroom',                  # "2 bedroom"
            r'(\d+)\s*br',                       # "2 br"
            r'bed.*?(\d+)',                      # "bed 2" or similar
        ]
        
        for pattern in bedroom_patterns:
            bed_match = re.search(pattern, text_lower)
            if bed_match and result['bedrooms'] is None:
                result['bedrooms'] = int(bed_match.group(1))
                break
        
        # Enhanced patterns for bathrooms
        bathroom_patterns = [
            r'(\d+(?:\.\d+)?)\s*ba(?:th)?(?:room)?s?',  # "1.5 ba", "2 bath", "2 bathrooms"
            r'(\d+(?:\.\d+)?)\s*bathroom',               # "1.5 bathroom"
            r'bath.*?(\d+(?:\.\d+)?)',                   # "bath 1.5" or similar
        ]
        
        for pattern in bathroom_patterns:
            bath_match = re.search(pattern, text_lower)
            if bath_match:
                result['bathrooms'] = float(bath_match.group(1))
                break
        
        return result

    def extract_availability_date(self, text: str) -> str:
        """Extract availability date with improved patterns"""
        if not text:
            return ""
        
        # Convert to string if it's not already
        text = str(text)
        
        # Patterns for availability date
        availability_patterns = [
            r'available[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',          # "Available: 10/1/25", "Available 10/1/2025"
            r'available[:\s]*([a-zA-Z]+\s+\d{1,2},?\s+\d{2,4})',        # "Available: October 1, 2025"
            r'available[:\s]*([a-zA-Z]+\s+\d{1,2}(?:st|nd|rd|th)?)',    # "Available: October 1st"
            r'available[:\s]*(\d{1,2}[/-]\d{1,2})',                     # "Available: 10/1"
            r'available[:\s]*([a-zA-Z]+)',                               # "Available: October"
            r'available[:\s]*([^<\n\r]+?)(?:\s*\||$)',                  # General available text
        ]
        
        for pattern in availability_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1).strip()
                # Clean up common suffixes
                date_str = re.sub(r'\s*\|.*$', '', date_str)
                date_str = re.sub(r'\s*<.*$', '', date_str)
                return date_str
        
        return ""

    def extract_square_feet(self, sqft_text: str) -> Optional[int]:
        """Extract square footage from text"""
        if not sqft_text:
            return None
            
        sqft_match = re.search(r'([\d,]+)', sqft_text.replace(',', ''))
        if sqft_match:
            return int(sqft_match.group(1).replace(',', ''))
        return None

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        return text.strip()

    def debug_html_structure(self, soup):
        """Debug function to understand the HTML structure"""
        self.logger.info("Analyzing HTML structure...")
        
        # Look for common listing indicators
        indicators = [
            'rent', 'RENT', 'bedroom', 'bathroom', 'apartment', 'listing',
            'price', 'available', 'bd', 'ba', 'sqft', 'square'
        ]
        
        for indicator in indicators:
            elements = soup.find_all(string=re.compile(indicator, re.IGNORECASE))
            if elements:
                self.logger.info(f"Found {len(elements)} elements containing '{indicator}'")

    def find_listing_urls(self, soup) -> List[str]:
        """Find individual listing detail URLs"""
        urls = []
        
        # Look for links to listing detail pages
        links = soup.find_all('a', href=re.compile(r'/listings/detail/'))
        for link in links:
            full_url = urljoin(self.base_url, link['href'])
            if full_url not in urls:
                urls.append(full_url)
        
        self.logger.info(f"Found {len(urls)} listing detail URLs")
        return urls

    def scrape_individual_listing(self, listing_url: str) -> Dict:
        """Scrape an individual listing page with improved data extraction"""
        try:
            # Add small random delay to avoid overwhelming the server
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.get(listing_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            listing_data = {
                'title': '',
                'rent_price': None,
                'bedrooms': None,
                'bathrooms': None,
                'square_feet': None,
                'address': '',
                'availability_date': '',
                'description': '',
                'utilities_included': '',
                'appliances': '',
                'amenities': '',
                'listing_url': listing_url,
                'pet_policy': '',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Get all text content for pattern matching
            page_text = soup.get_text()
            
            # Also get HTML for better structured extraction
            page_html = str(soup)
            
            # Extract title from page title or main heading
            title_tag = soup.find('title')
            if title_tag:
                title_text = self.clean_text(title_tag.get_text())
                # Remove common suffixes
                title_text = re.sub(r'\s*-\s*Tulire Realty.*$', '', title_text, flags=re.IGNORECASE)
                listing_data['title'] = title_text
            
            # Look for main heading if title is generic
            h1_tag = soup.find('h1')
            if h1_tag and (not listing_data['title'] or len(listing_data['title']) < 20):
                listing_data['title'] = self.clean_text(h1_tag.get_text())
            
            # IMPROVED PRICE EXTRACTION
            price_patterns = [
                r'\$[\d,]+(?:\.\d{2})?(?:\s*/month|\s*/mo|/month|/mo)?',  # Standard price format with month
                r'rent[:\s]*\$[\d,]+',                                    # "Rent: $1950"
                r'price[:\s]*\$[\d,]+',                                   # "Price: $1950"
                r'monthly[:\s]*\$[\d,]+',                                 # "Monthly: $1950"
            ]
            
            for pattern in price_patterns:
                price_match = re.search(pattern, page_text, re.IGNORECASE)
                if price_match:
                    listing_data['rent_price'] = self.extract_price(price_match.group())
                    break
            
            # IMPROVED BEDROOM/BATHROOM EXTRACTION
            # Extract from the full page text using improved function
            bed_bath_data = self.extract_bed_bath_improved(page_text)
            listing_data.update(bed_bath_data)
            
            # Also try to extract from common HTML patterns
            # Look for specific elements that might contain bed/bath info
            detail_elements = soup.find_all(['div', 'span', 'p'], class_=re.compile(r'detail|info|feature', re.I))
            for element in detail_elements:
                element_text = element.get_text()
                bed_bath_data_element = self.extract_bed_bath_improved(element_text)
                
                # Update if we found better data
                if bed_bath_data_element['bedrooms'] is not None and listing_data['bedrooms'] is None:
                    listing_data['bedrooms'] = bed_bath_data_element['bedrooms']
                if bed_bath_data_element['bathrooms'] is not None and listing_data['bathrooms'] is None:
                    listing_data['bathrooms'] = bed_bath_data_element['bathrooms']
            
            # IMPROVED AVAILABILITY DATE EXTRACTION
            listing_data['availability_date'] = self.extract_availability_date(page_text)
            
            # Also check for availability in HTML attributes or data attributes
            avail_elements = soup.find_all(attrs={'data-available': True})
            if avail_elements and not listing_data['availability_date']:
                listing_data['availability_date'] = avail_elements[0].get('data-available', '')
            
            # Look for address - improved pattern
            address_patterns = [
                r'\d+[^,\n]*(?:street|avenue|boulevard|road|place|lane|drive|way|court|terrace|blvd|ave|st|rd|dr|ln|ct|pl)[^,\n]*,\s*[^,\n]*,\s*[A-Z]{2}\s*\d{5}',
                r'address[:\s]*(.+?)(?:\n|<|$)',
                r'location[:\s]*(.+?)(?:\n|<|$)',
            ]
            
            for pattern in address_patterns:
                address_match = re.search(pattern, page_text, re.IGNORECASE)
                if address_match:
                    if pattern.startswith(r'\d+'):  # Full address pattern
                        address = address_match.group()
                    else:  # Label: address pattern
                        address = address_match.group(1)
                    listing_data['address'] = self.clean_text(address)
                    break
            
            # Look for square footage with improved patterns
            sqft_patterns = [
                r'(\d+[\d,]*)\s*(?:sq\.?\s*ft\.?|square\s*feet|sqft)',
                r'square\s*feet?[:\s]*(\d+[\d,]*)',
                r'size[:\s]*(\d+[\d,]*)\s*sq',
            ]
            
            for pattern in sqft_patterns:
                sqft_match = re.search(pattern, page_text, re.IGNORECASE)
                if sqft_match:
                    sqft_str = sqft_match.group(1).replace(',', '')
                    try:
                        listing_data['square_feet'] = int(sqft_str)
                        break
                    except ValueError:
                        continue
            
            # Look for description in meta tags or main content
            desc_tag = soup.find('meta', {'name': 'description'})
            if desc_tag:
                listing_data['description'] = self.clean_text(desc_tag.get('content', ''))
            
            # Look for amenities/features
            amenities_keywords = ['amenities', 'features', 'includes']
            for keyword in amenities_keywords:
                amenities_match = re.search(f'{keyword}[:\s]*(.+?)(?:\n\n|\n[A-Z]|$)', page_text, re.IGNORECASE | re.DOTALL)
                if amenities_match:
                    listing_data['amenities'] = self.clean_text(amenities_match.group(1))
                    break
            
            # Log extracted data for debugging
            self.logger.debug(f"Extracted data for {listing_url}: "
                            f"Price: {listing_data['rent_price']}, "
                            f"Beds: {listing_data['bedrooms']}, "
                            f"Baths: {listing_data['bathrooms']}, "
                            f"Available: {listing_data['availability_date']}")
            
            return listing_data
            
        except requests.RequestException as e:
            self.logger.error(f"HTTP error scraping {listing_url}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error scraping individual listing {listing_url}: {e}")
            return {}

    def scrape_individual_listing_threaded(self, listing_url: str) -> Dict:
        """Wrapper for threaded scraping"""
        return self.scrape_individual_listing(listing_url)

    def scrape_listings(self) -> List[Dict]:
        """Main method to scrape all listings"""
        self.logger.info("Starting to scrape listings...")
        
        try:
            response = self.session.get(self.listings_url, timeout=30)
            response.raise_for_status()
            
            self.logger.info(f"Successfully fetched page, status code: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug HTML structure
            self.debug_html_structure(soup)
            
            # Find all listing URLs
            listing_urls = self.find_listing_urls(soup)
            
            if not listing_urls:
                self.logger.warning("No listing URLs found!")
                return []
            
            # Apply max_listings limit if specified
            if self.max_listings:
                listing_urls = listing_urls[:self.max_listings]
                self.logger.info(f"Limited to first {len(listing_urls)} listings")
            
            listings = []
            
            if self.use_threading and len(listing_urls) > 10:
                # Use threading for faster scraping of multiple pages
                self.logger.info(f"Using threaded scraping with {self.max_workers} workers for {len(listing_urls)} listings")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tasks
                    future_to_url = {
                        executor.submit(self.scrape_individual_listing_threaded, url): url 
                        for url in listing_urls
                    }
                    
                    # Process completed tasks
                    for i, future in enumerate(as_completed(future_to_url), 1):
                        url = future_to_url[future]
                        try:
                            listing_data = future.result()
                            if listing_data:
                                listings.append(listing_data)
                                self.logger.info(f"Scraped listing {i}/{len(listing_urls)}: {listing_data.get('title', 'No title')[:50]}...")
                            else:
                                self.logger.warning(f"Failed to scrape {url}")
                        except Exception as e:
                            self.logger.error(f"Error processing {url}: {e}")
                        
                        # Progress update every 10 listings
                        if i % 10 == 0:
                            self.logger.info(f"Progress: {i}/{len(listing_urls)} listings processed")
            
            else:
                # Sequential scraping
                self.logger.info(f"Using sequential scraping for {len(listing_urls)} listings")
                for i, url in enumerate(listing_urls, 1):
                    try:
                        listing_data = self.scrape_individual_listing(url)
                        if listing_data:
                            listings.append(listing_data)
                            self.logger.info(f"Scraped listing {i}/{len(listing_urls)}: {listing_data.get('title', 'No title')[:50]}...")
                        else:
                            self.logger.warning(f"Failed to scrape {url}")
                        
                        # Progress update every 10 listings
                        if i % 10 == 0:
                            self.logger.info(f"Progress: {i}/{len(listing_urls)} listings processed")
                            
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
            
            self.logger.info(f"Successfully scraped {len(listings)} out of {len(listing_urls)} listings")
            return listings
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching main page: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return []

    def save_to_json(self, listings: List[Dict], filename: str = None) -> str:
        """Save listings to JSON file in scraped_data folder"""
        if filename is None:
            filename = "tulire_listings.json"  # Fixed filename
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(listings, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(listings)} listings to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
            return ""

    def save_to_csv(self, listings: List[Dict], filename: str = None) -> str:
        """Save listings to CSV file in scraped_data folder"""
        if not listings:
            self.logger.warning("No listings to save")
            return ""
        
        if filename is None:
            filename = "tulire_listings.csv"  # Fixed filename
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            fieldnames = listings[0].keys()
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(listings)
            
            self.logger.info(f"Saved {len(listings)} listings to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return ""

    def get_stats(self, listings: List[Dict]) -> Dict:
        """Get statistics about scraped listings"""
        if not listings:
            return {}
        
        stats = {
            'total_listings': len(listings),
            'with_price': len([l for l in listings if l.get('rent_price')]),
            'with_bedrooms': len([l for l in listings if l.get('bedrooms') is not None]),
            'with_bathrooms': len([l for l in listings if l.get('bathrooms')]),
            'with_address': len([l for l in listings if l.get('address')]),
            'with_square_feet': len([l for l in listings if l.get('square_feet')]),
            'with_availability': len([l for l in listings if l.get('availability_date')]),
        }
        
        if stats['with_price'] > 0:
            prices = [l['rent_price'] for l in listings if l.get('rent_price')]
            stats['price_range'] = f"${min(prices):,} - ${max(prices):,}"
            stats['avg_price'] = f"${sum(prices) // len(prices):,}"
        
        return stats

def main():
    """Main execution function"""
    print("🏠 Starting Enhanced Tulire Listings Scraper...")
    print("=" * 60)
    
    # Configuration options
    MAX_LISTINGS = None  # Set to None to scrape all, or a number like 20 for testing
    USE_THREADING = True  # Set to False for sequential scraping
    MAX_WORKERS = 5      # Number of concurrent threads
    
    scraper = TulireListingsScraper(
        max_listings=MAX_LISTINGS, 
        use_threading=USE_THREADING,
        max_workers=MAX_WORKERS
    )
    
    # Scrape listings
    listings = scraper.scrape_listings()
    
    if not listings:
        print("❌ No listings found. Check scraper.log for details")
        return
    
    # Display statistics
    stats = scraper.get_stats(listings)
    print(f"\n✅ Successfully scraped {len(listings)} listings")
    print("\n📊 Scraping Statistics:")
    print("-" * 40)
    for key, value in stats.items():
        if key != 'total_listings':
            print(f"{key.replace('_', ' ').title()}: {value}")
    
    # Display sample listing
    print(f"\n📋 Sample listing data:")
    print("-" * 40)
    if listings:
        sample = listings[0]
        for key, value in sample.items():
            if value and key != 'scraped_at':
                display_value = str(value)[:80] + "..." if len(str(value)) > 80 else str(value)
                print(f"{key}: {display_value}")
        print("-" * 40)
    
    # Save data
    json_file = scraper.save_to_json(listings)
    csv_file = scraper.save_to_csv(listings)
    
    print(f"\n💾 Data saved to scraped_data folder:")
    if json_file:
        print(f"   📄 JSON: {os.path.basename(json_file)}")
    if csv_file:
        print(f"   📊 CSV: {os.path.basename(csv_file)}")
    
    print(f"\n✨ Scraping completed!")
    print(f"📈 Success rate: {len(listings)}/{stats.get('total_listings', 'unknown')} listings")
    print(f"📁 All files are saved in the 'scraped_data' folder")

if __name__ == "__main__":
    main()