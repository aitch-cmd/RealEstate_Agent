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

    def extract_bed_bath(self, bed_bath_text: str) -> Dict[str, Optional[int]]:
        """Extract bedroom and bathroom counts"""
        result = {'bedrooms': None, 'bathrooms': None}
        
        if not bed_bath_text:
            return result
        
        # Pattern for "X bd / Y ba" or "Studio / 1 ba"
        bed_match = re.search(r'(\d+)\s*bd', bed_bath_text)
        bath_match = re.search(r'(\d+(?:\.\d+)?)\s*ba', bed_bath_text)
        
        if 'studio' in bed_bath_text.lower():
            result['bedrooms'] = 0
        elif bed_match:
            result['bedrooms'] = int(bed_match.group(1))
            
        if bath_match:
            result['bathrooms'] = float(bath_match.group(1))
            
        return result

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
        """Scrape an individual listing page with better error handling"""
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
            
            # Extract data from individual page
            page_text = soup.get_text()
            
            # Look for price - multiple patterns
            price_patterns = [
                r'\$[\d,]+(?:\.\d{2})?',  # Standard price format
                r'Rent:\s*\$[\d,]+',      # "Rent: $1950"
                r'Price:\s*\$[\d,]+',     # "Price: $1950"
            ]
            
            for pattern in price_patterns:
                price_match = re.search(pattern, page_text)
                if price_match:
                    listing_data['rent_price'] = self.extract_price(price_match.group())
                    break
            
            # Look for bed/bath - improved patterns
            bed_bath_patterns = [
                r'(\d+)\s*(?:bed|bd|bedroom).*?(\d+(?:\.\d+)?)\s*(?:bath|ba|bathroom)',
                r'(\d+)\s*bd\s*/\s*(\d+(?:\.\d+)?)\s*ba',
                r'Bedrooms?:\s*(\d+).*?Bathrooms?:\s*(\d+(?:\.\d+)?)',
            ]
            
            for pattern in bed_bath_patterns:
                bed_bath_match = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
                if bed_bath_match:
                    listing_data['bedrooms'] = int(bed_bath_match.group(1))
                    listing_data['bathrooms'] = float(bed_bath_match.group(2))
                    break
            
            # Check for studio
            if 'studio' in page_text.lower() and not listing_data['bedrooms']:
                listing_data['bedrooms'] = 0
            
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
            
            # Look for address - improved pattern
            address_patterns = [
                r'\d+[^,\n]*(?:street|avenue|boulevard|road|place|lane|drive|way|court|terrace|blvd|ave|st|rd|dr|ln|ct|pl)[^,\n]*,\s*[^,\n]*,\s*[A-Z]{2}\s*\d{5}',
                r'Address:\s*(.+?)(?:\n|$)',
                r'Location:\s*(.+?)(?:\n|$)',
            ]
            
            for pattern in address_patterns:
                address_match = re.search(pattern, page_text, re.IGNORECASE)
                if address_match:
                    address = address_match.group(1) if pattern.startswith(r'Address') or pattern.startswith(r'Location') else address_match.group()
                    listing_data['address'] = self.clean_text(address)
                    break
            
            # Look for square footage
            sqft_patterns = [
                r'(\d+)\s*(?:sq\.?\s*ft\.?|square\s*feet)',
                r'Square\s*Feet:\s*(\d+)',
                r'Size:\s*(\d+)\s*sq',
            ]
            
            for pattern in sqft_patterns:
                sqft_match = re.search(pattern, page_text, re.IGNORECASE)
                if sqft_match:
                    listing_data['square_feet'] = int(sqft_match.group(1).replace(',', ''))
                    break
            
            # Look for availability date
            avail_patterns = [
                r'Available:\s*(.+?)(?:\n|$)',
                r'Availability:\s*(.+?)(?:\n|$)',
                r'Move.in.*?(\d{1,2}/\d{1,2}/\d{4})',
            ]
            
            for pattern in avail_patterns:
                avail_match = re.search(pattern, page_text, re.IGNORECASE)
                if avail_match:
                    listing_data['availability_date'] = self.clean_text(avail_match.group(1))
                    break
            
            # Look for description in meta tags or main content
            desc_tag = soup.find('meta', {'name': 'description'})
            if desc_tag:
                listing_data['description'] = self.clean_text(desc_tag.get('content', ''))
            
            # Look for amenities/features
            amenities_keywords = ['amenities', 'features', 'includes']
            for keyword in amenities_keywords:
                amenities_match = re.search(f'{keyword}:(.+?)(?:\n\n|\n[A-Z])', page_text, re.IGNORECASE | re.DOTALL)
                if amenities_match:
                    listing_data['amenities'] = self.clean_text(amenities_match.group(1))
                    break
            
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