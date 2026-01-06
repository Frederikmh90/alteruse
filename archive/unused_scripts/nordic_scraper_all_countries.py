#!/usr/bin/env python3
"""
Nordic Scraper - Run all countries simultaneously with blocking detection
"""
import os
import sys
import time
import json
import logging
import requests
import trafilatura
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urlparse
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import argparse
from multiprocessing import Process

# Disable SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/nordic_scraper_all.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Known blocked domains to skip
BLOCKED_DOMAINS = {
    'solidaritet.dk': 'Returns HTTP 455 - Content blocking'
}

class NordicScraperMultiCountry:
    """Multi-country Nordic scraper with blocking detection."""

    def __init__(self, country: str, workers: int = 4):
        self.country = country.lower()
        self.workers = workers
        self.base_dir = Path("/work/FrederikMøllerHenriksen#7467/projects/ANM_observatory_datacollection")
        self.country_urls_dir = self.base_dir / "data" / f"{self.country}_urls"
        self.country_results_dir = self.base_dir / "data" / f"{self.country}_results"
        self.consolidated_dataset = self.base_dir / "data" / "nordic_countries" / "full_all_countries.csv"
        
        # Create results directory
        self.country_results_dir.mkdir(parents=True, exist_ok=True)
        
        # Thread-safe stats
        self.stats_lock = Lock()
        self.stats = {
            "urls_total": 0,
            "urls_successful": 0,
            "urls_failed": 0,
            "urls_blocked": 0,
            "domains_blocked": set(),
            "start_time": time.time()
        }

    def is_domain_blocked(self, url: str) -> bool:
        """Check if domain is known to be blocked."""
        try:
            domain = urlparse(url).netloc
            return domain in BLOCKED_DOMAINS
        except:
            return False

    def test_connection(self) -> bool:
        """Test basic internet connectivity."""
        try:
            response = requests.get('http://ipinfo.io/ip', timeout=10)
            ip = response.text.strip()
            logger.info(f"🌐 [{self.country.upper()}] Current IP: {ip}")
            return True
        except Exception as e:
            logger.error(f"❌ [{self.country.upper()}] Connection test failed: {e}")
            return False

    def load_urls_from_csv_files(self) -> List[str]:
        """Load URLs from all CSV files for this country."""
        logger.info(f"📂 [{self.country.upper()}] Loading URLs from {self.country}_urls directory")
        
        if not self.country_urls_dir.exists():
            logger.error(f"❌ [{self.country.upper()}] URL directory not found: {self.country_urls_dir}")
            return []
        
        all_urls = []
        csv_files = list(self.country_urls_dir.glob("*_new_urls_*.csv"))
        
        if not csv_files:
            logger.error(f"❌ [{self.country.upper()}] No URL CSV files found in {self.country_urls_dir}")
            return []
        
        logger.info(f"📄 [{self.country.upper()}] Found {len(csv_files)} URL files to process")
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if 'url' in df.columns:
                    file_urls = df['url'].dropna().tolist()
                    all_urls.extend(file_urls)
                    logger.info(f"✅ [{self.country.upper()}] Loaded {len(file_urls)} URLs from {csv_file.name}")
                else:
                    logger.warning(f"⚠️ [{self.country.upper()}] No 'url' column in {csv_file.name}")
                    
            except Exception as e:
                logger.error(f"❌ [{self.country.upper()}] Error reading {csv_file.name}: {e}")
        
        # Remove duplicates and blocked domains
        unique_urls = []
        blocked_count = 0
        
        for url in dict.fromkeys(all_urls):  # Remove duplicates while preserving order
            if self.is_domain_blocked(url):
                blocked_count += 1
                with self.stats_lock:
                    self.stats["domains_blocked"].add(urlparse(url).netloc)
            else:
                unique_urls.append(url)
        
        logger.info(f"📊 [{self.country.upper()}] Total URLs: {len(all_urls)}, Unique: {len(unique_urls)}, Blocked domains skipped: {blocked_count}")
        
        with self.stats_lock:
            self.stats["urls_total"] = len(unique_urls)
        
        return unique_urls

    def scrape_url_content(self, url: str) -> Optional[Dict]:
        """Scrape content from a single URL with improved error handling."""
        start_time = time.time()
        
        try:
            # Make request with better headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
            
            response = requests.get(
                url, 
                headers=headers, 
                timeout=30, 
                verify=False,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                # Extract content using trafilatura
                content = trafilatura.extract(
                    response.text,
                    no_fallback=False,
                    include_comments=False,
                    include_tables=True,
                    include_images=False
                )
                
                if content and len(content.strip()) > 100:
                    # Extract metadata
                    metadata = trafilatura.extract_metadata(response.text)
                    
                    article_data = {
                        'url': url,
                        'title': metadata.title if metadata else 'N/A',
                        'author': metadata.author if metadata else 'N/A',
                        'date': metadata.date if metadata else 'N/A',
                        'content': content.strip(),
                        'content_length': len(content.strip()),
                        'scraped_at': datetime.now().isoformat(),
                        'country': self.country,
                        'processing_time': round(time.time() - start_time, 2)
                    }
                    
                    with self.stats_lock:
                        self.stats["urls_successful"] += 1
                    
                    return article_data
                else:
                    logger.info(f"⚠️ [{self.country.upper()}] Content too short for {url}")
            elif response.status_code == 455:
                # New blocked domain detected
                domain = urlparse(url).netloc
                logger.warning(f"🚫 [{self.country.upper()}] New blocked domain detected: {domain} (HTTP 455)")
                with self.stats_lock:
                    self.stats["urls_blocked"] += 1
                    self.stats["domains_blocked"].add(domain)
            elif response.status_code == 404:
                logger.info(f"📭 [{self.country.upper()}] Not found (404) for {url}")
            else:
                logger.info(f"⚠️ [{self.country.upper()}] HTTP {response.status_code} for {url}")
                
        except requests.exceptions.Timeout:
            logger.info(f"⏰ [{self.country.upper()}] Timeout for {url}")
        except requests.exceptions.ConnectionError:
            logger.info(f"🔌 [{self.country.upper()}] Connection error for {url}")
        except Exception as e:
            logger.warning(f"❌ [{self.country.upper()}] Error scraping {url}: {e}")
            
        with self.stats_lock:
            self.stats["urls_failed"] += 1
            
        return None

    def scrape_urls_parallel(self, urls: List[str]) -> List[Dict]:
        """Scrape URLs in parallel with progress tracking."""
        logger.info(f"📄 [{self.country.upper()}] Scraping {len(urls)} URLs with {self.workers} workers")
        
        successful_results = []
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            future_to_url = {
                executor.submit(self.scrape_url_content, url): url 
                for url in urls
            }
            
            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_url), 1):
                url = future_to_url[future]
                
                try:
                    result = future.result()
                    if result:
                        successful_results.append(result)
                    
                    # Progress update every 100 URLs
                    if i % 100 == 0:
                        with self.stats_lock:
                            success_rate = (self.stats["urls_successful"] / i) * 100
                            blocked_rate = (self.stats["urls_blocked"] / i) * 100
                        logger.info(f"📊 [{self.country.upper()}] Progress: {i}/{len(urls)} | Success: {self.stats['urls_successful']} ({success_rate:.1f}%) | Blocked: {self.stats['urls_blocked']} ({blocked_rate:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"❌ [{self.country.upper()}] Error processing {url}: {e}")
        
        logger.info(f"✅ [{self.country.upper()}] Scraping Complete: {len(successful_results)}/{len(urls)} articles scraped successfully")
        return successful_results

    def save_results(self, results: List[Dict]):
        """Save scraping results."""
        if not results:
            logger.warning(f"⚠️ [{self.country.upper()}] No results to save")
            return
        
        # Save country-specific results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        country_file = self.country_results_dir / f"{self.country}_scraped_articles_{timestamp}.csv"
        
        df_country = pd.DataFrame(results)
        df_country.to_csv(country_file, index=False)
        logger.info(f"💾 [{self.country.upper()}] Results saved: {country_file}")
        logger.info(f"📊 [{self.country.upper()}] Articles saved: {len(results):,}")

    def run_scraping_pipeline(self, max_urls: Optional[int] = None) -> bool:
        """Run the complete scraping pipeline."""
        logger.info(f"🚀 [{self.country.upper()}] Starting Nordic Scraper")
        logger.info("=" * 60)
        
        # Step 1: Test connection
        if not self.test_connection():
            logger.error(f"❌ [{self.country.upper()}] Connection test failed - cannot continue")
            return False
        
        # Step 2: Load URLs from CSV files
        urls = self.load_urls_from_csv_files()
        if not urls:
            logger.error(f"❌ [{self.country.upper()}] No URLs found to scrape")
            return False
        
        # Step 3: Limit URLs if specified (for testing)
        if max_urls and len(urls) > max_urls:
            urls = urls[:max_urls]
            logger.info(f"🔬 [{self.country.upper()}] Limited to {max_urls} URLs for testing")
        
        # Step 4: Scrape URLs
        results = self.scrape_urls_parallel(urls)
        
        # Step 5: Save results
        self.save_results(results)
        
        # Step 6: Final stats
        elapsed_time = time.time() - self.stats["start_time"]
        logger.info(f"🏁 [{self.country.upper()}] Pipeline completed in {elapsed_time:.1f} seconds")
        logger.info(f"📈 [{self.country.upper()}] Final stats: {self.stats['urls_successful']} successful, {self.stats['urls_failed']} failed, {self.stats['urls_blocked']} blocked")
        
        if self.stats["domains_blocked"]:
            logger.info(f"🚫 [{self.country.upper()}] Blocked domains: {', '.join(self.stats['domains_blocked'])}")
        
        return len(results) > 0

def run_country_scraper(country: str, workers: int = 4, max_urls: Optional[int] = None):
    """Run scraper for a single country (used for multiprocessing)."""
    scraper = NordicScraperMultiCountry(country, workers)
    return scraper.run_scraping_pipeline(max_urls)

def main():
    parser = argparse.ArgumentParser(description="Nordic Multi-Country Scraper")
    parser.add_argument("--countries", default="denmark,sweden,finland", help="Comma-separated list of countries")
    parser.add_argument("--workers", type=int, default=4, help="Number of workers per country")
    parser.add_argument("--max-urls", type=int, help="Maximum URLs per country (for testing)")
    
    args = parser.parse_args()
    
    countries = [c.strip().lower() for c in args.countries.split(",")]
    
    try:
        logger.info(f"🌍 Starting multi-country scraping for: {', '.join(countries)}")
        logger.info(f"⚙️ Configuration: {args.workers} workers per country")
        
        # Run all countries in parallel processes
        processes = []
        for country in countries:
            process = Process(
                target=run_country_scraper, 
                args=(country, args.workers, args.max_urls)
            )
            process.start()
            processes.append(process)
            logger.info(f"🚀 Started {country} scraper process")
        
        # Wait for all processes to complete
        for i, process in enumerate(processes):
            process.join()
            logger.info(f"✅ {countries[i]} scraper completed")
        
        logger.info("🏆 All country scrapers completed!")
        return 0

    except KeyboardInterrupt:
        logger.info("⏹️ Scraping interrupted by user")
        for process in processes:
            process.terminate()
        return 1
    except Exception as e:
        logger.error(f"❌ Scraping error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
