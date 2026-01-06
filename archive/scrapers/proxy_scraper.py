#!/usr/bin/env python3
"""
Proxy-Enabled Content Scraper
=============================
Scrapes content through a SOCKS5 or HTTP proxy.
Designed to work with ExpressVPN via SSH tunnel.
"""

import pandas as pd
import requests
import trafilatura
import time
import logging
import sys
import argparse
import warnings
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

warnings.filterwarnings("ignore")


class ProxyContentScraper:
    def __init__(
        self, 
        urls_file: str, 
        output_dir: str, 
        proxy: str = None,
        max_workers: int = 20, 
        timeout: int = 20
    ):
        self.urls_file = Path(urls_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.proxy = proxy
        self.max_workers = max_workers
        self.timeout = timeout
        self.session_local = threading.local()
        
        # Build proxy dict
        self.proxy_dict = None
        if proxy:
            self.proxy_dict = {
                "http": proxy,
                "https": proxy,
            }
        
        # Stats
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'start_time': None
        }
        self.stats_lock = threading.Lock()
        
        # Setup logging
        log_file = self.output_dir / f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_session(self):
        """Get thread-local session with proxy configured."""
        if not hasattr(self.session_local, 'session'):
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'da,en-US;q=0.7,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20, 
                pool_maxsize=100,
                max_retries=2
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.session_local.session = session
        return self.session_local.session
    
    def test_proxy(self):
        """Test if proxy is working."""
        if not self.proxy_dict:
            self.logger.info("📡 No proxy configured - using direct connection")
            return True
            
        try:
            response = requests.get(
                "http://ipinfo.io/ip", 
                proxies=self.proxy_dict, 
                timeout=10
            )
            if response.status_code == 200:
                ip = response.text.strip()
                self.logger.info(f"✅ Proxy working! IP: {ip}")
                return True
        except Exception as e:
            self.logger.error(f"❌ Proxy test failed: {e}")
            return False
        return False
    
    def scrape_url(self, url: str) -> dict:
        """Scrape a single URL through proxy."""
        start = time.time()
        try:
            session = self.get_session()
            response = session.get(
                url, 
                timeout=self.timeout, 
                proxies=self.proxy_dict,
                allow_redirects=True, 
                verify=False
            )
            
            if response.status_code == 200:
                content = trafilatura.extract(
                    response.text,
                    include_comments=False,
                    include_tables=True,
                    favor_precision=True
                )
                metadata = trafilatura.extract_metadata(response.text)
                
                return {
                    'url': url,
                    'status_code': response.status_code,
                    'content': content or '',
                    'title': metadata.title if metadata else '',
                    'author': metadata.author if metadata else '',
                    'date': metadata.date if metadata else '',
                    'word_count': len((content or '').split()),
                    'response_time': time.time() - start,
                    'scraped_at': datetime.now().isoformat(),
                    'error': ''
                }
            else:
                return {
                    'url': url,
                    'status_code': response.status_code,
                    'content': '',
                    'title': '',
                    'author': '',
                    'date': '',
                    'word_count': 0,
                    'response_time': time.time() - start,
                    'scraped_at': datetime.now().isoformat(),
                    'error': f'HTTP {response.status_code}'
                }
        except Exception as e:
            return {
                'url': url,
                'status_code': 0,
                'content': '',
                'title': '',
                'author': '',
                'date': '',
                'word_count': 0,
                'response_time': time.time() - start,
                'scraped_at': datetime.now().isoformat(),
                'error': str(e)[:200]
            }
    
    def run(self):
        """Run the scraper."""
        self.logger.info(f"🚀 Starting Proxy Content Scraper")
        self.logger.info(f"📁 Input: {self.urls_file}")
        self.logger.info(f"📤 Output: {self.output_dir}")
        self.logger.info(f"🔐 Proxy: {self.proxy or 'None (direct)'}")
        self.logger.info(f"⚡ Workers: {self.max_workers}")
        
        # Test proxy first
        if self.proxy_dict and not self.test_proxy():
            self.logger.error("❌ Proxy not available! Check SSH tunnel.")
            self.logger.info("💡 Make sure to run on your Mac:")
            self.logger.info("   1. python3 scripts/local_socks_proxy.py")
            self.logger.info("   2. ssh -R 1080:localhost:1080 ucloud")
            return
        
        # Load URLs
        df = pd.read_csv(self.urls_file, on_bad_lines='skip')
        if 'url' in df.columns:
            urls = df['url'].dropna().unique().tolist()
        elif 'resolved_url' in df.columns:
            urls = df['resolved_url'].dropna().unique().tolist()
        else:
            urls = df.iloc[:, 0].dropna().unique().tolist()
            
        self.stats['total'] = len(urls)
        self.stats['start_time'] = time.time()
        
        self.logger.info(f"📊 Loaded {len(urls):,} unique URLs")
        
        # Process in batches
        batch_size = 500
        results = []
        batch_num = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.scrape_url, url): url for url in urls}
            
            for future in as_completed(future_to_url):
                result = future.result()
                results.append(result)
                
                with self.stats_lock:
                    if result['content']:
                        self.stats['success'] += 1
                    else:
                        self.stats['failed'] += 1
                    
                    processed = self.stats['success'] + self.stats['failed']
                    
                    # Log progress every 100 URLs
                    if processed % 100 == 0:
                        elapsed = time.time() - self.stats['start_time']
                        rate = processed / elapsed if elapsed > 0 else 0
                        eta = (self.stats['total'] - processed) / rate if rate > 0 else 0
                        
                        self.logger.info(
                            f"📈 Progress: {processed:,}/{self.stats['total']:,} ({100*processed/self.stats['total']:.1f}%) | "
                            f"Success: {self.stats['success']:,} | Failed: {self.stats['failed']:,} | "
                            f"Rate: {rate:.1f}/sec | ETA: {eta/60:.0f}min"
                        )
                    
                    # Save batch
                    if processed % batch_size == 0:
                        batch_num += 1
                        batch_df = pd.DataFrame(results[-batch_size:])
                        batch_file = self.output_dir / f'scraped_batch_{batch_num:04d}.csv'
                        batch_df.to_csv(batch_file, index=False)
                        self.logger.info(f"💾 Saved batch {batch_num}")
        
        # Save remaining results
        if results:
            final_df = pd.DataFrame(results)
            final_file = self.output_dir / 'scraped_all.csv'
            final_df.to_csv(final_file, index=False)
            self.logger.info(f"✅ All results saved to {final_file}")
        
        elapsed = time.time() - self.stats['start_time']
        self.logger.info(f"")
        self.logger.info(f"🎉 Scraping complete!")
        self.logger.info(f"📊 Total: {self.stats['total']:,} | Success: {self.stats['success']:,} | Failed: {self.stats['failed']:,}")
        self.logger.info(f"⏱️ Time: {elapsed/60:.1f} minutes | Rate: {self.stats['total']/elapsed:.1f} URLs/sec")


def main():
    parser = argparse.ArgumentParser(description='Proxy-Enabled Content Scraper')
    parser.add_argument('--urls-file', required=True, help='CSV file with URLs')
    parser.add_argument('--output-dir', required=True, help='Output directory')
    parser.add_argument('--proxy', default=None, help='Proxy URL (e.g., socks5://localhost:1080)')
    parser.add_argument('--workers', type=int, default=20, help='Parallel workers')
    parser.add_argument('--timeout', type=int, default=20, help='Request timeout')
    
    args = parser.parse_args()
    
    scraper = ProxyContentScraper(
        urls_file=args.urls_file,
        output_dir=args.output_dir,
        proxy=args.proxy,
        max_workers=args.workers,
        timeout=args.timeout
    )
    scraper.run()


if __name__ == '__main__':
    main()

