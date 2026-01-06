#!/usr/bin/env python3
"""
Simple Content Scraper
======================
Direct scraper for resolved URLs without proxy.
"""

import pandas as pd
import requests
import trafilatura
import time
import json
import logging
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class SimpleContentScraper:
    def __init__(self, urls_file: str, output_dir: str, max_workers: int = 40, timeout: int = 15):
        self.urls_file = Path(urls_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.timeout = timeout
        self.session_local = threading.local()
        
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
        if not hasattr(self.session_local, 'session'):
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'da,en-US;q=0.7,en;q=0.3',
            })
            adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=100)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.session_local.session = session
        return self.session_local.session
    
    def scrape_url(self, url: str) -> dict:
        start = time.time()
        try:
            session = self.get_session()
            response = session.get(url, timeout=self.timeout, allow_redirects=True, verify=False)
            
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
        self.logger.info(f"🚀 Starting Simple Content Scraper")
        self.logger.info(f"📁 Input: {self.urls_file}")
        self.logger.info(f"📤 Output: {self.output_dir}")
        self.logger.info(f"⚡ Workers: {self.max_workers}")
        
        # Load URLs
        df = pd.read_csv(self.urls_file)
        urls = df['url'].dropna().unique().tolist()
        self.stats['total'] = len(urls)
        self.stats['start_time'] = time.time()
        
        self.logger.info(f"📊 Loaded {len(urls)} unique URLs")
        
        # Process in batches
        batch_size = 1000
        results = []
        
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
                    
                    # Save batch
                    if processed % batch_size == 0:
                        batch_num = processed // batch_size
                        batch_df = pd.DataFrame(results[-batch_size:])
                        batch_file = self.output_dir / f'scraped_batch_{batch_num:04d}.csv'
                        batch_df.to_csv(batch_file, index=False)
                        
                        elapsed = time.time() - self.stats['start_time']
                        rate = processed / elapsed if elapsed > 0 else 0
                        
                        self.logger.info(
                            f"📦 Batch {batch_num} saved | "
                            f"Progress: {processed}/{self.stats['total']} ({100*processed/self.stats['total']:.1f}%) | "
                            f"Success: {self.stats['success']} | "
                            f"Failed: {self.stats['failed']} | "
                            f"Rate: {rate:.1f} URLs/sec"
                        )
        
        # Save remaining
        if results:
            final_df = pd.DataFrame(results)
            final_file = self.output_dir / 'scraped_final.csv'
            final_df.to_csv(final_file, index=False)
            self.logger.info(f"✅ Final results saved to {final_file}")
        
        elapsed = time.time() - self.stats['start_time']
        self.logger.info(f"🎉 Scraping complete!")
        self.logger.info(f"📊 Total: {self.stats['total']} | Success: {self.stats['success']} | Failed: {self.stats['failed']}")
        self.logger.info(f"⏱️ Time: {elapsed/60:.1f} minutes | Rate: {self.stats['total']/elapsed:.1f} URLs/sec")


def main():
    parser = argparse.ArgumentParser(description='Simple Content Scraper')
    parser.add_argument('--urls-file', required=True, help='CSV file with URLs')
    parser.add_argument('--output-dir', required=True, help='Output directory')
    parser.add_argument('--workers', type=int, default=40, help='Parallel workers')
    parser.add_argument('--timeout', type=int, default=15, help='Request timeout')
    
    args = parser.parse_args()
    
    scraper = SimpleContentScraper(
        urls_file=args.urls_file,
        output_dir=args.output_dir,
        max_workers=args.workers,
        timeout=args.timeout
    )
    scraper.run()


if __name__ == '__main__':
    main()

