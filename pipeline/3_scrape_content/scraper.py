#!/usr/bin/env python3
"""
Content Scraper with Auto-Reconnect
====================================
- Automatically detects when VPN/proxy tunnel goes down
- Pauses and waits for reconnection
- Resumes automatically when tunnel is back
- Graceful Ctrl+C handling with progress save
"""

import pandas as pd
import requests
import time
import logging
import sys
import argparse
import warnings
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import threading
import json
import os
import signal

# Suppress ALL warnings from libraries
warnings.filterwarnings("ignore")
os.environ['TRAFILATURA_LOG_LEVEL'] = 'CRITICAL'

for logger_name in ['trafilatura', 'trafilatura.core', 'trafilatura.utils', 
                     'urllib3', 'urllib3.connectionpool', 'requests', 
                     'chardet', 'charset_normalizer']:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

import trafilatura


class ContentScraper:
    """Content scraper with auto-reconnect and graceful shutdown."""
    
    def __init__(
        self, 
        urls_file: str, 
        output_dir: str, 
        proxy: str = None,
        max_workers: int = 20, 
        timeout: int = 15
    ):
        self.urls_file = Path(urls_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.proxy = proxy
        self.max_workers = max_workers
        self.timeout = timeout
        self.session_local = threading.local()
        
        # Progress tracking
        self.progress_file = self.output_dir / "scraping_progress.json"
        self.processed_urls = self._load_progress()
        
        # Proxy config
        self.proxy_dict = {"http": proxy, "https": proxy} if proxy else None
        
        # Control flags
        self.shutdown_requested = False
        self.proxy_healthy = True
        self.consecutive_proxy_errors = 0
        self.proxy_error_threshold = 10  # Pause after 10 consecutive proxy errors
        
        # Stats
        self.stats = {
            'total': 0, 'success': 0, 'failed': 0,
            'skipped': len(self.processed_urls), 'start_time': None
        }
        self.stats_lock = threading.Lock()
        self.results = []
        self.results_lock = threading.Lock()
        
        # Setup logging
        log_file = self.output_dir / f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.logger = logging.getLogger('scraper')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully."""
        if not self.shutdown_requested:
            self.logger.info("\n⚠️  Shutdown requested - saving progress...")
            self.shutdown_requested = True
        else:
            self.logger.info("🛑 Force quit")
            sys.exit(1)
    
    def _load_progress(self) -> set:
        """Load previously processed URLs."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return set(json.load(f).get('processed_urls', []))
            except:
                return set()
        return set()
    
    def _save_progress(self):
        """Save current progress."""
        with open(self.progress_file, 'w') as f:
            json.dump({
                'processed_urls': list(self.processed_urls),
                'last_updated': datetime.now().isoformat(),
                'total_processed': len(self.processed_urls)
            }, f)
    
    def _save_batch(self, batch_num: int, batch_results: list):
        """Save a batch of results."""
        if batch_results:
            batch_df = pd.DataFrame(batch_results)
            batch_file = self.output_dir / f'scraped_batch_{batch_num:04d}.csv'
            batch_df.to_csv(batch_file, index=False)
    
    def get_session(self):
        """Get thread-local session."""
        if not hasattr(self.session_local, 'session'):
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'da,en-US;q=0.7,en;q=0.3',
            })
            adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=100, max_retries=1)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.session_local.session = session
        return self.session_local.session
    
    def test_proxy(self, quiet=False) -> bool:
        """Test if proxy is working."""
        if not self.proxy_dict:
            return True
        try:
            response = requests.get("http://ipinfo.io/ip", proxies=self.proxy_dict, timeout=10)
            if response.status_code == 200:
                if not quiet:
                    self.logger.info(f"✅ Proxy working! IP: {response.text.strip()}")
                return True
        except:
            pass
        return False
    
    def wait_for_proxy(self):
        """Wait for proxy to become available, checking every 30 seconds."""
        self.logger.info("⏸️  Proxy down - pausing and waiting for reconnection...")
        self.logger.info("💡 Reconnect your VPN tunnel when ready")
        
        check_interval = 30
        while not self.shutdown_requested:
            time.sleep(check_interval)
            if self.test_proxy(quiet=True):
                self.logger.info("🔄 Proxy is back! Resuming scraping...")
                self.consecutive_proxy_errors = 0
                self.proxy_healthy = True
                return True
            self.logger.info(f"⏳ Still waiting for proxy... (checking every {check_interval}s)")
        
        return False
    
    def is_proxy_error(self, error_str: str) -> bool:
        """Check if error indicates proxy/tunnel failure."""
        proxy_indicators = [
            'Connection refused', 'Connection reset', 'SOCKS',
            'ProxyError', 'tunnel', 'NewConnectionError'
        ]
        return any(ind.lower() in error_str.lower() for ind in proxy_indicators)
    
    def scrape_url(self, url: str) -> dict:
        """Scrape a single URL."""
        start = time.time()
        result = {
            'url': url, 'status_code': 0, 'content': '', 'title': '',
            'author': '', 'date': '', 'word_count': 0,
            'response_time': 0, 'scraped_at': datetime.now().isoformat(), 'error': ''
        }
        
        try:
            session = self.get_session()
            response = session.get(url, timeout=self.timeout, proxies=self.proxy_dict,
                                   allow_redirects=True, verify=False)
            
            result['status_code'] = response.status_code
            result['response_time'] = time.time() - start
            
            if response.status_code == 200:
                content = trafilatura.extract(response.text, include_comments=False,
                                              include_tables=True, favor_precision=True)
                metadata = trafilatura.extract_metadata(response.text)
                result['content'] = content or ''
                result['title'] = metadata.title if metadata else ''
                result['author'] = metadata.author if metadata else ''
                result['date'] = metadata.date if metadata else ''
                result['word_count'] = len((content or '').split())
            else:
                result['error'] = f'HTTP {response.status_code}'
                
        except Exception as e:
            result['response_time'] = time.time() - start
            result['error'] = str(e)[:200]
        
        return result
    
    def run(self):
        """Run the scraper with auto-reconnect."""
        self.logger.info(f"🚀 Starting Content Scraper (with auto-reconnect)")
        self.logger.info(f"📁 Input: {self.urls_file}")
        self.logger.info(f"📤 Output: {self.output_dir}")
        self.logger.info(f"🔐 Proxy: {self.proxy or 'None (direct)'}")
        self.logger.info(f"⚡ Workers: {self.max_workers}")
        self.logger.info(f"💡 Press Ctrl+C to gracefully stop and save progress")
        
        # Initial proxy test
        if self.proxy_dict and not self.test_proxy():
            if not self.wait_for_proxy():
                return
        
        # Load URLs
        df = pd.read_csv(self.urls_file, on_bad_lines='skip')
        if 'url' in df.columns:
            all_urls = df['url'].dropna().unique().tolist()
        elif 'resolved_url' in df.columns:
            all_urls = df['resolved_url'].dropna().unique().tolist()
        else:
            all_urls = df.iloc[:, 0].dropna().unique().tolist()
        
        urls_to_process = [url for url in all_urls if url not in self.processed_urls]
        
        self.stats['total'] = len(all_urls)
        self.stats['skipped'] = len(self.processed_urls)
        self.stats['start_time'] = time.time()
        
        self.logger.info(f"📊 Total URLs: {len(all_urls):,}")
        self.logger.info(f"⏭️  Already processed: {self.stats['skipped']:,}")
        self.logger.info(f"🎯 URLs to process: {len(urls_to_process):,}")
        
        if not urls_to_process:
            self.logger.info("✅ All URLs already processed!")
            return
        
        # Find current batch number
        existing_batches = list(self.output_dir.glob('scraped_batch_*.csv'))
        batch_num = max([int(f.stem.split('_')[-1]) for f in existing_batches]) if existing_batches else 0
        self.logger.info(f"📂 Starting from batch {batch_num + 1}")
        
        # Process in smaller chunks for better control
        batch_size = 500
        current_batch_results = []
        url_index = 0
        
        while url_index < len(urls_to_process) and not self.shutdown_requested:
            # Check proxy health periodically
            if self.proxy_dict and self.consecutive_proxy_errors >= self.proxy_error_threshold:
                self.proxy_healthy = False
                # Save current progress before pausing
                if current_batch_results:
                    batch_num += 1
                    self._save_batch(batch_num, current_batch_results)
                    for r in current_batch_results:
                        self.processed_urls.add(r['url'])
                    self._save_progress()
                    self.logger.info(f"💾 Saved batch {batch_num} before pause")
                    current_batch_results = []
                
                if not self.wait_for_proxy():
                    break
            
            # Get next chunk of URLs
            chunk_end = min(url_index + self.max_workers * 2, len(urls_to_process))
            chunk_urls = urls_to_process[url_index:chunk_end]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.scrape_url, url): url for url in chunk_urls}
                
                for future in as_completed(futures):
                    if self.shutdown_requested:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    
                    result = future.result()
                    current_batch_results.append(result)
                    url_index += 1
                    
                    # Check for proxy errors
                    if result['error'] and self.is_proxy_error(result['error']):
                        self.consecutive_proxy_errors += 1
                    else:
                        self.consecutive_proxy_errors = 0
                    
                    # Update stats
                    with self.stats_lock:
                        if result['content']:
                            self.stats['success'] += 1
                        else:
                            self.stats['failed'] += 1
                        
                        processed = self.stats['success'] + self.stats['failed']
                        total_done = processed + self.stats['skipped']
                        
                        # Log every 100
                        if processed % 100 == 0:
                            elapsed = time.time() - self.stats['start_time']
                            rate = processed / elapsed if elapsed > 0 else 0
                            remaining = len(urls_to_process) - processed
                            eta = remaining / rate if rate > 0 else 0
                            
                            self.logger.info(
                                f"📈 Progress: {total_done:,}/{self.stats['total']:,} ({100*total_done/self.stats['total']:.1f}%) | "
                                f"Success: {self.stats['success']:,} | Failed: {self.stats['failed']:,} | "
                                f"Rate: {rate:.1f}/sec | ETA: {eta/60:.0f}min"
                            )
                        
                        # Save batch every 500
                        if len(current_batch_results) >= batch_size:
                            batch_num += 1
                            self._save_batch(batch_num, current_batch_results)
                            for r in current_batch_results:
                                self.processed_urls.add(r['url'])
                            self._save_progress()
                            self.logger.info(f"💾 Saved batch {batch_num} + progress")
                            current_batch_results = []
        
        # Save any remaining results
        if current_batch_results:
            batch_num += 1
            self._save_batch(batch_num, current_batch_results)
            for r in current_batch_results:
                self.processed_urls.add(r['url'])
            self._save_progress()
            self.logger.info(f"💾 Saved final batch {batch_num}")
        
        # Summary
        elapsed = time.time() - self.stats['start_time']
        self.logger.info("")
        if self.shutdown_requested:
            self.logger.info("⏹️  Scraping paused (Ctrl+C)")
        else:
            self.logger.info("🎉 Scraping complete!")
        self.logger.info(f"📊 This session: {self.stats['success'] + self.stats['failed']:,} URLs")
        self.logger.info(f"📊 Total processed: {len(self.processed_urls):,}/{self.stats['total']:,}")
        self.logger.info(f"✅ Success: {self.stats['success']:,} | ❌ Failed: {self.stats['failed']:,}")
        self.logger.info(f"⏱️ Time: {elapsed/60:.1f} minutes")
        self.logger.info(f"💡 Run again to resume from where you left off")


def main():
    parser = argparse.ArgumentParser(description='Content Scraper with Auto-Reconnect')
    parser.add_argument('--urls-file', required=True, help='CSV file with URLs')
    parser.add_argument('--output-dir', required=True, help='Output directory')
    parser.add_argument('--proxy', default=None, help='Proxy URL (e.g., socks5://localhost:8888)')
    parser.add_argument('--workers', type=int, default=20, help='Parallel workers')
    parser.add_argument('--timeout', type=int, default=15, help='Request timeout')
    
    args = parser.parse_args()
    
    scraper = ContentScraper(
        urls_file=args.urls_file,
        output_dir=args.output_dir,
        proxy=args.proxy,
        max_workers=args.workers,
        timeout=args.timeout
    )
    scraper.run()


if __name__ == '__main__':
    main()
