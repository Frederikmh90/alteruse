#!/usr/bin/env python3
"""Simple batch scraper with strict timeouts - processes sequentially."""
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()


def scrape_url(url, timeout=3):
    """Scrape a single URL with strict timeout."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract title
            title = None
            if soup.title:
                title = soup.title.string
            
            # Extract content
            content = None
            for tag in ['article', 'main', 'body']:
                elem = soup.find(tag)
                if elem:
                    content = elem.get_text(separator=' ', strip=True)[:5000]
                    break
            
            # Extract author
            author = None
            for meta in soup.find_all('meta'):
                if meta.get('name', '').lower() in ['author', 'article:author']:
                    author = meta.get('content')
                    break
            
            # Extract date
            date = None
            for meta in soup.find_all('meta'):
                if meta.get('property', '').lower() in ['article:published_time', 'og:published_time']:
                    date = meta.get('content')
                    break
            
            return {
                'url': url,
                'status_code': resp.status_code,
                'content': content,
                'title': title,
                'author': author,
                'date': date,
                'word_count': len(content.split()) if content else 0,
                'response_time': resp.elapsed.total_seconds(),
                'scraped_at': datetime.now().isoformat(),
                'error': None
            }
        else:
            return {'url': url, 'status_code': resp.status_code, 'content': None, 'title': None, 
                    'author': None, 'date': None, 'word_count': 0, 'response_time': 0,
                    'scraped_at': datetime.now().isoformat(), 'error': f'http_{resp.status_code}'}
    except requests.Timeout:
        return {'url': url, 'status_code': -1, 'content': None, 'title': None, 'author': None, 
                'date': None, 'word_count': 0, 'response_time': 0,
                'scraped_at': datetime.now().isoformat(), 'error': 'timeout'}
    except Exception as e:
        return {'url': url, 'status_code': -1, 'content': None, 'title': None, 'author': None, 
                'date': None, 'word_count': 0, 'response_time': 0,
                'scraped_at': datetime.now().isoformat(), 'error': str(e)[:100]}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/old_facebook_urls_to_scrape_clean.csv')
    parser.add_argument('--output-dir', default='data/old_facebook_scraped')
    parser.add_argument('--timeout', type=int, default=3)
    parser.add_argument('--batch-size', type=int, default=100)
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Clear old files
    for f in output_dir.glob('scraped_batch_*.csv'):
        f.unlink()
    
    df = pd.read_csv(args.input)
    urls = df['url'].tolist()
    
    logger.info(f'Processing {len(urls)} URLs with {args.timeout}s timeout')
    
    results = []
    batch_num = 0
    success = 0
    failed = 0
    start_time = time.time()
    
    for i, url in enumerate(urls):
        result = scrape_url(url, timeout=args.timeout)
        results.append(result)
        
        if result['content']:
            success += 1
        else:
            failed += 1
        
        # Save batch
        if len(results) >= args.batch_size:
            batch_num += 1
            batch_df = pd.DataFrame(results)
            batch_df.to_csv(output_dir / f'scraped_batch_{batch_num:04d}.csv', index=False)
            
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(urls) - i - 1) / rate / 60 if rate > 0 else 0
            
            logger.info(f'Batch {batch_num} | Progress: {i+1}/{len(urls)} ({100*(i+1)/len(urls):.1f}%) | Success: {success} | Failed: {failed} | Rate: {rate:.1f}/sec | ETA: {eta:.0f}min')
            results = []
    
    # Save remaining
    if results:
        batch_num += 1
        batch_df = pd.DataFrame(results)
        batch_df.to_csv(output_dir / f'scraped_batch_{batch_num:04d}.csv', index=False)
        logger.info(f'Saved final batch {batch_num}')
    
    logger.info(f'Done! Total: {len(urls)}, Success: {success}, Failed: {failed}')


if __name__ == '__main__':
    main()

