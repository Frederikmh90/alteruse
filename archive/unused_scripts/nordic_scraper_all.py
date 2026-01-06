#!/usr/bin/env python3
import sys, time, logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, trafilatura, pandas as pd, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler("logs/nordic_scraper_all.log"), logging.StreamHandler()])
log = logging.getLogger("scraper")
BLOCKED = {"solidaritet.dk", "indblik.dk"}
class Scraper:
    def __init__(self, country: str, workers: int = 4):
        self.country = country; self.workers = workers
        self.urls_dir = Path("data") / f"{country}_urls"
        self.out_dir = Path("data") / f"{country}_results"; self.out_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"success": 0, "fail": 0, "blocked": 0, "start": time.time()}
    def _is_blocked(self, url: str) -> bool:
        try: return urlparse(url).netloc in BLOCKED
        except Exception: return False
    def load_urls(self) -> List[str]:
        if not self.urls_dir.exists(): log.error(f"URL dir missing: {self.urls_dir}"); return []
        files = sorted(self.urls_dir.glob("*_new_urls_*.csv"))
        if not files: log.error(f"No URL CSVs in {self.urls_dir}"); return []
        urls = []
        for f in files:
            try:
                df = pd.read_csv(f)
                if "url" in df.columns: urls.extend(df["url"].dropna().tolist())
            except Exception as e: log.warning(f"Read error {f.name}: {e}")
        uniq, seen = [], set()
        for u in urls:
            if u not in seen and not self._is_blocked(u): uniq.append(u); seen.add(u)
            elif self._is_blocked(u): self.stats["blocked"] += 1
        log.info(f"{self.country.upper()}: loaded {len(uniq):,} URLs (blocked skipped: {self.stats['blocked']:,})")
        return uniq
    def scrape_one(self, url: str) -> Optional[Dict]:
        try:
            r = requests.get(url, headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
                                           "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                           "Accept-Language":"en-US,en;q=0.9","Connection":"keep-alive"},
                             timeout=30, verify=False, allow_redirects=True)
            if r.status_code != 200: return None
            content = trafilatura.extract(r.text, no_fallback=False)
            if content and len(content.strip()) > 100:
                meta = trafilatura.extract_metadata(r.text); self.stats["success"] += 1
                return {"url":url,"title":getattr(meta,"title",None),"author":getattr(meta,"author",None),
                        "date":getattr(meta,"date",None),"content":content.strip(),"content_length":len(content.strip()),
                        "country":self.country,"scraped_at":datetime.now().isoformat()}
        except Exception: pass
        self.stats["fail"] += 1; return None
    def run(self, max_workers: int) -> None:
        urls = self.load_urls()
        if not urls:
            return
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(self.scrape_one,u):u for u in urls}
            for i,f in enumerate(as_completed(futs),1):
                r=f.result()
                if r: results.append(r)
                if i % 500 == 0:
                    rate = (self.stats["success"]/i)*100
                    log.info(f"{self.country.upper()}: {i}/{len(urls)} | success {self.stats['success']} ({rate:.1f}%)")
        if results:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S"); out = self.out_dir / f"{self.country}_scraped_articles_{ts}.csv"
            pd.DataFrame(results).to_csv(out, index=False); log.info(f"{self.country.upper()}: saved {len(results):,} -> {out}")
        log.info(f"{self.country.upper()}: done | success {self.stats['success']:,} | fail {self.stats['fail']:,} | blocked {self.stats['blocked']:,} | elapsed {time.time()-self.stats['start']:.0f}s")
def main():
    if len(sys.argv)<2: print("Usage: python3 nordic_scraper_all.py <country> [workers]"); return 1
    country=sys.argv[1].lower(); workers=int(sys.argv[2]) if len(sys.argv)>2 else 4
    Scraper(country).run(workers); return 0
if __name__=="__main__": raise SystemExit(main())
