#!/usr/bin/env python3
import argparse, logging, time
from pathlib import Path
from datetime import datetime
import pandas as pd
from trafilatura import sitemaps
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("discovery")
def discover_urls_for_country(startlist: Path, existing: Path|None, country: str, outdir: Path) -> int:
    if not startlist.exists(): log.error(f"Missing: {startlist}"); return 0
    df = pd.read_excel(startlist)
    if "Website" not in df.columns or "Country" not in df.columns:
        log.error("Startlist must have 'Website' and 'Country'"); return 0
    df = df[df["Country"].str.lower() == country.lower()]
    log.info(f"{country.upper()}: {len(df)} domains")
    existing_urls = set()
    if existing and existing.exists():
        try:
            ex = pd.read_csv(existing)
            if "url" in ex.columns: existing_urls = set(ex["url"].dropna().tolist())
            log.info(f"Loaded {len(existing_urls):,} existing URLs")
        except Exception as e: log.warning(f"Existing CSV read issue: {e}")
    outdir.mkdir(parents=True, exist_ok=True)
    total_new = 0
    for _, r in df.iterrows():
        domain = str(r["Website"]).strip()
        if not domain: continue
        try:
            urls = sitemaps.sitemap_search(domain) or []
            new_urls = [u for u in urls if u not in existing_urls]
            if new_urls:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                domain_clean = domain.replace("https://","").replace("http://","").strip("/").replace("/","_").replace(".","_")
                out = outdir / f"{domain_clean}_new_urls_{ts}.csv"
                pd.DataFrame({"url": new_urls}).to_csv(out, index=False)
                total_new += len(new_urls)
                log.info(f"{domain}: saved {len(new_urls):,} -> {out}")
            else:
                log.info(f"{domain}: no new URLs")
        except Exception as e:
            log.warning(f"{domain}: discovery error: {e}")
        time.sleep(0.5)
    log.info(f"{country.upper()}: total new URLs {total_new:,}")
    return total_new
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--country", required=True, choices=['denmark','sweden','finland','norway'])
    p.add_argument("--startlist", required=True)
    p.add_argument("--existing", default="")
    a = p.parse_args()
    return 0 if discover_urls_for_country(Path(a.startlist), Path(a.existing) if a.existing else None, a.country, Path("data")/f"{a.country}_urls") >= 0 else 1
if __name__ == "__main__": raise SystemExit(main())
