# Alteruse - URL Extraction & Scraping Pipeline

A data pipeline for extracting, resolving, and scraping URLs from browser history and Facebook data donations.

## Project Structure

```
alteruse/
├── pipeline/                      # Main pipeline (3 stages)
│   ├── 1_extract_urls/           # Stage 1: Extract URLs
│   │   ├── browser.py            # From browser history
│   │   └── facebook.py           # From Facebook data
│   ├── 2_resolve_urls/           # Stage 2: Resolve URLs
│   │   ├── resolver.py           # URL resolver engine
│   │   ├── run_resolution.py     # Single-threaded runner
│   │   └── parallel_runner.sh    # Parallel runner (faster)
│   └── 3_scrape_content/         # Stage 3: Scrape Content
│       ├── scraper.py            # Main scraper (with resume!)
│       ├── scraper_direct.py     # Direct scraper (no proxy)
│       └── vpn_proxy.py          # Local SOCKS proxy for VPN
├── scripts/                      # Utility scripts
│   ├── compare_datasets.py       # Compare old/new data
│   └── isolate_new_data.py       # Extract new files only
├── config/                       # Configuration
├── core/                         # Core processing modules
├── utils/                        # Shared utilities
├── docs/                         # Documentation
├── data/                         # Data files
└── archive/                      # Old/unused files
```

## Pipeline Stages

### Stage 1: Extract URLs
```bash
# From browser history
python3 pipeline/1_extract_urls/browser.py --browser-dir /path/to/data --output-dir /path/to/output

# From Facebook data  
python3 pipeline/1_extract_urls/facebook.py --input-dir /path/to/data --output-dir /path/to/output
```

### Stage 2: Resolve URLs (on UCloud VM)
```bash
# Parallel (recommended - much faster)
bash pipeline/2_resolve_urls/parallel_runner.sh

# Single-threaded
python3 pipeline/2_resolve_urls/run_resolution.py
```

### Stage 3: Scrape Content (via ExpressVPN)

The scraper **automatically resumes** if stopped! Just run it again.

**Setup (on your Mac):**
```bash
# Terminal 1: Start VPN proxy (use port 9999 - port 1080 often has issues)
python3 pipeline/3_scrape_content/vpn_proxy.py 9999

# Terminal 2: Connect ExpressVPN, then create tunnel
ssh -R 9999:localhost:9999 ucloud
```

**Test the tunnel (on UCloud):**
```bash
curl --socks5 localhost:9999 http://ipinfo.io/ip
# Should show your ExpressVPN IP, not UCloud IP
```

**Run (on UCloud):**
```bash
cd /work/Datadonationer/urlextraction_scraping
source venv/bin/activate
python3 scrapers/proxy_scraper.py \
    --urls-file data/new_data_251126/combined_resolved_urls.csv \
    --output-dir data/new_data_251126/scraped_content \
    --proxy socks5://localhost:9999 \
    --workers 30
```

**Resume:** Just run the same command again - it skips already processed URLs!

**Rotate IP:** Disconnect/reconnect ExpressVPN to a different server.

## Troubleshooting

### Port 1080 doesn't work
Port 1080 (default SOCKS port) is sometimes blocked or conflicts with other services.

**Solution:** Use port 9999 instead:
```bash
# On Mac
python3 pipeline/3_scrape_content/vpn_proxy.py 9999
ssh -R 9999:localhost:9999 ucloud

# On UCloud
--proxy socks5://localhost:9999
```

### Tunnel connects but curl hangs
The tunnel might bind to IPv6 only. Force IPv4:
```bash
curl --socks5 127.0.0.1:9999 http://ipinfo.io/ip
```

### SSH tunnel disconnects
Use keep-alive options:
```bash
ssh -R 9999:localhost:9999 -o ServerAliveInterval=60 -o ServerAliveCountMax=3 ucloud
```

### Verify tunnel is working
```bash
# On UCloud, check if port is listening
netstat -tlnp 2>/dev/null | grep 9999

# Test the proxy
curl --socks5 127.0.0.1:9999 --connect-timeout 10 http://ipinfo.io/ip
```

## Documentation

- [ExpressVPN SSH Tunnel Guide](docs/EXPRESSVPN_SSH_TUNNEL_GUIDE.md) - Detailed VPN setup

## SSH Configuration

Add to `~/.ssh/config`:
```
Host ucloud
    HostName ssh.cloud.sdu.dk
    User ucloud
    Port 2479
    IdentityFile ~/.ssh/id_rsa
```

## Requirements

```bash
pip install -r requirements.txt
```
