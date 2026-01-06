# ExpressVPN Tunnel Setup for UCloud Scraping

Since UCloud VMs don't support the TUN kernel module required for OpenVPN, we use an **SSH reverse tunnel** through your local Mac.

## How It Works

```
UCloud VM → SSH Tunnel → Your Mac (with ExpressVPN) → Internet
```

All scraping traffic goes through ExpressVPN on your Mac, protecting the research network IP.

---

## Setup Instructions

### Step 1: Connect ExpressVPN on Your Mac
Open ExpressVPN app and connect to your preferred server (Denmark, Sweden, etc.)

### Step 2: Start the Local SOCKS5 Proxy
In Terminal on your Mac:
```bash
cd /Users/Codebase/projects/alteruse
python3 scripts/local_socks_proxy.py
```

### Step 3: Create SSH Reverse Tunnel to UCloud
In another Terminal window on your Mac:
```bash
ssh -R 1080:localhost:1080 ucloud
```

This forwards port 1080 from UCloud to your local SOCKS proxy.

### Step 4: Run the Scraper on UCloud
In the SSH session (or another terminal connected to UCloud):
```bash
cd /work/Datadonationer/urlextraction_scraping
source venv/bin/activate
python3 scrapers/proxy_scraper.py \
    --urls-file data/new_data_251126/combined_resolved_urls.csv \
    --output-dir data/new_data_251126/scraped_content \
    --proxy socks5://localhost:1080 \
    --workers 20
```

---

## Verify It's Working

On UCloud, test the proxy:
```bash
curl --socks5 localhost:1080 http://ipinfo.io/ip
```

This should show your ExpressVPN IP, not the UCloud IP.

---

## Rotating IPs

To rotate IPs, simply:
1. Disconnect ExpressVPN on your Mac
2. Reconnect to a different server
3. The scraper will automatically use the new IP

You can do this periodically without restarting the scraper or tunnel.

---

## Troubleshooting

### "Connection refused" on port 1080
Make sure both:
- `local_socks_proxy.py` is running on your Mac
- SSH reverse tunnel is active (`ssh -R 1080:localhost:1080 ucloud`)

### Slow speeds
Reduce workers: `--workers 10`

### Tunnel disconnects
Add `-o ServerAliveInterval=60` to the SSH command:
```bash
ssh -R 1080:localhost:1080 -o ServerAliveInterval=60 ucloud
```

