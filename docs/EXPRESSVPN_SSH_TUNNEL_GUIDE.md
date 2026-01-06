# ExpressVPN SSH Tunnel for UCloud Scraping

## Overview

This guide explains how we route web scraping traffic from UCloud through ExpressVPN using an SSH reverse tunnel through your local Mac.

---

## The Problem

UCloud VMs run in a containerized environment that **lacks the TUN/TAP kernel module** required for OpenVPN connections. This means we cannot run ExpressVPN directly on the VM.

Without VPN, scraping would use the Danish Research Network (Forskningsnettet) IP, which:
- Could expose the academic network to rate limiting or blocking
- Is not ethical for large-scale web scraping
- Provides no IP rotation capability

---

## The Solution: SSH Reverse Tunnel

We create a "tunnel" that routes traffic from UCloud → Your Mac → ExpressVPN → Internet.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           YOUR MAC                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   ExpressVPN │───▶│ SOCKS Proxy  │◀───│  SSH Reverse Tunnel  │  │
│  │   (System)   │    │ (Port 1080)  │    │  (Listens on 1080)   │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│         │                   │                       ▲               │
│         ▼                   ▼                       │               │
│    ┌─────────┐         Internet                     │               │
│    │ VPN IP  │                                      │               │
│    │85.x.x.x │                                      │               │
│    └─────────┘                                      │               │
└─────────────────────────────────────────────────────│───────────────┘
                                                      │
                                              SSH Connection
                                              (Port 2479)
                                                      │
┌─────────────────────────────────────────────────────│───────────────┐
│                         UCLOUD VM                   │               │
│  ┌──────────────────┐    ┌──────────────────────────▼─────────┐    │
│  │  Python Scraper  │───▶│  localhost:1080 (Tunnel Endpoint)  │    │
│  │  (proxy_scraper) │    │  Forwards to Mac's SOCKS Proxy     │    │
│  └──────────────────┘    └────────────────────────────────────┘    │
│                                                                     │
│  IP: 130.225.164.101 (Research Network - NOT USED for scraping)   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## How It Works Step-by-Step

### 1. ExpressVPN on Your Mac
When you connect ExpressVPN on your Mac, ALL network traffic from your Mac goes through the VPN tunnel. Your Mac gets a new external IP (e.g., 85.203.47.171 from ExpressVPN).

### 2. Local SOCKS5 Proxy (`local_socks_proxy.py`)
This Python script runs on your Mac and creates a SOCKS5 proxy server on port 1080. When a connection comes in, it:
- Accepts the SOCKS5 handshake
- Connects to the requested website
- Forwards data back and forth

Because ExpressVPN is active system-wide, these connections go through the VPN.

### 3. SSH Reverse Tunnel (`ssh -R`)
The command `ssh -R 1080:localhost:1080 ucloud` means:
- Connect to UCloud via SSH
- **Reverse forward** port 1080 on UCloud to port 1080 on your Mac
- Any connection to `localhost:1080` on UCloud is tunneled to your Mac

### 4. Python Scraper with Proxy
The scraper is configured with `--proxy socks5://localhost:1080`. When it makes a request:
1. Request goes to localhost:1080 on UCloud
2. SSH tunnels it to localhost:1080 on your Mac
3. SOCKS proxy connects to the target website
4. Traffic flows through ExpressVPN
5. Response comes back through the same path

---

## When to Rotate IPs

You should consider rotating your ExpressVPN server when:

1. **Rate limiting detected**: Many failed requests, HTTP 429 errors, or CAPTCHAs
2. **Periodic rotation**: Every few hours for safety
3. **Geographic needs**: Switch to servers near your target websites

### How to Rotate

1. On your Mac, disconnect ExpressVPN
2. Connect to a different server (Sweden, Norway, UK, etc.)
3. The scraper continues automatically - no restart needed!

Verify the new IP:
```bash
ssh ucloud 'curl --socks5 localhost:1080 http://ipinfo.io/ip'
```

---

## Commands Reference

### Start Local SOCKS Proxy (Mac Terminal 1)
```bash
cd /Users/Codebase/projects/alteruse
python3 scripts/local_socks_proxy.py
```

### Create SSH Tunnel (Mac Terminal 2)
```bash
ssh -R 1080:localhost:1080 -o ServerAliveInterval=60 ucloud
```
The `-o ServerAliveInterval=60` keeps the connection alive.

### Run Scraper (UCloud)
```bash
cd /work/Datadonationer/urlextraction_scraping
source venv/bin/activate
python3 scrapers/proxy_scraper.py \
    --urls-file data/new_data_251126/combined_resolved_urls.csv \
    --output-dir data/new_data_251126/scraped_content \
    --proxy socks5://localhost:1080 \
    --workers 20
```

### Check Progress
```bash
ssh ucloud 'tail -10 /work/Datadonationer/urlextraction_scraping/data/new_data_251126/scraped_content/scraping_*.log'
```

### Verify VPN IP
```bash
ssh ucloud 'curl --socks5 localhost:1080 http://ipinfo.io/ip'
```

---

## Troubleshooting

### "Connection refused" on port 1080
- Ensure `local_socks_proxy.py` is running on your Mac
- Ensure SSH tunnel is active
- Check if ExpressVPN is connected

### Slow speeds
- Reduce workers: `--workers 10`
- Try a geographically closer ExpressVPN server
- Check your home internet speed

### SSH tunnel disconnects
Use tmux or screen on your Mac to keep the tunnel alive:
```bash
tmux new -s vpn-tunnel
ssh -R 1080:localhost:1080 -o ServerAliveInterval=60 ucloud
# Press Ctrl+B, then D to detach
```

### Scraper stops unexpectedly
Run with nohup to continue if SSH disconnects:
```bash
nohup python3 scrapers/proxy_scraper.py ... > scraping.log 2>&1 &
```

---

## Security Notes

- The SSH tunnel is encrypted end-to-end
- ExpressVPN provides additional encryption
- Your Mac's IP is hidden behind ExpressVPN
- UCloud's research network IP is never exposed to target websites

---

## Files Used

| File | Location | Purpose |
|------|----------|---------|
| `local_socks_proxy.py` | `scripts/` | SOCKS5 proxy on Mac |
| `proxy_scraper.py` | `scrapers/` | Content scraper with proxy support |
| `combined_resolved_urls.csv` | `data/new_data_251126/` | URLs to scrape |
| `scraped_content/` | `data/new_data_251126/` | Output directory |










