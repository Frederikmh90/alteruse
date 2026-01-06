#!/usr/bin/env python3
"""
VM ExpressVPN Connector - Easy VPN setup and testing for Norwegian scraper
"""

import os
import sys
import time
import subprocess
import requests
import logging
from pathlib import Path
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_credentials(config_path="config/credentials.yaml"):
    """Load credentials from config file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            return config.get("expressvpn", {})
    except Exception as e:
        logger.error(f"❌ Failed to load config: {e}")
        return {}

def create_auth_file(username, password, output_file="expressvpn_auth.txt"):
    """Create auth file for OpenVPN."""
    try:
        with open(output_file, "w") as f:
            f.write(f"{username}\n{password}")
        os.chmod(output_file, 0o600)  # Secure permissions
        logger.info(f"✅ Created auth file: {output_file}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create auth file: {e}")
        return False

def find_ovpn_files(config_dir="config"):
    """Find all .ovpn config files in config directory."""
    ovpn_files = list(Path(config_dir).glob("*.ovpn"))
    return ovpn_files


def check_current_ip():
    """Check current IP address."""
    try:
        response = requests.get("http://ipinfo.io/json", timeout=10)
        data = response.json()
        return {
            "ip": data.get("ip"),
            "country": data.get("country"),
            "city": data.get("city"),
            "org": data.get("org"),
        }
    except Exception as e:
        logger.error(f"❌ Failed to check IP: {e}")
        return None


def connect_vpn(ovpn_file):
    """Connect to VPN using OpenVPN config file."""
    try:
        logger.info(f"🔐 Connecting to VPN using: {ovpn_file}")
        
        # Ensure auth file exists in current directory for OpenVPN to find it
        # The OVPN files expect 'auth-user-pass expressvpn_auth.txt'
        if not os.path.exists("expressvpn_auth.txt"):
            # Try to load from config and create it
            creds = load_credentials()
            if creds and creds.get("username") and creds.get("password"):
                create_auth_file(creds["username"], creds["password"])
            else:
                logger.error("❌ expressvpn_auth.txt missing and no credentials in config")
                return False

        # Start OpenVPN in background
        cmd = f"sudo openvpn --config {ovpn_file} --daemon"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ OpenVPN started successfully")
            
            # Wait for connection
            logger.info("⏳ Waiting for VPN connection...")
            time.sleep(10)
            
            # Check new IP
            new_ip_info = check_current_ip()
            if new_ip_info:
                logger.info(
                    f"🌍 New IP: {new_ip_info['ip']} ({new_ip_info['country']}, {new_ip_info['city']})"
                )
                return True
            else:
                logger.error("❌ Failed to verify new IP")
                return False
        else:
            logger.error(f"❌ OpenVPN failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ VPN connection failed: {e}")
        return False


def disconnect_vpn():
    """Disconnect VPN."""
    try:
        logger.info("🔌 Disconnecting VPN...")
        subprocess.run("sudo pkill openvpn", shell=True)
        time.sleep(3)
        
        # Check IP after disconnect
        ip_info = check_current_ip()
        if ip_info:
            logger.info(f"🌍 Disconnected - IP: {ip_info['ip']} ({ip_info['country']})")
        
        return True
    except Exception as e:
        logger.error(f"❌ VPN disconnect failed: {e}")
        return False


def main():
    logger.info("🚀 VM ExpressVPN Connector")
    logger.info("=" * 50)
    
    # Check current IP first
    logger.info("📡 Checking current IP...")
    current_ip = check_current_ip()
    if current_ip:
        logger.info(
            f"🌍 Current IP: {current_ip['ip']} ({current_ip['country']}, {current_ip['city']})"
        )
    
    # Find config files
    ovpn_files = find_ovpn_files()
    if not ovpn_files:
        logger.error("❌ No .ovpn config files found in config/ directory!")
        sys.exit(1)
    
    logger.info(f"📂 Found {len(ovpn_files)} config files:")
    for i, file in enumerate(ovpn_files):
        logger.info(f"   {i+1}. {file.name}")
    
    # Interactive selection
    if len(sys.argv) > 1:
        if sys.argv[1] == "disconnect":
            disconnect_vpn()
            sys.exit(0)
        else:
            try:
                selection = int(sys.argv[1]) - 1
                if 0 <= selection < len(ovpn_files):
                    selected_file = ovpn_files[selection]
                else:
                    logger.error("❌ Invalid selection")
                    sys.exit(1)
            except ValueError:
                logger.error("❌ Invalid selection")
                sys.exit(1)
    else:
        # Default to first file (Denmark) if not specified
        selected_file = ovpn_files[0]
        logger.info(f"👉 Defaulting to: {selected_file.name}")
    
    # Connect to VPN
    if connect_vpn(selected_file):
        logger.info("🎉 VPN Connected Successfully!")
    else:
        logger.error("❌ VPN connection failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
