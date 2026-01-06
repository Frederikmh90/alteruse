#!/usr/bin/env python3
"""
Configuration Loader for ExpressVPN Proxy Scraper
================================================
Loads ExpressVPN credentials and settings from YAML config file
with fallback to environment variables for flexibility.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

def load_expressvpn_config() -> Dict:
    """
    Load ExpressVPN configuration from multiple sources in order of priority:
    1. config/credentials.yaml file
    2. Environment variables
    3. Default values (for demo/testing)
    
    Returns:
        Dict containing ExpressVPN configuration
    """
    config = {}
    
    # Try to load from YAML config file first
    config_file = Path("config/credentials.yaml")
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                
            if yaml_config and 'expressvpn' in yaml_config:
                expressvpn_config = yaml_config['expressvpn']
                config['username'] = expressvpn_config.get('username', '')
                config['password'] = expressvpn_config.get('password', '')
                
                # Also load scraping settings if available
                if 'scraping' in yaml_config:
                    config['scraping'] = yaml_config['scraping']
                    
                # Load proxy servers if available
                if 'proxy_servers' in yaml_config:
                    config['proxy_servers'] = yaml_config['proxy_servers']
                
                logger.info("✅ Loaded ExpressVPN config from config/credentials.yaml")
                
                # Validate that credentials are not placeholder values
                if (config['username'] == 'your_email@example.com' or 
                    config['password'] == 'your_password' or
                    not config['username'] or 
                    not config['password']):
                    logger.warning("⚠️  Please update config/credentials.yaml with your actual ExpressVPN credentials")
                else:
                    logger.info(f"🔐 Using ExpressVPN account: {config['username']}")
                    return config
                    
        except Exception as e:
            logger.warning(f"⚠️  Could not load config/credentials.yaml: {e}")
    
    # Fallback to environment variables
    env_username = os.getenv('EXPRESSVPN_USERNAME')
    env_password = os.getenv('EXPRESSVPN_PASSWORD')
    
    if env_username and env_password:
        config['username'] = env_username
        config['password'] = env_password
        logger.info("✅ Loaded ExpressVPN credentials from environment variables")
        logger.info(f"🔐 Using ExpressVPN account: {env_username}")
        return config
    
    # No valid credentials found
    logger.error("❌ No ExpressVPN credentials found!")
    print_credential_instructions()
    return None

def print_credential_instructions():
    """Print instructions for setting up ExpressVPN credentials."""
    print("\n" + "="*60)
    print("🔐 ExpressVPN Credentials Required")
    print("="*60)
    print()
    print("📄 Option 1: Config File (Recommended)")
    print("   1. Edit: config/credentials.yaml")
    print("   2. Replace 'your_email@example.com' with your ExpressVPN email")
    print("   3. Replace 'your_password' with your ExpressVPN password")
    print("   4. Save the file")
    print()
    print("🖥️  Option 2: Environment Variables (Temporary)")
    print("   macOS/Linux:")
    print("     export EXPRESSVPN_USERNAME=your_email@example.com")
    print("     export EXPRESSVPN_PASSWORD=your_password")
    print()
    print("   Windows:")
    print("     set EXPRESSVPN_USERNAME=your_email@example.com")
    print("     set EXPRESSVPN_PASSWORD=your_password")
    print()
    print("💡 These are the same credentials you use to log into ExpressVPN normally")
    print("🔒 The config file is automatically excluded from git for security")
    print()

def validate_credentials(username: str, password: str) -> bool:
    """
    Validate that credentials are not placeholder values.
    
    Args:
        username: ExpressVPN username/email
        password: ExpressVPN password
        
    Returns:
        True if credentials appear valid, False otherwise
    """
    if not username or not password:
        return False
        
    # Check for placeholder values
    placeholder_usernames = [
        'your_email@example.com',
        'your_username',
        'username',
        'example@example.com'
    ]
    
    placeholder_passwords = [
        'your_password',
        'password',
        'your_expressvpn_password'
    ]
    
    if username.lower() in [p.lower() for p in placeholder_usernames]:
        return False
        
    if password.lower() in [p.lower() for p in placeholder_passwords]:
        return False
    
    return True

if __name__ == "__main__":
    # Test the configuration loading
    print("🧪 Testing ExpressVPN Configuration Loading")
    print("-" * 50)
    
    config = load_expressvpn_config()
    
    if config:
        username = config.get('username')
        password = config.get('password')
        
        if validate_credentials(username, password):
            print(f"✅ Valid credentials found for: {username}")
            print("🚀 Ready to start ExpressVPN proxy scraping!")
        else:
            print("❌ Credentials appear to be placeholder values")
            print("Please update config/credentials.yaml with your actual ExpressVPN credentials")
    else:
        print("❌ No valid credentials found")
        print("Please set up your ExpressVPN credentials first")