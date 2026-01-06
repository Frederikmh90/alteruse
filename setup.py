#!/usr/bin/env python3
"""
Setup script for the Alteruse Data Donation Project
==================================================
A professional Python package for processing browser and Facebook data donations.
"""

from setuptools import setup, find_packages
import os


# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()


# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [
            line.strip() for line in fh if line.strip() and not line.startswith("#")
        ]


setup(
    name="alteruse",
    version="1.0.0",
    author="AlterUse Research Team",
    author_email="frmohe@ruc.dk",
    description="A comprehensive data donation processing framework for browser and Facebook data analysis",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/alteruse/alteruse",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Sociology",
        "Topic :: Internet :: WWW/HTTP :: Browsers",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
        "optimized": [
            "polars>=0.19.0",
            "ray>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "alteruse-browser=core.browser_processing:main",
            "alteruse-facebook=core.facebook_processing:main",
            "alteruse-news-analysis=core.news_analysis:main",
            "alteruse-url-resolve=pipelines.url_resolution.complete_pipeline:main",
            "alteruse-scrape=scrapers.browser_scraper:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.txt", "*.md", "*.json", "*.csv"],
    },
    keywords="data-donation, browser-data, facebook-data, url-extraction, content-scraping, research",
    project_urls={
        "Bug Reports": "https://github.com/alteruse/alteruse/issues",
        "Source": "https://github.com/alteruse/alteruse",
        "Documentation": "https://alteruse.readthedocs.io/",
    },
)
