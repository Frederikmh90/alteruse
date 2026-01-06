#!/usr/bin/env python3
"""
Diagnostic script to analyze why news sources aren't being detected in recently viewed content
"""

import json
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import List, Dict, Any, Tuple, Optional

# Reuse the news source lists from facebook_news_analysis.py
alternative_news_sources = [
    "180grader.dk",
    "24nyt.dk",
    "arbejderen.dk",
    "denkorteavis.dk",
    "dkdox.tv",
    "document.dk",
    "folkets.dk",
    "frihedensstemme.dk",
    "indblik.dk",
    "konfront.dk",
    "kontrast.dk",
    "newspeek.info",
    "nordfront.dk",
    "piopio.dk",
    "redox.dk",
    "sameksistens.com",
    "solidaritet.dk",
    "psst-nyt.dk",
    "180grader.dk",
    "24nyt.dk",
    "arbejderen.dk",
    "dagensblaeser.net",
    "danmarksfriefjernsyn.dk",
    "denkorteavis.dk",
    "denuafhaengige.dk",
    "dkdox.tv",
    "document.dk",
    "folkets.dk",
    "freeobserver.org",
    "tv.frihedensstemme.dk",
    "frihedsbrevet.dk",
    "indblik.dk",
    "indblik.net",
    "konfront.dk",
    "kontrast.dk",
    "newspeek.info",
    "nordfront.dk",
    "piopio.dk",
    "redox.dk",
    "responsmedie.dk",
    "sameksistens.com",
    "solidaritet.dk",
    "aktuelltfokus.se",
    "arbetaren.se",
    "bubb.la",
    "bulletin.nu",
    "detgodasamhallet.com",
    "direktaktion.nu",
    "epochtimes.se",
    "exakt24.se",
    "feministisktperspektiv.se",
    "flamman.se",
    "folkungen.se",
    "friasidor.is",
    "friatider.se",
    "ledarsidorna.se",
    "nationalisten.se",
    "newsvoice.se",
    "nordfront.se",
    "nyadagbladet.se",
    "nyatider.nu",
    "nyheteridag.se",
    "nyhetsbyran.org",
    "proletaren.se",
    "svegot.se",
    "riks.se",
    "samnytt.se",
    "samtiden.nu",
    "tidningensyre.se",
    "vaken.se",
    "addendum.org",
    "allesroger.at",
    "alpenschau.com",
    "anschlaege.at",
    "auf1.tv",
    "contra-magazin.com",
    "info-direkt.eu",
    "kontrast.at",
    "moment.at",
    "mosaik-blog.at",
    "neuezeit.at",
    "report24.news",
    "tagesstimme.comunser-mitteleuropa.com",
    "unsere-zeitung.at",
    "unzensuriert.at",
    "volksstimme.at",
    "wochenblick.at",
    "zackzack.at",
    "zurzeit.at",
    "achgut.com",
    "akweb.de",
    "anonymousnews.org",
    "Anti-spiegel.ru",
    "antifainfoblatt.de",
    "blauenarzisse.de",
    "bnr.de",
    "compact-online.de",
    "der-rechte-rand.de",
    "dieunbestechlichen.com",
    "direkteaktion.org",
    "ef-magazin.de",
    "epochtimes.de",
    "extremnews.com",
    "free21.org",
    "freiewelt.net",
    "diefreiheitsliebe.de",
    "jacobin.de",
    "journalistenwatch.com",
    "jungefreiheit.de",
    "jungewelt.de",
    "jungle.world",
    "kenfm.de",
    "kla.tvde",
    "klassegegenklasse.org",
    "konkret-magazin.de",
    "kraut-zone.de",
    "lotta-magazin.de",
    "marx21.de",
    "missy-magazine.de",
    "mmnews.de",
    "multipolar-magazin.de",
    "nachdenkseiten.de",
    "nachrichtenspiegel.de",
    "neopresse.com",
    "nuoviso.tv",
    "opposition24.com",
    "perspektive-online.net",
    "philosophia-perennis.com",
    "pi-news.net",
    "politikstube.com",
    "pravda-tv.com",
    "redglobe.de",
    "reitschuster.de",
    "rf-news.de",
    "de.rt.com",
    "rubikon.news",
    "sezession.de",
    "unsere-zeit.de",
    "truth24.net",
    "zaronews.world",
    "zuerst.de",
]

mainstream_news_sources = [
    # Danish National Media
    # Danish Regional/Local Media
    "dr.dk",
    "tv2.dk",
    "politiken.dk",
    "berlingske.dk",
    "information.dk",
    "jyllands-posten.dk",
    "borsen.dk",
    "ekstrabladet.dk",
    "bt.dk",
    "kristeligt-dagblad.dk",
    "weekendavisen.dk",
    "finans.dk",
    "altinget.dk",
    "tv2ostjylland.dk",
    "dbrs.dk",
    "samsoposten.dk",
    "mediawatch.dk",
    "lokalnytkolding.dk",
    "hvidovreavis.dk",
    "sydmedier.dk",
    "folkebladetlemvig.dk",
    "nyborgavis.dk",
    "lokalnythjoerring.dk",
    "nordhavn-avis.dk",
    "lokalnytaalborg.dk",
    "midtvendsysselavis.dk",
    "Østvendsysselfolkeblad.dk",
    "lokalnytassens.dk",
    "koldingavisen.dk",
    "pingvinnyt.dk",
    "naernyt.dk",
    "fla.de",
    "hverdagsnyt.dk",
    "oestbirk-avis.dk",
    "hornsherredlokalavis.dk",
    "voreslokalavis.dk",
    "medtechnews.dk",
    "hjertingposten.dk",
    "nyheder.dk",
    "saebyavis.dk",
    "valavis.blogspot.com",
    "lyngposten.com",
    "aeroedagblad.dk",
    "vafo.dk",
    "kanalfrederikshavn.dk",
    "fodevarewatch.dk",
    "nb-okonomi.dk",
    "tv2kosmopol.dk",
    "ugebrev.dk",
    "lokalnytkoebenhavn.dk",
    "skivefolkeblad.dk",
    "lokalnytodense.dk",
    "vodskovavis.dk",
    "farsoeavis.dk",
    "frederiksbergliv.dk",
    "videnskab.dk",
    "dragoer-nyt.dk",
    "jyllandsavisen.dk",
    "tvmidtvest.dk",
    "a4medier.dk",
    "midtjyllandsavis.dk",
    "viborg-folkeblad.dk",
    "jyllands-posten.dk",
    "ib.dk",
    "lokalnytfredericia.dk",
    "skanderborg.lokalavisen.dk",
    "landbrugsavisen.dk",
    "lokalnytnyborg.dk",
    "folkebladet.info",
    "orestad-avis.dk",
    "netavisnord.dk",
    "standby.dk",
    "erhvervplus.dk",
    "olfi.dk",
    "zetland.dk",
    "kunmors.dk",
    "tvsyd.dk",
    "odenseavisen.dk",
    "radar.dk",
    "nibeavis.dk",
    "tv2bornholm.dk",
    "pov.international",
    "skanderborgliv.dk",
    "politiken.dk",
    "fanougeblad.dk",
]


def analyze_recently_viewed(file_path: str) -> List[Dict[str, Any]]:
    """Analyze recently_viewed.json file to understand news source detection."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = []
        news_entries = []

        # Navigate through the nested structure
        if "recently_viewed" in data:
            for section in data["recently_viewed"]:
                if "children" in section:
                    for child in section["children"]:
                        if "entries" in child:
                            for entry in child["entries"]:
                                timestamp = entry.get("timestamp")
                                entry_data = entry.get("data", {})

                                # Extract URI/URL
                                uri = entry_data.get("uri", "")
                                name = entry_data.get("name", "")
                                watch_time = entry_data.get("watch_time", "0")

                                # Parse URL to get domain
                                try:
                                    parsed_url = urlparse(uri)
                                    domain = parsed_url.netloc.lower()
                                except:
                                    domain = ""

                                # Check if this is a news source
                                is_mainstream = any(
                                    source in domain
                                    for source in mainstream_news_sources
                                )
                                is_alternative = any(
                                    source in domain
                                    for source in alternative_news_sources
                                )

                                if is_mainstream or is_alternative:
                                    news_entries.append(
                                        {
                                            "timestamp": timestamp,
                                            "name": name,
                                            "uri": uri,
                                            "domain": domain,
                                            "is_mainstream": is_mainstream,
                                            "is_alternative": is_alternative,
                                            "watch_time": watch_time,
                                        }
                                    )

        return news_entries

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []


def main():
    """Analyze recently viewed content for news sources."""
    # List of Facebook accounts to analyze
    accounts = [
        "474-4477-c-146147_2025-05-01T19__4477g1746129403719srxFewNlbAZju5243uu5243ufacebook1183916482010520250SiJ208U-nC7KQv8",
        "474-4477-c-146762_2025-05-20T20__4477g1747771587440suGaFXyPAnvju5919uu5919ufacebook10007887586883220052025edRETwUs-d3aYLfw",
        "474-4477-c-146200_2025-05-03T18__4477g1746296303218sb9nDFQYZicju5337uu5337ufacebooksunekrage20250503zrIyPb23-l9VGvZO",
        "474-4477-c-146228_2025-05-06T11__4477g1746531668711sRpIAQEMxihju5394uu5394ufacebookfinnrj06052025jXOVMn29-Uv3JePv",
        "474-4477-c-146112_2025-05-01T13__4477g1746107119525sSNiln2iKnlju5130uu5130ufacebookasgerschaarup01052025w0EZEqgG-mlkGJL9",
        "474-4477-c-146690_2025-05-14T06__4477g1747204611341s0Dv7V4vdMUju5724uu5724ufacebooklarsholstein1014052025NPyxJlhR-oZfsQxc",
        "4477g1748862082907sTsZKB1OSf5ju6043uu6043uFACEBOOK-Z6oXhYL",
        "474-4477-c-146708_2025-05-15T13__4477g1747314612752s0kclZn3eTrju5760uu5760ufacebooklisbethcolsen15052025oTIl1kHw-nKEwd4n",
        "474-4477-c-146660_2025-05-11T07__4477g1746946862474sHKmRH5kslsju5643uu5643ufacebooksusannejoergensen211052025WFRNE9AH-QOTshil",
        "474-4477-c-146328_2025-05-10T09__4477g1746870239016siQsW9GuXzEju5634uu5634ufacebook10000515790741507052025NCcjt7b1-rELiweX",
        "474-4477-c-146268_2025-05-08T06__4477g1746684503953svCkGA0QroOju5496uu5496ufacebooksimpelmand08052025PQeVM10O-C8HgyIJ",
        "474-4477-c-146176_2025-05-02T07__4477g1746171884135sD5wFzG5HlFju5282uu5282ufacebookjakobbrunse02052025mqAchuod-sVXhpqL",
        "474-4477-c-146720_2025-05-15T18__4477g1747333068762soIZTxyI5BDju5781uu5781ufacebook708647836150520257uqaSg2z-chi8Iwx",
        "474-4477-c-146661_2025-05-11T07__4477g1746950098611s815nG4gCYFju5642uu5642ufacebook10008313718357311052025aCWE2cd0-dAs0D5b",
        "474-4477-c-146670_2025-05-11T20__4477g1746996527439smJ3MRHEbJsju5264uu5264ufacebooktroelsjungquisthansen11052025cqFASfo3-ugMOdck",
        "474-4477-c-146688_2025-05-13T15__4477g1747151757502saI4xrj1iTQju5720uu5720ufacebookkristianhartwignolan13052025CNsrGhD6-H5RzND9",
        "474-4477-c-146184_2025-05-02T11__4477g1746184824121srkXs8CFMLQju5221uu5221ufacebooksteenhansen90002052025ZeSlAMts-sdFjzrm",
        "474-4477-c-146307_2025-05-09T12__4477g1746792994334sVD3JWHRsk1ju5596uu5596ufacebookandreasnyvang01052025mzOMlgVS-uFoJ8VQ",
        "474-4477-c-146761_2025-05-20T17__4477g1747760438180snHByfsDcDOju5940uu5940ufacebookmichaelsode1020052025YriaWgbj-zeIHGZl",
    ]

    print("Analyzing Recently Viewed Content for News Sources")
    print("=" * 80)

    all_news_entries = []
    accounts_with_news = 0

    for account in accounts:
        # Try both possible paths
        paths = [
            f"data/Samlet_06112025/Facebook/{account}/logged_information/interactions/recently_viewed.json",
            f"data/Kantar_download_398_unzipped_new/{account}/logged_information/interactions/recently_viewed.json",
        ]

        news_entries = []
        for path in paths:
            if os.path.exists(path):
                news_entries = analyze_recently_viewed(path)
                if news_entries:
                    break

        if news_entries:
            accounts_with_news += 1
            print(f"\nAccount: {account}")
            print(f"Found {len(news_entries)} news entries")

            # Print some examples
            for entry in news_entries[:5]:  # Show first 5 entries
                print(f"\n  Entry:")
                print(f"    Name: {entry['name']}")
                print(f"    Domain: {entry['domain']}")
                print(f"    URI: {entry['uri']}")
                print(
                    f"    Type: {'Mainstream' if entry['is_mainstream'] else 'Alternative'}"
                )

            all_news_entries.extend(news_entries)

    print("\nSummary")
    print("=" * 80)
    print(f"Total accounts analyzed: {len(accounts)}")
    print(f"Accounts with news content: {accounts_with_news}")
    print(f"Total news entries found: {len(all_news_entries)}")

    if all_news_entries:
        # Convert to DataFrame for analysis
        df = pd.DataFrame(all_news_entries)

        # Count by type
        mainstream_count = df["is_mainstream"].sum()
        alternative_count = df["is_alternative"].sum()

        print(f"\nNews Source Types:")
        print(f"  Mainstream news: {mainstream_count}")
        print(f"  Alternative news: {alternative_count}")

        # Show most common domains
        print("\nMost Common News Domains:")
        domain_counts = df["domain"].value_counts().head(10)
        for domain, count in domain_counts.items():
            print(f"  {domain}: {count} entries")


if __name__ == "__main__":
    main()
