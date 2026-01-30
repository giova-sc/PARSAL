# downloaders/elsevier.py
import os
import json
import random
import asyncio
from typing import Tuple, List, Dict, Optional
import aiohttp
from urllib.parse import quote_plus
from .base_downloader import BaseDownloader
from . import utils

# API KEY

ELSEVIER_API_KEY = os.getenv("ELSEVIER_API_KEY")
#ELSEVIER_API_KEY = os.getenv("ELSEVIER_API_KEY", "MANUAL_API_KEY")
ELSEVIER_INST_TOKEN = os.getenv("ELSEVIER_INST_TOKEN", "")
USER_AGENTS = ["Mozilla/5.0 (Research; TDM) Chrome/120.0 Academic/TDM"]

class ElsevierDownloader(BaseDownloader):
    async def download(self, session: aiohttp.ClientSession, doi: str, publisher_dir: str) -> Tuple[bool, List[str], str]:
        if not ELSEVIER_API_KEY:
            return False, [], "api_key_missing"
        
        formats_downloaded = []
        headers = {"X-ELS-APIKey": ELSEVIER_API_KEY, "User-Agent": random.choice(USER_AGENTS)}
        if ELSEVIER_INST_TOKEN:
            headers["X-ELS-Insttoken"] = ELSEVIER_INST_TOKEN
        
        # PDF
        try:
            async with session.get(f"https://api.elsevier.com/content/article/doi/{quote_plus(doi)}", 
                                 headers={**headers, "Accept": "application/pdf"}, 
                                 timeout=aiohttp.ClientTimeout(total=25)) as response:
                if response.status == 200:
                    content = await response.read()
                    is_valid, pages, _ = utils.validate_pdf_multi_library(content, doi)
                    if is_valid:
                        filename = os.path.join(publisher_dir, 'pdf', f"{utils.sanitize_filename(doi)}.pdf")
                        with open(filename, 'wb') as f: f.write(content)
                        formats_downloaded.append('pdf')
        except Exception as e:
            print(f"  - Elsevier PDF error: {e}")

        # XML and JSON
        for fmt, accept_header in [('xml', 'application/xml'), ('json', 'application/json')]:
            try:
                await asyncio.sleep(0.5)
                async with session.get(f"https://api.elsevier.com/content/article/doi/{quote_plus(doi)}", 
                                     headers={**headers, "Accept": accept_header},
                                     timeout=aiohttp.ClientTimeout(total=20)) as response:
                    if response.status == 200:
                        content = await response.text()
                        if content:
                            filename = os.path.join(publisher_dir, fmt, f"{utils.sanitize_filename(doi)}.{fmt}")
                            with open(filename, 'w', encoding='utf-8') as f: f.write(content)
                            formats_downloaded.append(fmt)
            except Exception: pass
        
        return len(formats_downloaded) > 0, formats_downloaded, "success" if formats_downloaded else "failed"

    async def _fetch_full_metadata(self, session: aiohttp.ClientSession, doi: str) -> Dict:
        """Helper function to get full metadata for a single DOI."""
        if not doi: return {}
        meta_url = f"https://api.elsevier.com/content/article/doi/{quote_plus(doi)}"
        headers = {"X-ELS-APIKey": ELSEVIER_API_KEY, "Accept": "application/json"}
        
        try:
            async with session.get(meta_url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    coredata = data.get('full-text-retrieval-response', {}).get('coredata', {})
                    authors = [author.get('$') for author in coredata.get('dc:creator', []) if author]
                    abstract = coredata.get('dc:description', '')
                    return {'authors': authors, 'abstract': abstract}
                return {}
        except Exception:
            return {}

    async def search(self, session: aiohttp.ClientSession, keyword: str, year: Optional[int] = None, max_results: int = 200) -> List[Dict]:
        """Searches for articles and then enriches the results with full metadata."""
        if not ELSEVIER_API_KEY:
            print("  - [Elsevier Search] API key (ELSEVIER_API_KEY) is missing.")
            return []

        # Reduce the limit for the Scopus API to prevent 400 errors (IMPORTANT) <- depends on API key
        safe_max_results = 25

        # Build the query
        query = f'TITLE-ABS-KEY("{keyword}")'
        if year and 1900 < year < 2100:
            query += f' AND PUBYEAR IS {year}'

        search_url = "https://api.elsevier.com/content/search/scopus"
        headers = {"X-ELS-APIKey": ELSEVIER_API_KEY, "Accept": "application/json"}
        params = {"query": query, "count": safe_max_results}

        try:
            print(f"  - [Elsevier Search] Step 1: Searching for '{keyword}' (Year: {year or 'All'})")
            async with session.get(search_url, headers=headers, params=params, timeout=30) as response:
                if response.status != 200:
                    print(f"  - [Elsevier Search] API Error: HTTP {response.status} - {await response.text()}")
                    return []

                data = await response.json()
                
                articles_to_enrich = []
                for entry in data.get('search-results', {}).get('entry', []):
                    cover_date = entry.get('prism:coverDate', 'N/A')
                    extracted_year = cover_date.split('-')[0] if cover_date else 'N/A'
                    
                    articles_to_enrich.append({
                        'doi': entry.get('prism:doi', ''),
                        'title': entry.get('dc:title', 'No Title'),
                        'editor': 'Elsevier',
                        'year': extracted_year,
                    })

                if not articles_to_enrich:
                    return []

                print(f"  - [Elsevier Search] Step 2: Enriching {len(articles_to_enrich)} articles.")
                tasks = [self._fetch_full_metadata(session, article['doi']) for article in articles_to_enrich]
                metadata_results = await asyncio.gather(*tasks)

                enriched_articles = []
                for i, article in enumerate(articles_to_enrich):
                    full_meta = metadata_results[i]
                    article.update({
                        'authors': full_meta.get('authors', ['N/A']),
                        'abstract': full_meta.get('abstract', 'No abstract available.'),
                        'keywords': []
                    })
                    enriched_articles.append(article)
                
                print(f"  - [Elsevier Search] Found and enriched {len(enriched_articles)} results.")
                return enriched_articles

        except Exception as e:
            print(f"  - [Elsevier Search] Critical error during search: {e}")
            return []