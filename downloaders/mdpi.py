# downloaders/mdpi.py
import os, json, random, asyncio, re
from typing import Tuple, List, Dict, Optional
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from .base_downloader import BaseDownloader
from . import utils

USER_AGENTS = ["Mozilla/5.0 (Research; TDM) Chrome/120.0 Academic/TDM"]

class MdpiDownloader(BaseDownloader):
    async def download(self, session: aiohttp.ClientSession, doi: str, publisher_dir: str) -> Tuple[bool, List[str], str]:
        # Download implementation here
        pass

    async def search(self, session: aiohttp.ClientSession, keyword: str, year: Optional[int] = None, max_results: int = 200) -> List[Dict]:
        """Searches for MDPI articles using the CrossRef API."""
        search_url = "https://api.crossref.org/works"

        filters = ["prefix:10.3390"]
        # FIX: Only add year filter if it's a valid year
        if year and 1900 < year < 2100:
            filters.append(f"from-pub-date:{year}-01-01,until-pub-date:{year}-12-31")

        params = {
            "query.bibliographic": keyword,
            "filter": ",".join(filters),
            "rows": max_results,
            "mailto": "parsal.user@example.com" 
        }

        try:
            print(f"  - [MDPI/CrossRef Search] Searching for '{keyword}' (Year: {year or 'All'})")
            async with session.get(search_url, params=params, timeout=30) as response:
                if response.status != 200:
                    print(f"  - [MDPI/CrossRef Search] API Error: HTTP {response.status}")
                    return []

                data = await response.json()
                articles = []

                for item in data.get('message', {}).get('items', []):
                    authors_list = [f"{author.get('given', '')} {author.get('family', '')}".strip() for author in item.get('author', [])]
                    
                    extracted_year = 'N/A'
                    if 'published' in item and item['published']['date-parts']:
                        extracted_year = str(item['published']['date-parts'][0][0])

                    standardized_article = {
                        'doi': item.get('DOI', ''),
                        'title': ''.join(item.get('title', ['No Title'])),
                        'authors': authors_list, 'editor': 'MDPI',
                        'year': extracted_year,
                        'abstract': item.get('abstract', 'No abstract available.').replace('<jats:p>', '').replace('</jats:p>', '').strip(),
                        'keywords': item.get('subject', [])
                    }
                    articles.append(standardized_article)
                
                print(f"  - [MDPI/CrossRef Search] Found {len(articles)} results.")
                return articles
                
        except Exception as e:
            print(f"  - [MDPI/CrossRef Search] Critical error: {e}")
            return []