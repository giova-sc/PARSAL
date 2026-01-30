# downloaders/wiley.py
import os, json, random, asyncio
from typing import Tuple, List, Dict, Optional
import aiohttp
from urllib.parse import quote_plus
from .base_downloader import BaseDownloader
from . import utils


WILEY_TDM_TOKEN = os.getenv("WILEY_TDM_TOKEN")
#WILEY_TDM_TOKEN = os.getenv("WILEY_TDM_TOKEN", "MANUAL WILEY TOKEN")
USER_AGENTS = ["Mozilla/5.0 (Research; TDM) Chrome/120.0 Academic/TDM"]

class WileyDownloader(BaseDownloader):
    async def download(self, session: aiohttp.ClientSession, doi: str, publisher_dir: str) -> Tuple[bool, List[str], str]:
        if not WILEY_TDM_TOKEN:
            return False, [], "api_key_missing"
        
        formats_downloaded = []
        headers = {"Wiley-TDM-Client-Token": WILEY_TDM_TOKEN, "User-Agent": random.choice(USER_AGENTS)}
        tdm_url = f"https://api.wiley.com/onlinelibrary/tdm/v1/articles/{quote_plus(doi)}"
        
        try:
            # 1. PDF Download
            await asyncio.sleep(random.uniform(1.5, 2.5))
            async with session.get(tdm_url, headers={**headers, "Accept": "application/pdf"}, timeout=35) as response:
                if response.status == 200:
                    content = await response.read()
                    is_valid, pages, _ = utils.validate_pdf_multi_library(content, doi)
                    if is_valid:
                        pdf_path = os.path.join(publisher_dir, 'pdf', f"{utils.sanitize_filename(doi)}.pdf")
                        with open(pdf_path, 'wb') as f: f.write(content)
                        formats_downloaded.append('pdf')
                else:
                    print(f"  - [Wiley Download] PDF download failed with status: {response.status}")

            # 2. JSON Metadata (only if PDF succeeded)
            if 'pdf' in formats_downloaded:
                await asyncio.sleep(1.0)
                async with session.get(tdm_url, headers={**headers, "Accept": "application/json"}, timeout=30) as json_response:
                    if json_response.status == 200:
                        json_data = await json_response.json()
                        if json_data:
                            json_path = os.path.join(publisher_dir, 'json', f"{utils.sanitize_filename(doi)}.json")
                            with open(json_path, 'w', encoding='utf-8') as f: json.dump(json_data, f, indent=2)
                            formats_downloaded.append('json')

            return len(formats_downloaded) > 0, sorted(list(set(formats_downloaded))), "success" if formats_downloaded else "failed"

        except Exception as e:
            return False, [], f"wiley_error: {str(e)[:30]}"

    async def search(self, session: aiohttp.ClientSession, keyword: str, year: Optional[int] = None, max_results: int = 200) -> List[Dict]:
        search_url = "https://api.crossref.org/works"
        
        filters = ["prefix:10.1002,prefix:10.1111"]
        if year and 1900 < year < 2100:
            filters.append(f"from-pub-date:{year}-01-01,until-pub-date:{year}-12-31")

        params = {
            "query.bibliographic": keyword,
            "filter": ",".join(filters),
            "rows": max_results,
            "mailto": "parsal.user@example.com"
        }
        
        try:
            print(f"  - [Wiley/CrossRef Search] Searching for '{keyword}' (Year: {year or 'All'})")
            async with session.get(search_url, params=params, timeout=30) as response:
                if response.status != 200:
                    print(f"  - [Wiley/CrossRef Search] API Error: HTTP {response.status}")
                    return []

                data = await response.json()
                articles = []

                for item in data.get('message', {}).get('items', []):
                    authors_list = [f"{author.get('given', '')} {author.get('family', '')}".strip() for author in item.get('author', [])]
                    
                    extracted_year = 'N/A'
                    date_parts = None
                    if 'published-print' in item and item['published-print']['date-parts']:
                        date_parts = item['published-print']['date-parts'][0]
                    elif 'published-online' in item and item['published-online']['date-parts']:
                        date_parts = item['published-online']['date-parts'][0]
                    
                    if date_parts: extracted_year = str(date_parts[0])

                    standardized_article = {
                        'doi': item.get('DOI', ''),
                        'title': ''.join(item.get('title', ['No Title'])),
                        'authors': authors_list, 'editor': 'Wiley',
                        'year': extracted_year,
                        'abstract': item.get('abstract', 'No abstract available.').replace('<jats:p>', '').replace('</jats:p>', '').strip(),
                        'keywords': item.get('subject', [])
                    }
                    articles.append(standardized_article)

                print(f"  - [Wiley/CrossRef Search] Found {len(articles)} results.")
                return articles
                
        except Exception as e:
            print(f"  - [Wiley/CrossRef Search] Critical error: {e}")
            return []