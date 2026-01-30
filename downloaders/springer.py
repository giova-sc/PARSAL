# downloaders/springer.py
import os, json, random, asyncio, re, xml.etree.ElementTree as ET
from typing import Tuple, List, Dict, Optional
import aiohttp
from urllib.parse import quote_plus
from .base_downloader import BaseDownloader
from . import utils

# API Key
SPRINGER_API_KEY = os.getenv("SPRINGER_API_KEY") 
USER_AGENTS = ["Mozilla/5.0 (Research; TDM) Chrome/120.0 Academic/TDM"]

class SpringerDownloader(BaseDownloader):
    def _extract_enhanced_metadata_from_jats(self, jats_xml: str, doi: str) -> Dict:
        try:
            jats_xml = re.sub(' xmlns="[^"]+"', '', jats_xml, count=1)
            root = ET.fromstring(jats_xml)
            abstract_elem = root.find('.//abstract')
            if abstract_elem is not None:
                return {'abstract': ''.join(abstract_elem.itertext()).strip()}
        except Exception:
            return {}
        return {}

    async def _fetch_full_metadata(self, session: aiohttp.ClientSession, doi: str) -> Dict:
        if not doi or not SPRINGER_API_KEY: return {}
        jats_url = f"https://api.springernature.com/openaccess/jats?q=doi:\"{doi}\"&api_key={SPRINGER_API_KEY}"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            await asyncio.sleep(0.5)
            async with session.get(jats_url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    content = await response.text()
                    if '<article' in content:
                        return self._extract_enhanced_metadata_from_jats(content, doi)
        except Exception:
            pass
        return {}

    async def download(self, session: aiohttp.ClientSession, doi: str, publisher_dir: str) -> Tuple[bool, List[str], str]:
        formats_downloaded = []
        headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/xml, */*"}

        # 1. JATS XML Full Text API
        if SPRINGER_API_KEY:
            jats_url = f"https://api.springernature.com/openaccess/jats?q=doi:\"{doi}\"&api_key={SPRINGER_API_KEY}"
            try:
                await asyncio.sleep(0.8)
                async with session.get(jats_url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        content = await response.text()
                        if ('<article' in content) and len(content) > 2000:
                            xml_path = os.path.join(publisher_dir, 'xml', f"{utils.sanitize_filename(doi)}.xml")
                            with open(xml_path, 'w', encoding='utf-8') as f: f.write(content)
                            formats_downloaded.append('xml')
                            
                            json_data = self._extract_enhanced_metadata_from_jats(content, doi)
                            if json_data:
                                json_path = os.path.join(publisher_dir, 'json', f"{utils.sanitize_filename(doi)}.json")
                                with open(json_path, 'w', encoding='utf-8') as f: json.dump(json_data, f, indent=2)
                                formats_downloaded.append('json')
            except Exception as e:
                print(f"  - Springer JATS error: {e}")

        # 2. PDF Download
        pdf_url = f"https://link.springer.com/content/pdf/{quote_plus(doi)}.pdf"
        try:
            async with session.get(pdf_url, headers={**headers, "Accept": "application/pdf"}, timeout=25) as response:
                if response.status == 200:
                    content = await response.read()
                    is_valid, _, _ = utils.validate_pdf_multi_library(content, doi)
                    if is_valid:
                        pdf_path = os.path.join(publisher_dir, 'pdf', f"{utils.sanitize_filename(doi)}.pdf")
                        with open(pdf_path, 'wb') as f: f.write(content)
                        formats_downloaded.append('pdf')
        except Exception as e:
            print(f"  - Springer PDF error: {e}")
            
        return len(formats_downloaded) > 0, sorted(list(set(formats_downloaded))), "success" if formats_downloaded else "failed"

    async def search(self, session: aiohttp.ClientSession, keyword: str, year: Optional[int] = None, max_results: int = 200) -> List[Dict]:
        if not SPRINGER_API_KEY:
            print("  - [Springer Search] API key (SPRINGER_API_KEY) not set. Skipping search.")
            return []

        query = f'keyword:"{quote_plus(keyword)}"'
        if year and 1900 < year < 2100:
            query += f' AND year:{year}'

        search_url = f"https://api.springernature.com/metadata/json?q=({query})&p={max_results}&api_key={SPRINGER_API_KEY}"
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        
        try:
            print(f"  - [Springer Search] Step 1: Searching for '{keyword}' (Year: {year or 'All'})")
            async with session.get(search_url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    print(f"  - [Springer Search] API Error: HTTP {response.status} - Check your SPRINGER_API_KEY.")
                    return []

                data = await response.json()
                articles_to_enrich = []
                
                for record in data.get('records', []):
                    pub_date = record.get('publicationDate', 'N/A')
                    extracted_year = pub_date.split('-')[0] if pub_date else 'N/A'
                    articles_to_enrich.append({
                        'doi': record.get('doi', ''),
                        'title': record.get('title', 'No Title'),
                        'authors': [creator.get('creator') for creator in record.get('creators', [])],
                        'editor': 'Springer', 'year': extracted_year,
                        'keywords': [kw['keyword'] for kw in record.get('keyword', []) if isinstance(kw, dict)]
                    })
                
                if not articles_to_enrich: return []
                
                print(f"  - [Springer Search] Step 2: Enriching {len(articles_to_enrich)} articles.")
                tasks = [self._fetch_full_metadata(session, article['doi']) for article in articles_to_enrich]
                metadata_results = await asyncio.gather(*tasks)

                enriched_articles = []
                for i, article in enumerate(articles_to_enrich):
                    full_meta = metadata_results[i]
                    article['abstract'] = full_meta.get('abstract', 'No abstract available.')
                    enriched_articles.append(article)

                print(f"  - [Springer Search] Found and enriched {len(enriched_articles)} results.")
                return enriched_articles

        except Exception as e:
            print(f"  - [Springer Search] Critical error during search: {e}")
            return []