# downloaders/arxiv.py
import os, json, random, asyncio, re, xml.etree.ElementTree as ET
from typing import Tuple, List, Dict, Optional
import aiohttp
from urllib.parse import quote_plus
from .base_downloader import BaseDownloader
from . import utils

class ArxivDownloader(BaseDownloader):
    def _parse_single_entry(self, entry: ET.Element) -> Dict:
        """Extracts metadata from a single ArXiv <entry> XML tag."""
        try:
            arxiv_id_url = entry.find('id').text
            arxiv_id = arxiv_id_url.split('/abs/')[-1]
            doi = f"arxiv:{arxiv_id}"

            title = entry.find('title').text.strip().replace('\n', ' ')
            abstract = entry.find('summary').text.strip().replace('\n', ' ')
            published_date = entry.find('published').text
            year = published_date.split('-')[0]
            
            authors = [author.find('name').text for author in entry.findall('author')]
            keywords = [cat.get('term') for cat in entry.findall('category')]

            return {
                'doi': doi, 'title': title, 'authors': authors, 'editor': 'ArXiv',
                'year': year, 'abstract': abstract, 'keywords': keywords
            }
        except Exception as e:
            print(f"  - ArXiv entry parsing error: {e}")
            return {}

    async def download(self, session: aiohttp.ClientSession, doi: str, publisher_dir: str) -> Tuple[bool, List[str], str]:
        if not doi.lower().startswith('arxiv:'):
            return False, [], "not_arxiv"
        
        arxiv_id = doi.split(':', 1)[1]
        formats_downloaded = []
        
        # 1. API call for metadata
        api_url = f"http://export.arxiv.org/api/query?id_list={arxiv_id}"
        full_metadata = {}
        try:
            await asyncio.sleep(random.uniform(3, 5))
            print(f"  - [ArXiv] API call for {doi}")
            async with session.get(api_url, timeout=20) as response:
                if response.status == 200:
                    xml_content = await response.text()
                    
                    xml_path = os.path.join(publisher_dir, 'xml', f"{utils.sanitize_filename(doi)}.xml")
                    with open(xml_path, 'w', encoding='utf-8') as f: f.write(xml_content)
                    formats_downloaded.append('xml')
                    
                    # This parsing is simple, just to get the pdf_url if it exists
                    try:
                        root = ET.fromstring(xml_content)
                        entry = root.find("{http://www.w3.org/2005/Atom}entry")
                        if entry is not None:
                            for link in entry.findall('{http://www.w3.org/2005/Atom}link[@title="pdf"]'):
                                full_metadata['pdf_url'] = link.get('href')
                    except Exception:
                        pass # Ignore if parsing for pdf_url fails

                else:
                    print(f"  - [ArXiv] API error: HTTP {response.status}")

        except Exception as e:
            print(f"  - [ArXiv] API error: {e}")

        # 2. PDF Download
        pdf_url = full_metadata.get('pdf_url', f"https://arxiv.org/pdf/{arxiv_id}.pdf")
        try:
            await asyncio.sleep(random.uniform(3, 5))
            print(f"  - [ArXiv] Downloading PDF for {doi}")
            async with session.get(pdf_url, timeout=30) as response:
                if response.status == 200:
                    content = await response.read()
                    is_valid, _, _ = utils.validate_pdf_multi_library(content, doi)
                    if is_valid:
                        pdf_path = os.path.join(publisher_dir, 'pdf', f"{utils.sanitize_filename(doi)}.pdf")
                        with open(pdf_path, 'wb') as f: f.write(content)
                        formats_downloaded.append('pdf')
                else:
                    print(f"  - [ArXiv] PDF download error: HTTP {response.status}")
        except Exception as e:
            print(f"  - [ArXiv] PDF download error: {e}")

        return len(formats_downloaded) > 0, sorted(list(set(formats_downloaded))), "success" if formats_downloaded else "download_failed"


    async def search(self, session: aiohttp.ClientSession, keyword: str, year: Optional[int] = None, max_results: int = 200) -> List[Dict]:
        query = f'all:"{keyword}"'
        if year and 1900 < year < 2100:
            query += f' AND submittedDate:[{year}0101 TO {year}1231]'

        search_url = "http://export.arxiv.org/api/query"
        params = {"search_query": query, "start": 0, "max_results": max_results, "sortBy": "submittedDate", "sortOrder": "descending"}
        
        try:
            print(f"  - [ArXiv Search] Searching for '{keyword}' (Year: {year or 'All'})")
            await asyncio.sleep(random.uniform(3, 5))
            async with session.get(search_url, params=params, timeout=30) as response:
                if response.status != 200:
                    print(f"  - [ArXiv Search] API Error: HTTP {response.status}")
                    return []

                xml_content = await response.text()
                # Remove namespace for easier parsing
                xml_content = re.sub(r' xmlns="[^"]+"', '', xml_content, count=1)
                root = ET.fromstring(xml_content)
                
                articles = [meta for entry in root.findall("entry") if (meta := self._parse_single_entry(entry))]
                
                print(f"  - [ArXiv Search] Found {len(articles)} results.")
                return articles

        except Exception as e:
            print(f"  - [ArXiv Search] Critical error during search: {e}")
            return []