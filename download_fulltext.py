#!/usr/bin/env python3
import os
import csv
import time
import re
import json
import asyncio
import threading
import traceback
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
import aiohttp

# Suppress warnings
import warnings
warnings.filterwarnings("ignore")

# Import specific downloaders and utilities
try:
    from downloaders.elsevier import ElsevierDownloader
    from downloaders.springer import SpringerDownloader
    from downloaders.wiley import WileyDownloader
    from downloaders.arxiv import ArxivDownloader
    from downloaders.mdpi import MdpiDownloader
    from downloaders.acl import AclDownloader
    from downloaders.utils import sanitize_filename
    from doi_mapping import get_editor_from_doi
except ImportError as e:
    print("="*60)
    print("IMPORT ERROR:")
    print(f"   Error details: {e}")
    print("="*60)
    exit(1)

# Configuration
MAX_CONCURRENT_DOWNLOADS = 5

class EnhancedFullTextDownloader:
    def __init__(self, csv_file: Optional[str] = None):
        self.csv_file = csv_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_output_dir = script_dir
        
        self.stats = defaultdict(lambda: defaultdict(int))
        self.csv_lock = threading.Lock()
        self.query_index_files = {}

        self.dispatch_table = {
            "Elsevier": ElsevierDownloader(),
            "Springer": SpringerDownloader(),
            "Wiley": WileyDownloader(),
            "MDPI": MdpiDownloader(),
            "ArXiv": ArxivDownloader(),
            "ACL": AclDownloader()
        }
        print("🚀 ENHANCED Full Text Downloader Initialized")

    def get_downloader_for_publisher(self, publisher: str):
        for key, instance in self.dispatch_table.items():
            if key.lower() in publisher.lower():
                return instance
        return None

    def _sanitize_query_name(self, query: str) -> str:
        safe_name = re.sub(r'[^\w\s-]', '', query).strip()
        return re.sub(r'[\s_]+', '_', safe_name)
        
    async def _search_orchestrator(self, keyword: str, publishers: List[str], year: Optional[int]):
        connector = aiohttp.TCPConnector(limit=len(publishers) * 2, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                asyncio.create_task(downloader.search(session, keyword, year))
                for pub_name in publishers
                if (downloader := self.get_downloader_for_publisher(pub_name)) and hasattr(downloader, 'search')
            ]
            results_from_apis = await asyncio.gather(*tasks, return_exceptions=True)
            
            all_articles = []
            for result in results_from_apis:
                if isinstance(result, list):
                    all_articles.extend(result)
                elif isinstance(result, Exception):
                    print(f"  - An error occurred during an API search: {result}")
            
            return all_articles

    def search_live_apis(self, keyword: str, publishers: List[str], year: Optional[int] = None) -> List[Dict]:
        print(f"--- Starting live search for '{keyword}' on {publishers} ---")
        if not publishers:
            return []
        
        results = asyncio.run(self._search_orchestrator(keyword, publishers, year))
        print(f"--- Live search completed. Found {len(results)} total articles. ---")
        return results

    def _create_index_file(self, index_path: str):
        try:
            with open(index_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['doi', 'title', 'authors', 'keywords', 'editor', 'available_formats', 'path_folder', 'fulltext_quality'])
            print(f"📄 Created index file: {index_path}")
        except Exception as e:
            print(f"❌ Error creating index file {index_path}: {e}")

    def _append_to_index(self, query: str, article_data: Dict, available_formats: List[str]):
        index_file = self.query_index_files.get(query)
        if not index_file:
            print(f"⚠️ Index file for query '{query}' not found. Cannot append.")
            return

        doi = article_data.get('doi', 'N/A')
        title = article_data.get('title', 'N/A')
        authors = '; '.join(article_data.get('authors', []))
        keywords = '; '.join(article_data.get('keywords', []))
        editor = article_data.get('editor', 'N/A')
        path_folder = sanitize_filename(editor)
        formats_str = ';'.join(available_formats)
        quality = "full" if len(available_formats) >= 2 else "basic"
        
        with self.csv_lock:
            try:
                with open(index_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([doi, title, authors, keywords, editor, formats_str, path_folder, quality])
            except Exception as e:
                print(f"  ❌ Error updating index for {doi}: {e}")

    def _create_query_structure(self, query: str, publishers: Set[str], output_base_dir: str):
        query_dir_name = self._sanitize_query_name(query)
        query_path = os.path.join(output_base_dir, query_dir_name)
        
        index_file_path = os.path.join(query_path, f"index_{query_dir_name}.csv")
        self.query_index_files[query] = index_file_path
        
        if not os.path.exists(index_file_path):
            os.makedirs(query_path, exist_ok=True)
            self._create_index_file(index_file_path)

        for publisher in publishers:
            publisher_dir_name = sanitize_filename(publisher)
            for fmt in ['pdf', 'xml', 'json', 'text']:
                os.makedirs(os.path.join(query_path, publisher_dir_name, fmt), exist_ok=True)
        return query_path

    async def _download_article(self, session: aiohttp.ClientSession, article_info: Dict, query: str, query_path: str):
        doi = article_info.get('doi')
        publisher = article_info.get('editor')

        if not doi or not publisher:
            return doi, False, "missing_doi_or_publisher"

        downloader = self.get_downloader_for_publisher(publisher)
        if not downloader:
            return doi, False, "unsupported_publisher"

        publisher_dir_name = sanitize_filename(publisher)
        publisher_dir = os.path.join(query_path, publisher_dir_name)
            
        try:
            success, formats, reason = await downloader.download(session, doi, publisher_dir)
            if success:
                self.stats[query][publisher] += 1
                # --- PUNTO CHIAVE ---
                # La scrittura sul file CSV avviene qui, subito dopo
                # un download andato a buon fine, per ogni singolo articolo.
                self._append_to_index(query, article_info, formats)
            return doi, success, reason
        except Exception as e:
            return doi, False, f"error: {str(e)[:30]}"

    async def _process_download_batch(self, batch: List[Dict], query: str, query_path: str, progress_callback=None):
        total_articles = len(batch)
        processed_count = 0
        
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_DOWNLOADS, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self._download_article(session, article, query, query_path) for article in batch]
            results = []
            for future in asyncio.as_completed(tasks):
                result = await future
                processed_count += 1
                if progress_callback:
                    progress_callback(processed_count, total_articles, f"Downloading... {processed_count}/{total_articles}")
                results.append(result)
            return results

    def download_selected_articles(self, articles_to_download: List[Dict], query_name: str, output_base_dir: str, progress_callback=None) -> Dict:
        print(f"\n--- Starting download from GUI for query: '{query_name}' ---")
        
        publishers = {art.get('editor', 'Unknown') for art in articles_to_download}
        query_path = self._create_query_structure(query_name, publishers, output_base_dir)
        
        final_results = {'successful': [], 'failed': []}
        try:
            batch_results = asyncio.run(self._process_download_batch(articles_to_download, query_name, query_path, progress_callback))
            
            for doi, success, reason in batch_results:
                if success:
                    final_results['successful'].append(doi)
                else:
                    final_results['failed'].append(doi)
        
        except Exception as e:
            print(f"❌ Critical error during download orchestration: {e}")
            traceback.print_exc()

        return final_results