# downloaders/base_downloader.py

import aiohttp
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional

class BaseDownloader(ABC):

    @abstractmethod
    async def download(
        self, 
        session: aiohttp.ClientSession, 
        doi: str, 
        publisher_dir: str
    ) -> Tuple[bool, List[str], str]:
        """
        Downloads the full text of an article given a DOI.
        """
        pass

    @abstractmethod
    async def search(
        self,
        session: aiohttp.ClientSession,
        keyword: str,
        year: Optional[int] = None,
        max_results: int = 200
    ) -> List[Dict]:
        """
        Performs a search via an API, with an optional year filter,
        and returns a list of articles in a standardized format.
        """
        pass