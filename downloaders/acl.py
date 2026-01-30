# downloaders/acl.py
import asyncio
from typing import Tuple, List, Dict, Optional

import aiohttp

try:
    from acl_anthology import Anthology
    ANTHOLOGY_AVAILABLE = True
except ImportError:
    ANTHOLOGY_AVAILABLE = False

from .base_downloader import BaseDownloader

# --- Anthology singleton (evita di bloccare l'event loop) ---
_ANTHOLOGY_SINGLETON: Optional["Anthology"] = None
_ANTHOLOGY_INIT_LOCK = asyncio.Lock()

async def _get_anthology_singleton() -> Optional["Anthology"]:
    global _ANTHOLOGY_SINGLETON
    if not ANTHOLOGY_AVAILABLE:
        return None
    if _ANTHOLOGY_SINGLETON is not None:
        return _ANTHOLOGY_SINGLETON
    async with _ANTHOLOGY_INIT_LOCK:
        if _ANTHOLOGY_SINGLETON is not None:
            return _ANTHOLOGY_SINGLETON
        try:
            # from_repo() fa I/O sync su disco: spostiamolo in thread
            _ANTHOLOGY_SINGLETON = await asyncio.to_thread(Anthology.from_repo)
        except Exception as e:
            print(f"❌ CRITICAL: ACL Anthology init failed: {e}")
            _ANTHOLOGY_SINGLETON = None
    return _ANTHOLOGY_SINGLETON

# --- Helper robusti per campi ACL ---

def _text_or_empty(obj) -> str:
    if obj is None:
        return ""
    # MarkupText in acl-anthology espone spesso .text_
    for attr in ("text_", "text", "string"):
        if hasattr(obj, attr):
            try:
                val = getattr(obj, attr)
                return (val if isinstance(val, str) else str(val)).strip()
            except Exception:
                pass
    try:
        return str(obj).strip()
    except Exception:
        return ""

def _authors_as_list(paper) -> List[str]:
    """
    paper.authors è una lista di NameSpecification.
    Usare .first / .last (o il Name interno) per costruire 'First Last'.
    """
    out: List[str] = []
    try:
        for ns in (getattr(paper, "authors", []) or []):
            first = getattr(ns, "first", None)
            last = getattr(ns, "last", None)
            if first and last:
                out.append(f"{first} {last}")
                continue
            if last:
                out.append(str(last))
                continue
            if first:
                out.append(str(first))
                continue
            # Fallback: usare il Name interno, se disponibile
            name_obj = getattr(ns, "name", None)
            if name_obj is not None:
                try:
                    if hasattr(name_obj, "as_full"):
                        out.append(name_obj.as_full())
                    else:
                        # prova {first} {last}
                        f = getattr(name_obj, "first", None)
                        l = getattr(name_obj, "last", None)
                        s = " ".join([x for x in [f, l] if x]).strip()
                        out.append(s if s else str(name_obj))
                except Exception:
                    out.append(str(name_obj))
            else:
                # Ultimo fallback
                out.append(str(ns))
    except Exception:
        pass
    # pulizia
    return [a.strip() for a in out if a and a.strip()]

def _paper_id(paper) -> Optional[str]:
    for attr in ("full_id", "id", "anthology_id", "bibkey"):
        if hasattr(paper, attr):
            try:
                return str(getattr(paper, attr))
            except Exception:
                continue
    return None

def _paper_year(paper) -> Optional[int]:
    try:
        y = getattr(paper, "year", None)
        return int(str(y)) if y is not None else None
    except Exception:
        return None

def _paper_doi(paper) -> Optional[str]:
    try:
        d = getattr(paper, "doi", None)
        return str(d) if d else None
    except Exception:
        return None

def _paper_publisher(paper) -> str:
    try:
        p = getattr(paper, "publisher", None)
        if p:
            return str(p).strip()
    except Exception:
        pass
    # Fallback sicuro (mai None)
    return "ACL Anthology"

def _paper_pdf_url(paper) -> Optional[str]:
    pid = _paper_id(paper)
    return f"https://aclanthology.org/{pid}.pdf" if pid else None

def _paper_page_url(paper) -> Optional[str]:
    pid = _paper_id(paper)
    return f"https://aclanthology.org/{pid}/" if pid else None


class AclDownloader(BaseDownloader):
    def __init__(self) -> None:
        # Istanza gestita come singleton; qui non carichiamo nulla di pesante
        pass

    async def download(
        self,
        session: aiohttp.ClientSession,
        doi: str,
        publisher_dir: str,
    ) -> Tuple[bool, List[str], str]:
        """
        Support both:
        - internal ids: 'acl:<paper_id>'
        - real DOIs: '10.18653/v1/<paper_id>' (or other DOIs; in extremis cerchiamo nel repo)
        Returns: (ok, [saved_paths], message)
        """
        # Ricava l'ACL id
        acl_id: Optional[str] = None
        if doi:
            low = doi.lower()
            if low.startswith("acl:"):
                acl_id = doi.split(":", 1)[1]
            elif "/v1/" in doi:  # la maggior parte dei DOI ACL embedda l'ID
                acl_id = doi.split("/v1/", 1)[1]
            elif low.startswith("10."):
                anthology = await _get_anthology_singleton()
                if anthology is not None:
                    # ricerca lenta ma affidabile
                    def _find_by_doi() -> Optional[str]:
                        for p in anthology.papers():
                            try:
                                if getattr(p, "doi", None) == doi:
                                    return _paper_id(p)
                            except Exception:
                                continue
                        return None
                    acl_id = await asyncio.to_thread(_find_by_doi)

        if not acl_id:
            return (False, [], f"ACL download: unable to resolve paper id from doi='{doi}'")

        pdf_url = f"https://aclanthology.org/{acl_id}.pdf"

        # Scarica il PDF (semplice; adatta se hai già una routine comune)
        try:
            async with session.get(pdf_url, timeout=120) as resp:
                if resp.status != 200:
                    return (False, [], f"ACL download: HTTP {resp.status} for {pdf_url}")
                content = await resp.read()

            # Salvataggio
            safe_id = acl_id.replace("/", "_")
            out_path = f"{publisher_dir}/{safe_id}.pdf"
            # NB: scrittura sincrona delegata a thread per non bloccare
            def _write():
                import os
                os.makedirs(publisher_dir, exist_ok=True)
                with open(out_path, "wb") as f:
                    f.write(content)
            await asyncio.to_thread(_write)

            return (True, [out_path], "ok")
        except Exception as e:
            return (False, [], f"ACL download error: {e}")

    async def search(
        self,
        session: aiohttp.ClientSession,
        keyword: str,
        year: Optional[int] = None,
        max_results: int = 200,
    ) -> List[Dict]:
        if not ANTHOLOGY_AVAILABLE:
            print("  - [ACL Search] acl-anthology-py not available. Skipping.")
            return []

        anthology = await _get_anthology_singleton()
        if anthology is None:
            print("  - [ACL Search] Anthology instance unavailable. Skipping.")
            return []

        key = (keyword or "").strip().lower()
        if not key:
            return []

        min_year = int(year) if year else 0
        cap = max(1, int(max_results))

        # Iterazione pesante in thread per non bloccare l'event loop
        return await asyncio.to_thread(self._search_sync, anthology, key, min_year, cap)

    # --- worker sincrono ---
    def _search_sync(self, anthology: "Anthology", key: str, min_year: int, cap: int) -> List[Dict]:
        out: List[Dict] = []
        try:
            iterator = anthology.papers()  # <- CORRETTO: è un generatore, non un dict
        except Exception as e:
            print(f"  - [ACL Search] ERROR getting papers iterator: {e}")
            return out

        for paper in iterator:
            if len(out) >= cap:
                break
            try:
                py = _paper_year(paper) or 0
                if py < min_year:
                    continue

                title = _text_or_empty(getattr(paper, "title", None))
                abstract = _text_or_empty(getattr(paper, "abstract", None))
                if key not in title.lower() and key not in abstract.lower():
                    continue

                pid = _paper_id(paper) or ""
                real_doi = _paper_doi(paper)  # può essere None
                publisher_name = _paper_publisher(paper)

                record: Dict = {
                    "id": pid,
                    "title": title or "Untitled",
                    "authors": _authors_as_list(paper),          # ['Alice Rossi', 'Bob Bianchi', ...]
                    "year": py if py else None,
                    "abstract": abstract or None,
                    # Mantieni compatibilità download: se il DOI vero non c'è, usa 'acl:<id>'
                    "doi": real_doi if real_doi else f"acl:{pid}",
                    # Extra: se vuoi mostrare il DOI “vero” in GUI, usa questo campo
                    "external_doi": real_doi,
                    "url": _paper_page_url(paper),
                    "pdf_url": _paper_pdf_url(paper),
                    # IMPORTANTISSIMO: mai None, così sanitize_filename non esplode
                    "publisher": publisher_name,                 # es. "Association for Computational Linguistics" o fallback "ACL Anthology"
                    "source": "ACL",
                    # alcune versioni espongono venue come lista di id; lasciamo stringa se disponibile
                    "venue": getattr(paper, "get_journal_title", lambda: None)() or getattr(paper, "venue", None),
                }
                out.append(record)
            except Exception:
                continue

        print(f"  - [ACL Search] Found {len(out)} results from local data.")
        return out
