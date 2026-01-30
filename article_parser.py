#!/usr/bin/env python3
import os
import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
from collections import defaultdict, OrderedDict
import traceback

# Optional BeautifulSoup
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

class ScientificArticleParser:
    """
    Parses scientific articles from various formats (JSON, XML, HTML, OCR text)
    into a standardized structure with metadata and ordered sections.
    """

    # --- CONSTANTS & REGEX PATTERNS ---
    
    # Patterns to remove from the start of abstracts
    PREFIXES_TO_REMOVE = [
        r'^Abstract\s*:?\s*',
        r'^ABSTRACT\s*:?\s*',
        r'^Summary\s*:?\s*',
        r'^SUMMARY\s*:?\s*',
        r'^Introduction\s*:?\s*',
        r'^INTRODUCTION\s*:?\s*',
        r'^Background\s*:?\s*',
        r'^BACKGROUND\s*:?\s*',
        r'^Objectives?\s*:?\s*',
        r'^OBJECTIVES?\s*:?\s*',
        r'^Purpose\s*:?\s*',
        r'^PURPOSE\s*:?\s*',
        r'^Abstract(Introduction|Background|Objectives?|Purpose|Aims?|Methods?)\s*',
        r'^ABSTRACT(INTRODUCTION|BACKGROUND|OBJECTIVES?|PURPOSE|AIMS?|METHODS?)\s*'
    ]

    # Patterns to identify numbered section headers (e.g., "1. Introduction")
    SECTION_PATTERNS = [
        r'^(\d+\.?\d*\.?\d*)\s+([A-Z][A-Za-z\s\-:,\(\)]+)$',
        r'^#\s*(\d+\.?\d*\.?\d*)\s*([A-Z][A-Za-z\s\-:,\(\)]+)$',
        r'^##\s*(\d+\.?\d*\.?\d*)\s*([A-Z][A-Za-z\s\-:,\(\)]+)$',
        r'^###\s*(\d+\.?\d*\.?\d*)\s*([A-Z][A-Za-z\s\-:,\(\)]+)$',
        r'^(\d+)\s+([A-Z][A-Z\s\-:,\(\)]{5,})$', # All caps title with number
        r'^(\d+\.\d+)\s+([A-Z][A-Za-z\s\-:,\(\)]{5,})$',
    ]

    # Standard unnumbered section headers
    SPECIAL_SECTION_PATTERNS = [
        r'^(Abstract|ABSTRACT)$',
        r'^(Introduction|INTRODUCTION)$',
        r'^(Related Work|RELATED WORK)$',
        r'^(Preliminaries|PRELIMINARIES)$',
        r'^(Background|BACKGROUND)$',
        r'^(Problem Definition|PROBLEM DEFINITION)$',
        r'^(Methods|METHODS|Materials and Methods|MATERIALS AND METHODS|Methodology|METHODOLOGY)$',
        r'^(Results|RESULTS|Experimental Results|EXPERIMENTAL RESULTS|Evaluation|EVALUATION)$',
        r'^(Discussion|DISCUSSION)$',
        r'^(Conclusion|CONCLUSION|Conclusions|CONCLUSIONS)$',
        r'^(Acknowledgments?|ACKNOWLEDGMENTS?)$',
        r'^(References|REFERENCES|Bibliography|BIBLIOGRAPHY)$',
        r'^(Keywords?|Key words?|KEYWORDS?)$',
        r'^(Appendix|APPENDIX)',
        r'^(Data Availability Statement|DATA AVAILABILITY STATEMENT)$',
        r'^(Author Contributions|AUTHOR CONTRIBUTIONS)$',
        r'^(Funding|FUNDING)$',
        r'^(Conflicts of Interest|CONFLICTS OF INTEREST)$',
        r'^(Ethics Statement|ETHICS STATEMENT)$',
        # Technical/Specific Headers
        r'^(Auto-Prep|AUTO-PREP)',
        r'^(Offline Model Training|OFFLINE MODEL TRAINING)',
        r'^(Online Global Graph Search|ONLINE GLOBAL GRAPH SEARCH)',
        r'^(Experimental Evaluation|EXPERIMENTAL EVALUATION)',
        r'^(Transformation Models|TRANSFORMATION MODELS)',
        r'^(Join Models|JOIN MODELS)',
        r'^(Graph Representations|GRAPH REPRESENTATIONS)',
        r'^(Implementation|IMPLEMENTATION)',
        r'^(Experimental Setup|EXPERIMENTAL SETUP)',
        r'^(System Design|SYSTEM DESIGN)',
        r'^(Case Study|CASE STUDY)',
        r'^(Performance Analysis|PERFORMANCE ANALYSIS)',
        r'^(Future Work|FUTURE WORK)',
        r'^(Limitations|LIMITATIONS)',
    ]

    # Lines to completely ignore during parsing (noise, headers, footers)
    SKIP_PATTERNS = [
        r'^\s*$',
        r'^[\*\-=\+]{3,}$',
        r'^Figure \d+',
        r'^Table \d+',
        r'^Supplementary',
        r'^\|.*\|.*\|',
        r'^[:\-\+\=\|]{4,}$',
        r'^\d+\s*\|\s*\d+',
        r'^Algorithm \d+',
        r'^Theorem \d+',
        r'^Proposition \d+',
        r'^Definition \d+',
        r'^Example \d+',
        r'^Lemma \d+',
        r'^Proof',
        r'^\$\$',
        r'^arXiv:',
        r'^doi:',
        r'^\s*\d+\s*$',
        r'^[A-Z\s]{15,}$',
        r'^\(a\)\s|\(b\)\s|\(c\)\s|\(d\)\s',
        r'^[A-Za-z]+\s+\d+:',
        r'^\w+\s+[A-Z][a-z]+\s+[A-Z][a-z]+',
        r'^[A-Z][a-z]+\s+(University|Institute|Research|Laboratory)',
        r'^email:|^Email:|^E-mail:',
        r'^\d{4}\s*$|^\d{1,2}/\d{1,2}/\d{4}',
        r'^Page \d+|^p\. \d+',
        r'^\([^)]){1,5}\)',
        r'^[A-Z]{2,}\s*:',
        r'^https?://',
        r'^www\.',
        r'^\['+'\d+'+'\]',
        r'^[a-z]+@[a-z]+\.',
        r'^\d+\.\d+\.\d+',
        r'^Copyright',
        r'^©',
        r'^\d{4}-\d{4}',
        r'^PVLDB|^VLDB',
        r'^ACM|^IEEE',
        r'^pp\. \d+',
        r'^Vol\. \d+',
        r'^No\. \d+',
        r'^How to cite this article:',
        r'^Received:|^Revised:|^Accepted:|^DOI:',
    ]

    # Lines to exclude from within a valid section's content
    CONTENT_SKIP_PATTERNS = [
        r'^\|.*\|.*\|',
        r'^[:\-\+\=\|]{4,}$',
        r'^\$\$.*\$\$$',
        r'^Algorithm \d+|^Theorem \d+|^Proposition \d+|^Definition \d+|^Example \d+|^Lemma \d+',
        r'^\([a-z]\)\s|\(\d+\)\s',
        r'^Figure \d+|^Table \d+|^Equation \d+',
        r'^\d+\.\s*\[[^\]]+\]',
        r'^\['+'[^\]]+'+'\]\s*\d+',
        r'^Input:|^Output:|^Input\s|^Output\s',
        r'^\d+\s+(foreach|while|if|else|return)',
        r'^[A-Z][a-z]+\s+[A-Z]\.[A-Z]\.',
        r'^\w+\s*=\s*', 
        r'^[a-zA-Z]+\(\s*[a-zA-Z]',
        r'^\d+\s*:\s*', 
        r'^••|^■|^▪',
        r'^arXiv:\d+\.\d+',
        r'^DOI:\s*\d+',
        r'^\d+\s+[a-z]+\s+[a-z]+\s+do$',
        r'^return\s+|^if\s+|^while\s+|^for\s+',
    ]

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.stats = defaultdict(lambda: defaultdict(int))
        self.failed_files = []
        self._last_structured_data = None
        self._collected_dois_data = None

    def load_collected_dois(self) -> Dict[str, Dict]:
        """Load collected_dois.csv for additional authors and keywords"""
        if self._collected_dois_data is not None:
            return self._collected_dois_data
        
        collected_dois_path = self.base_path / "doi_retrieval" / "collected_dois.csv"
        self._collected_dois_data = {}
        
        if not collected_dois_path.exists():
            print(f"WARNING: collected_dois.csv not found: {collected_dois_path}")
            return self._collected_dois_data
        
        try:
            with open(collected_dois_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    doi = row.get('DOI', '').strip()
                    if doi:
                        # Process authors
                        authors_str = row.get('Authors', '').strip()
                        authors = []
                        if authors_str:
                            for delimiter in [';', ',', '|']:
                                if delimiter in authors_str:
                                    authors = [a.strip() for a in authors_str.split(delimiter) if a.strip()]
                                    break
                            if not authors:
                                authors = [authors_str]
                        
                        # Process keywords
                        keywords_str = row.get('Keywords', '').strip()
                        keywords = []
                        if keywords_str:
                            for delimiter in [';', ',', '|']:
                                if delimiter in keywords_str:
                                    keywords = [k.strip() for k in keywords_str.split(delimiter) if k.strip()]
                                    break
                            if not keywords:
                                keywords = [keywords_str]
                        
                        self._collected_dois_data[doi] = {
                            'authors': authors,
                            'title': row.get('Title', '').strip(),
                            'keywords': keywords,
                            'editor': row.get('Editor', '').strip(),
                            'year': row.get('Year', '').strip(),
                            'abstract': row.get('Abstract', '').strip()
                        }
            
            print(f"Loaded collected_dois.csv: {len(self._collected_dois_data)} DOIs found")
            return self._collected_dois_data
            
        except Exception as e:
            print(f"Error loading collected_dois.csv: {e}")
            return {}

    def enhance_with_collected_data(self, result: Dict, doi: str, editor: str) -> Dict:
        """Merge data from collected_dois.csv for ArXiv, Wiley, and ACL"""
        editor_lower = editor.lower()
        if not any(pub in editor_lower for pub in ['arxiv', 'wiley', 'acl', 'anthology']):
            return result
        
        collected_data = self.load_collected_dois()
        if not collected_data or doi not in collected_data:
            return result
        
        collected_info = collected_data[doi]
        print(f"    Merging data from collected_dois.csv for {doi}")
        
        def normalize_authors(authors_list):
            if not authors_list:
                return []
            normalized = []
            for author in authors_list:
                if isinstance(author, dict):
                    name = author.get('name', '') or author.get('author', '') or author.get('given_name', '') or author.get('family_name', '')
                    if name:
                        normalized.append(str(name).strip())
                elif isinstance(author, str):
                    author_clean = author.strip()
                    if author_clean.startswith("{'") and author_clean.endswith("'}"):
                        try:
                            import ast
                            author_dict = ast.literal_eval(author_clean)
                            if isinstance(author_dict, dict):
                                name = author_dict.get('name', '') or author_dict.get('author', '')
                                if name:
                                    normalized.append(str(name).strip())
                                    continue
                        except:
                            pass
                    if author_clean:
                        normalized.append(author_clean)
                else:
                    str_author = str(author).strip()
                    if str_author:
                        normalized.append(str_author)
            return normalized
        
        def normalize_keywords(keywords_list):
            if not keywords_list:
                return []
            normalized = []
            for keyword in keywords_list:
                if isinstance(keyword, (str, int, float)):
                    str_keyword = str(keyword).strip()
                    if str_keyword:
                        normalized.append(str_keyword)
                elif isinstance(keyword, dict):
                    kw_text = keyword.get('keyword', '') or keyword.get('name', '') or str(keyword)
                    if kw_text:
                        normalized.append(str(kw_text).strip())
                else:
                    str_keyword = str(keyword).strip()
                    if str_keyword:
                        normalized.append(str_keyword)
            return normalized
        
        # Merge authors
        existing_authors = normalize_authors(result.get('authors', []))
        collected_authors = normalize_authors(collected_info.get('authors', []))
        
        if not existing_authors and collected_authors:
            result['authors'] = collected_authors
            print(f"    Authors added from collected_dois: {len(collected_authors)}")
        elif existing_authors and collected_authors:
            existing_names_lower = {name.lower().strip() for name in existing_authors}
            combined_authors = existing_authors[:]
            
            for author in collected_authors:
                author_lower = author.lower().strip()
                if author_lower not in existing_names_lower:
                    combined_authors.append(author)
                    existing_names_lower.add(author_lower)
            
            if len(combined_authors) > len(existing_authors):
                result['authors'] = combined_authors
                print(f"    Authors combined: {len(combined_authors)} total")
        
        # Merge keywords
        existing_keywords = normalize_keywords(result.get('keywords', []))
        collected_keywords = normalize_keywords(collected_info.get('keywords', []))
        
        if not existing_keywords and collected_keywords:
            result['keywords'] = collected_keywords
            print(f"    Keywords added from collected_dois: {len(collected_keywords)}")
        elif existing_keywords and collected_keywords:
            existing_set = set(existing_keywords)
            collected_set = set(collected_keywords)
            combined_keywords = list(existing_set.union(collected_set))
            if len(combined_keywords) > len(existing_keywords):
                result['keywords'] = combined_keywords
                print(f"    Keywords combined: {len(combined_keywords)} total")
        
        # Merge title and abstract if missing
        if not result.get('title') and collected_info.get('title'):
            result['title'] = collected_info['title']
            print(f"    Title added from collected_dois")
        
        if not result.get('abstract') and collected_info.get('abstract'):
            result['abstract'] = self._clean_abstract(collected_info['abstract'])
            print(f"    Abstract added from collected_dois")
        
        return result

    def scan_available_queries(self) -> List[str]:
        """Scan available queries"""
        queries = []
        try:
            for item in self.base_path.iterdir():
                if item.is_dir():
                    index_file = item / f"index_{item.name}.csv"
                    if index_file.exists():
                        queries.append(item.name)
        except Exception as e:
            print(f"Error scanning queries: {e}")
        return sorted(queries)

    def parse_index_csv(self, query_name: str) -> List[Dict]:
        """Parse index CSV to get available files"""
        index_file = self.base_path / query_name / f"index_{query_name}.csv"
        if not index_file.exists():
            print(f"Index file not found: {index_file}")
            return []
        
        articles = []
        with open(index_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                articles.append({
                    'doi': row.get('doi', '').strip(),
                    'title': row.get('title', '').strip(),
                    'editor': row.get('editor', '').strip(),
                    'available_formats': row.get('available_formats', '').split(';'),
                    'path_folder': row.get('path_folder', '').strip()
                })
        
        print(f"Found {len(articles)} in index")
        return articles

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text or not isinstance(text, str):
            return ''
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _clean_abstract(self, abstract_text: str) -> str:
        """Cleans the abstract by removing common prefixes and unwanted headers"""
        if not abstract_text or not isinstance(abstract_text, str):
            return ''
        
        cleaned = self._clean_text(abstract_text)
        if not cleaned:
            return ''
        
        for pattern in self.PREFIXES_TO_REMOVE:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
            cleaned = cleaned.strip()
        
        cleaned = re.sub(r'^[:\-\.\,\;\s]+', '', cleaned)
        
        if len(cleaned) < 20:
            return ''
        
        return cleaned.strip()

    def _sanitize_filename(self, doi: str) -> str:
        """Convert DOI into a valid filename"""
        return doi.replace('/', '_').replace(':', '_').replace('.', '_')

    def _clean_section_title(self, title: str) -> str:
        """Clean section titles"""
        if not title:
            return ""
        title = re.sub(r'^\d+(\.\d+)*\.?\s*', '', title)
        title = self._clean_text(title)
        return title.title() if title else ""

    def _normalize_section_title(self, title: str) -> str:
        """Normalize a section title for comparison"""
        if not title:
            return ""
        
        # Remove numbering, spaces, punctuation
        normalized = re.sub(r'^\d+[\.\d]*\s*', '', title.lower())
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized

    def _are_sections_similar(self, title1: str, title2: str, threshold: float = 0.7) -> bool:
        """Determine if two section titles are similar"""
        if not title1 or not title2:
            return False
        
        # Jaccard similarity on words
        words1 = set(title1.split())
        words2 = set(title2.split())
        
        if not words1 or not words2:
            return False
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        similarity = len(intersection) / len(union) if union else 0
        
        return similarity >= threshold

    def _combine_sections_with_order(self, structured_sections: Dict[str, str], raw_sections: Dict[str, str]) -> OrderedDict:
        """Combine sections while preserving the document's logical order"""
        combined = OrderedDict()
        
        # Normalize structured section titles for comparison
        structured_normalized = {}
        for title, content in structured_sections.items():
            normalized_title = self._normalize_section_title(title)
            structured_normalized[normalized_title] = title
        
        # Order map from raw OCR text
        raw_order = {title: idx for idx, title in enumerate(raw_sections.keys())}
        
        # Sections to add with their positions
        sections_to_add = []
        
        # First, add all existing structured sections
        for title, content in structured_sections.items():
            combined[title] = content
        
        # Then, identify new sections from raw OCR
        for raw_title, raw_content in raw_sections.items():
            normalized_raw = self._normalize_section_title(raw_title)
            
            # Check if this section already exists
            found_duplicate = False
            for norm_structured_title in structured_normalized.keys():
                if self._are_sections_similar(normalized_raw, norm_structured_title):
                    found_duplicate = True
                    print(f"    Duplicate section found: '{raw_title}' ≈ '{structured_normalized[norm_structured_title]}'")
                    break
            
            if not found_duplicate:
                # Determine where to insert this section
                raw_position = raw_order[raw_title]
                sections_to_add.append((raw_title, raw_content, raw_position))
                print(f"    Scheduled section to add: '{raw_title}' (pos: {raw_position})")
        
        # Sort sections to add by position
        sections_to_add.sort(key=lambda x: x[2])
        
        # If we have sections to add, rebuild the order
        if sections_to_add:
            final_ordered = OrderedDict()
            structured_titles = list(structured_sections.keys())
            
            for new_title, new_content, new_pos in sections_to_add:
                inserted = False
                
                # Look for structured sections that come after this one in the original text
                for struct_title in structured_titles:
                    # Find the position of this structured section in the raw text
                    struct_pos_in_raw = None
                    for raw_title, raw_pos in raw_order.items():
                        if self._are_sections_similar(
                            self._normalize_section_title(struct_title),
                            self._normalize_section_title(raw_title)
                        ):
                            struct_pos_in_raw = raw_pos
                            break
                    
                    # If this structured section comes after the new section in the text
                    if struct_pos_in_raw and new_pos < struct_pos_in_raw:
                        # Add all structured sections that come before
                        for pre_title in structured_titles:
                            if pre_title not in final_ordered and pre_title != struct_title:
                                pre_pos_in_raw = None
                                for raw_title, raw_pos in raw_order.items():
                                    if self._are_sections_similar(
                                        self._normalize_section_title(pre_title),
                                        self._normalize_section_title(raw_title)
                                    ):
                                        pre_pos_in_raw = raw_pos
                                        break
                                if not pre_pos_in_raw or pre_pos_in_raw < new_pos:
                                    final_ordered[pre_title] = structured_sections[pre_title]
                        
                        # Add the new section
                        final_ordered[new_title] = new_content
                        inserted = True
                        break
                
                # If not inserted, put it at the end
                if not inserted:
                    final_ordered[new_title] = new_content
            
            # Add remaining structured sections
            for struct_title, struct_content in structured_sections.items():
                if struct_title not in final_ordered:
                    final_ordered[struct_title] = struct_content
            
            combined = final_ordered
        
        return combined

    def _load_ocr_sections(self, json_path: Path, query_name: str, publisher: str, doi: str = "") -> Dict[str, str]:
        """Load OCR sections by combining structured.json and raw OCR text for maximum coverage with correct order"""
        try:
            all_sections = OrderedDict()
            
            # First, try structured.json
            structured_sections = self._load_structured_json_sections(json_path, query_name, publisher, doi)
            if structured_sections:
                for title, content in structured_sections.items():
                    all_sections[title] = content
                print(f"    Loaded {len(structured_sections)} sections from structured.json")
            
            # Second fallback: individual markdown files (only if structured is empty)
            if not all_sections:
                markdown_sections = self._load_markdown_sections(json_path, query_name, publisher)
                if markdown_sections:
                    for title, content in markdown_sections.items():
                        all_sections[title] = content
                    print(f"    Loaded {len(markdown_sections)} sections from individual markdown files")
            
            # Always also try raw OCR text for additional sections
            publisher_lower = publisher.lower()
            ocr_supported_publishers = ['wiley', 'mdpi', 'arxiv', 'acl', 'anthology']
            
            if any(pub in publisher_lower for pub in ocr_supported_publishers):
                raw_text_sections = self._load_raw_ocr_sections(json_path, query_name, publisher, doi)
                if raw_text_sections:
                    initial_count = len(all_sections)
                    combined_sections = self._combine_sections_with_order(all_sections, raw_text_sections)
                    new_sections_count = len(combined_sections) - initial_count
                    
                    if new_sections_count > 0:
                        print(f"    Added {new_sections_count} additional sections from raw OCR text")
                    
                    all_sections = combined_sections
                    total_from_raw = len(raw_text_sections)
                    print(f"    Extracted {total_from_raw} total sections from raw OCR text ({new_sections_count} new)")
            else:
                print(f"    Raw OCR extraction not supported for {publisher}")
            
            if all_sections:
                print(f"    FINAL TOTAL: {len(all_sections)} combined sections in correct order")
                print(f"    Final sections: {list(all_sections.keys())}")
            
            return dict(all_sections)
            
        except Exception as e:
            print(f"    Error loading OCR sections: {e}")
            return {}

    def _load_structured_json_sections(self, json_path: Path, query_name: str, publisher: str, doi: str = "") -> Dict[str, str]:
        """Load sections from _structured.json file"""
        try:
            possible_paths = [
                self.base_path / query_name / publisher / "text",
                self.base_path / query_name / publisher,
                self.base_path / publisher / "text",
                self.base_path / publisher,
                json_path.parent,
                self.base_path
            ]
            
            # Specific handling for all publishers
            if 'wiley' in publisher.lower() and doi:
                search_name = self._sanitize_filename(doi)
                print(f"    Wiley search_name: {search_name}")
            elif 'arxiv' in publisher.lower() and doi:
                if doi.startswith('arXiv:'):
                    search_name = doi.replace('arXiv:', 'arXiv_').replace('.', '_')
                else:
                    search_name = self._sanitize_filename(doi)
                print(f"    ArXiv search_name: {search_name}")
            elif ('acl' in publisher.lower() or 'anthology' in publisher.lower()) and doi:
                search_name = self._sanitize_filename(doi)
                print(f"    ACL search_name: {search_name}")
            else:
                search_name = json_path.stem
                print(f"    Default search_name: {search_name}")
            
            structured_patterns = [
                f"{search_name}_structured.json",
                f"{search_name.replace('_comprehensive', '')}_structured.json",
                f"{search_name.replace('_', '-')}_structured.json",
                f"{search_name.replace('-', '_')}_structured.json"
            ]
            
            # Debug: show what we're searching for
            print(f"    Searching for structured files with patterns: {structured_patterns}")
            
            for base_path in possible_paths:
                if not base_path.exists():
                    continue
                
                print(f"    Checking path: {base_path}")
                
                for pattern in structured_patterns:
                    structured_file = base_path / pattern
                    print(f"    Trying file: {structured_file}")
                    
                    if structured_file.exists():
                        print(f"    FOUND structured.json: {structured_file}")
                        
                        with open(structured_file, 'r', encoding='utf-8') as f:
                            structured_data = json.load(f)
                        
                        self._last_structured_data = structured_data
                        sections = OrderedDict()
                        structured_sections = structured_data.get('sections', {})
                        
                        print(f"    Sections found in file: {list(structured_sections.keys())}")
                        
                        if not structured_sections:
                            continue
                        
                        for section_id, section_data in structured_sections.items():
                            if isinstance(section_data, dict):
                                title = section_data.get('title', section_id)
                                content = section_data.get('content', '')
                                if title and content:
                                    clean_title = self._clean_section_title(title)
                                    if clean_title:
                                        sections[clean_title] = self._clean_text(content)
                                        print(f"    Section extracted: {clean_title}")
                            elif isinstance(section_data, str):
                                clean_title = self._clean_section_title(section_id)
                                if clean_title:
                                    sections[clean_title] = self._clean_text(section_data)
                                    print(f"    Section extracted: {clean_title}")
                        
                        print(f"    Total sections extracted: {len(sections)}")
                        return dict(sections)
            
            print(f"    No structured.json file found for {search_name}")
            return {}
            
        except Exception as e:
            print(f"    Error reading structured.json: {e}")
            return {}

    def _load_markdown_sections(self, json_path: Path, query_name: str, publisher: str) -> Dict[str, str]:
        """FALLBACK: Load sections from individual markdown files"""
        try:
            possible_paths = [
                self.base_path / query_name / publisher / "text" / "sections",
                self.base_path / publisher / "text" / "sections",
                self.base_path / "text" / "sections"
            ]
            
            ocr_sections_path = None
            for path in possible_paths:
                if path.exists():
                    ocr_sections_path = path
                    break
            
            if not ocr_sections_path:
                return {}
            
            json_filename = json_path.stem
            article_folder = None
            best_match_score = 0
            
            for folder in ocr_sections_path.iterdir():
                if not folder.is_dir():
                    continue
                
                folder_name = folder.name
                match_score = 0
                
                if json_filename == folder_name:
                    article_folder = folder
                    break
                
                if json_filename in folder_name or folder_name in json_filename:
                    match_score = max(len(json_filename), len(folder_name))
                
                if publisher.lower() == 'arxiv':
                    arxiv_pattern = r'(\d{4})[\._](\d{4,5})'
                    json_match = re.search(arxiv_pattern, json_filename)
                    folder_match = re.search(arxiv_pattern, folder_name)
                    if json_match and folder_match and json_match.groups() == folder_match.groups():
                        match_score = 1000
                
                if match_score > best_match_score:
                    best_match_score = match_score
                    article_folder = folder
            
            if not article_folder:
                return {}
            
            sections = {}
            md_files = list(article_folder.glob("*.md"))
            
            for md_file in md_files:
                try:
                    with open(md_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    section_name = md_file.stem
                    section_name = re.sub(r'^[a-z]+_|^\d+_|^[ivxlc]+_', '', section_name, flags=re.IGNORECASE)
                    section_name = section_name.replace('_', ' ').title()
                    
                    if content.strip():
                        sections[section_name] = self._clean_text(content)
                except Exception:
                    continue
            
            return sections
            
        except Exception as e:
            print(f"    Error reading markdown sections: {e}")
            return {}

    def _load_raw_ocr_sections(self, json_path: Path, query_name: str, publisher: str, doi: str = "") -> Dict[str, str]:
        """Extract sections from raw OCR files (markdown or txt) - improved version"""
        try:
            possible_paths = [
                self.base_path / query_name / publisher / "text",
                self.base_path / query_name / publisher,
                self.base_path / publisher / "text", 
                self.base_path / publisher,
                json_path.parent,
                self.base_path
            ]
            
            # Determine base filename
            if 'wiley' in publisher.lower() and doi:
                search_name = self._sanitize_filename(doi)
            elif 'arxiv' in publisher.lower() and doi:
                if doi.startswith('arXiv:'):
                    search_name = doi.replace('arXiv:', 'arXiv_').replace('.', '_')
                else:
                    search_name = self._sanitize_filename(doi)
            else:
                search_name = json_path.stem
            
            # Patterns to locate OCR files
            ocr_patterns = [
                f"{search_name}_olmocr.md",
                f"{search_name}_olmocr.txt", 
                f"{search_name}_ocr.md",
                f"{search_name}_ocr.txt",
                f"{search_name}.md",
                f"{search_name}.txt"
            ]
            
            # Find OCR file
            ocr_file = None
            for base_path in possible_paths:
                if not base_path.exists():
                    continue
                
                for pattern in ocr_patterns:
                    potential_file = base_path / pattern
                    if potential_file.exists():
                        ocr_file = potential_file
                        print(f"    Found raw OCR file: {ocr_file}")
                        break
                
                if ocr_file:
                    break
            
            if not ocr_file:
                print(f"    No raw OCR file found for {search_name}")
                return {}
            
            # Read and parse content
            with open(ocr_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return self._parse_sections_from_raw_text(content)
            
        except Exception as e:
            print(f"    Error reading raw OCR file: {e}")
            return {}

    def _parse_sections_from_raw_text(self, content: str) -> Dict[str, str]:
        """Parse sections from raw OCR text - improved with ordering"""
        try:
            sections = OrderedDict()
            lines = content.split('\n')
            current_section = None
            current_content = []
            
            in_table_block = False
            table_line_count = 0
            in_algorithm_block = False
            in_bibliography = False
            in_references = False
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Detect start of References/Bibliography section
                if re.match(r'^(References|REFERENCES|Bibliography|BIBLIOGRAPHY)', line):
                    in_references = True
                    # Save previous section
                    if current_section and current_content:
                        filtered_content = self._filter_section_content(current_content, self.CONTENT_SKIP_PATTERNS)
                        if filtered_content:
                            content_text = '\n'.join(filtered_content).strip()
                            if self._is_valid_section_content(content_text):
                                sections[current_section] = self._clean_text(content_text)
                    
                    current_section = "References"
                    current_content = []
                    continue
                
                # If in References, keep adding until the next main section
                if in_references:
                    # Check if a new main section starts
                    new_section_found = False
                    for pattern in self.SECTION_PATTERNS:
                        if re.match(pattern, line):
                            new_section_found = True
                            break
                    
                    if not new_section_found:
                        for pattern in self.SPECIAL_SECTION_PATTERNS:
                            if re.match(pattern, line, re.IGNORECASE) and not line.lower().startswith('ref'):
                                new_section_found = True
                                break
                    
                    if new_section_found:
                        # Save References and start a new section
                        if current_section == "References" and current_content:
                            refs_content = [l for l in current_content if len(l) > 10][:50]
                            if refs_content:
                                sections[current_section] = '\n'.join(refs_content)
                        in_references = False
                    else:
                        # Continue adding to References
                        if len(line) > 10 and not any(re.match(p, line) for p in self.SKIP_PATTERNS):
                            current_content.append(line)
                        continue
                
                # Detect algorithm block start
                if re.match(r'^Algorithm \d+', line):
                    in_algorithm_block = True
                    continue
                
                # If in an algorithm block, look for the end
                if in_algorithm_block:
                    if any(re.match(pattern, line) for pattern in self.SECTION_PATTERNS + [p for p in self.SPECIAL_SECTION_PATTERNS]):
                        in_algorithm_block = False
                    else:
                        continue
                
                # Detect table block start
                if re.match(r'^\|.*\|.*\|', line):
                    in_table_block = True
                    table_line_count = 0
                    continue
                
                # If in a table block
                if in_table_block:
                    table_line_count += 1
                    if not re.match(r'^\|.*\|.*\||[:\-\+\=\|]{3,}', line):
                        if table_line_count > 5:
                            continue
                        else:
                            in_table_block = False
                    else:
                        continue
                
                # Skip global skip patterns
                if any(re.match(pattern, line) for pattern in self.SKIP_PATTERNS):
                    continue
                
                # Check for a new numbered section
                section_match = None
                for pattern in self.SECTION_PATTERNS:
                    match = re.match(pattern, line)
                    if match:
                        section_match = match
                        break
                
                # Check special sections
                if not section_match:
                    for pattern in self.SPECIAL_SECTION_PATTERNS:
                        match = re.match(pattern, line, re.IGNORECASE)
                        if match:
                            section_match = (None, match.group(1))
                            break
                
                if section_match:
                    # Save previous section if exists
                    if current_section and current_content:
                        filtered_content = self._filter_section_content(current_content, self.CONTENT_SKIP_PATTERNS)
                        
                        if filtered_content:
                            content_text = '\n'.join(filtered_content).strip()
                            if self._is_valid_section_content(content_text):
                                sections[current_section] = self._clean_text(content_text)
                    
                    # Start new section
                    if isinstance(section_match, tuple):
                        current_section = section_match[1].strip()
                    else:
                        section_title = section_match.group(2).strip()
                        current_section = section_title
                    
                    current_content = []
                    continue
                
                # If inside a section, add content
                if current_section:
                    if self._is_valid_content_line(line):
                        current_content.append(line)
            
            # Save last section
            if current_section and current_content:
                if current_section == "References":
                    refs_content = [l for l in current_content if len(l) > 10][:50]
                    if refs_content:
                        sections[current_section] = '\n'.join(refs_content)
                else:
                    filtered_content = self._filter_section_content(current_content, self.CONTENT_SKIP_PATTERNS)
                    if filtered_content:
                        content_text = '\n'.join(filtered_content).strip()
                        if self._is_valid_section_content(content_text):
                            sections[current_section] = self._clean_text(content_text)
            
            # Clean section names
            cleaned_sections = OrderedDict()
            for section_name, section_content in sections.items():
                clean_name = re.sub(r'^\d+\.?\d*\.?\d*\s*', '', section_name)
                clean_name = self._clean_section_title(clean_name)
                
                # Avoid duplicating the abstract
                if clean_name.lower() == 'abstract':
                    continue
                    
                if clean_name and section_content:
                    # Limit length for non-References sections
                    if clean_name.lower() != 'references':
                        cleaned_sections[clean_name] = section_content[:4000]
                    else:
                        cleaned_sections[clean_name] = section_content[:8000]
            
            print(f"    Extracted {len(cleaned_sections)} sections from raw text: {list(cleaned_sections.keys())}")
            return dict(cleaned_sections)
            
        except Exception as e:
            print(f"    Error parsing sections from raw text: {e}")
            return {}

    def _filter_section_content(self, content_lines: List[str], skip_patterns: List[str]) -> List[str]:
        """Filter section content by removing undesirable patterns"""
        filtered_content = []
        for content_line in content_lines:
            if not any(re.match(pattern, content_line) for pattern in skip_patterns):
                cleaned_line = content_line.strip()
                if (len(cleaned_line) > 15 and
                    not cleaned_line.isdigit() and
                    not re.match(r'^[A-Z\s]+$', cleaned_line) and
                    cleaned_line.count('|') < 3 and
                    not re.match(r'^\d+\s*$', cleaned_line) and
                    not re.match(r'^[().\-\s]+$', cleaned_line)):
                    filtered_content.append(cleaned_line)
        return filtered_content

    def _is_valid_content_line(self, line: str) -> bool:
        """Check if a line is valid to be included as section content"""
        return (len(line) > 8 and len(line) < 800 and
                not any(keyword in line.lower() for keyword in 
                       ['page', 'doi:', 'journal', 'volume', 'copyright', '©', 'arxiv:', 'email', '@',
                        'university', 'institute', 'laboratory', 'department']) and
                not re.match(r'^\d+$', line) and
                not re.match(r'^[A-Z\s]{10,}$', line) and
                not line.startswith('http') and
                line.count('|') < 2 and
                not re.match(r'^\([a-z]\)\s*$', line) and
                not re.match(r'^\d+\.\s*$', line))

    def _is_valid_section_content(self, content: str) -> bool:
        """Check if section content is valid (substantial narrative text)"""
        return (len(content) > 100 and
                content.count(' ') > 20 and
                content.count('.') > 2 and
                not content.startswith('|') and
                not re.match(r'^[\d\s\|\-\+\=:]+$', content) and
                len(content.split()) > 25)

    def safe_to_list(self, obj):
        """Safely convert an object to a list"""
        if obj is None:
            return []
        elif isinstance(obj, (list, tuple)):
            return list(obj)
        elif isinstance(obj, str):
            return [obj]
        elif hasattr(obj, '__iter__'):
            try:
                return list(obj)
            except:
                return [str(obj)]
        else:
            return [str(obj)]

    def parse_arxiv_json(self, json_path: Path, query_name: str) -> Dict:
        """Parse ArXiv JSON with OCR sections support + collected_dois.csv integration"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            result = {
                'title': self._clean_text(data.get('title', '')),
                'authors': data.get('authors', []) if isinstance(data.get('authors', []), list) else [],
                'abstract': self._clean_abstract(data.get('summary', '')),
                'keywords': [data.get('id', '').split('.')[0]] if '.' in data.get('id', '') else [],
                'sections': {},
                'doi': data.get('doi', ''),
                'editor': 'ArXiv'
            }
            
            if result['authors']:
                clean_authors = []
                for author in result['authors']:
                    if isinstance(author, dict):
                        name = author.get('name', '') or author.get('author', '') or author.get('given_name', '') or author.get('family_name', '')
                        if name:
                            clean_authors.append(str(name).strip())
                    elif isinstance(author, str) and author.strip():
                        clean_authors.append(author.strip())
                    elif author:
                        clean_authors.append(str(author).strip())
                result['authors'] = clean_authors
            if result['keywords']:
                result['keywords'] = [str(kw) for kw in result['keywords'] if kw]
            
            result = self.enhance_with_collected_data(result, result.get('doi', ''), 'ArXiv')
            return result
        except Exception as e:
            print(f"ArXiv error: {e}")
            return {}

    def parse_wiley_json(self, json_path: Path, query_name: str) -> Dict:
        """Parse Wiley TDM JSON with OCR sections support + collected_dois.csv integration"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'Wiley'
            }
            
            if 'items' in data and data['items']:
                item = data['items'][0]
                result['title'] = self._clean_text(item.get('title', ''))
                result['doi'] = item.get('doi', '')
                result['abstract'] = self._clean_abstract(item.get('abstract', ''))
                
                keywords = item.get('keywords', [])
                if keywords:
                    result['keywords'] = [self._clean_text(str(k)) for k in keywords if k]
                
                contributors = item.get('contributors', {}).get('authors', [])
                if contributors:
                    for author in contributors:
                        if isinstance(author, dict):
                            given = author.get('givenNames', '')
                            family = author.get('familyName', '')
                            full_name = f"{given} {family}".strip()
                            if full_name:
                                result['authors'].append(full_name)
                        elif isinstance(author, str) and author.strip():
                            result['authors'].append(author.strip())
            
            result = self.enhance_with_collected_data(result, result.get('doi', ''), 'Wiley')
            return result
        except Exception as e:
            print(f" Wiley error: {e}")
            return {}

    def parse_acl_json(self, json_path: Path, query_name: str) -> Dict:
        """Parse ACL Anthology JSON with OCR sections support + collected_dois.csv integration"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'ACL_Anthology'
            }
            
            result['title'] = self._clean_text(data.get('title', ''))
            result['doi'] = data.get('doi', '') or data.get('url', '')
            result['abstract'] = self._clean_abstract(data.get('abstract', ''))
            
            authors = data.get('authors', []) or data.get('author', [])
            if authors:
                for author in authors:
                    if isinstance(author, dict):
                        name = author.get('name', '') or f"{author.get('first', '')} {author.get('last', '')}".strip()
                    else:
                        name = str(author)
                    if name:
                        result['authors'].append(self._clean_text(name))
            
            keywords = data.get('keywords', []) or data.get('topics', [])
            if keywords:
                result['keywords'] = [self._clean_text(str(k)) for k in keywords if k]
            
            result = self.enhance_with_collected_data(result, result.get('doi', ''), 'ACL_Anthology')
            return result
        except Exception as e:
            print(f"ACL error: {e}")
            return {}

    def parse_springer_json(self, json_path: Path) -> Dict:
        """Parse Springer JSON with better sections extraction"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'Springer'
            }
            
            if 'title' in data and 'sections' in data and isinstance(data['sections'], dict):
                result['title'] = self._clean_text(data.get('title', ''))
                result['abstract'] = self._clean_abstract(data.get('abstract', ''))
                result['doi'] = data.get('doi', '')
                
                authors = self.safe_to_list(data.get('authors', []))
                for author in authors:
                    if isinstance(author, dict):
                        name = author.get('name', str(author))
                    else:
                        name = str(author)
                    if name:
                        result['authors'].append(self._clean_text(name))
                
                keywords = self.safe_to_list(data.get('keywords', []))
                for kw in keywords:
                    if isinstance(kw, str) and kw.strip():
                        result['keywords'].append(self._clean_text(kw))
                
                sections = data.get('sections', {})
                if isinstance(sections, dict):
                    for section_name, section_content in sections.items():
                        if section_name and section_content:
                            clean_name = self._clean_text(section_name)
                            clean_content = self._clean_text(str(section_content))
                            if clean_name and clean_content:
                                result['sections'][clean_name] = clean_content
                
                return result
            
            if 'records' in data and data['records']:
                record = data['records'][0]
                result['title'] = self._clean_text(record.get('title', ''))
                
                creators = self.safe_to_list(record.get('creators', []))
                result['authors'] = [self._clean_text(str(c.get('creator', c) if isinstance(c, dict) else c)) 
                                   for c in creators if c]
                
                keywords = self.safe_to_list(record.get('keyword', []))
                result['keywords'] = [self._clean_text(str(k)) for k in keywords if k]
                
                result['abstract'] = self._clean_abstract(record.get('abstract', ''))
                result['doi'] = record.get('doi', '')
                
                if 'sections' in record and isinstance(record['sections'], dict):
                    for sec_name, sec_content in record['sections'].items():
                        if sec_name and sec_content:
                            result['sections'][self._clean_text(sec_name)] = self._clean_text(str(sec_content))
            
            elif 'response' in data and data['response'].get('docs'):
                doc = data['response']['docs'][0]
                
                title = doc.get('title', '')
                title_list = self.safe_to_list(title)
                result['title'] = self._clean_text(str(title_list[0]) if title_list else '')
                
                creators = self.safe_to_list(doc.get('creators', []))
                result['authors'] = [self._clean_text(str(a)) for a in creators if a]
                
                keywords = self.safe_to_list(doc.get('keyword', []))
                result['keywords'] = [self._clean_text(str(k)) for k in keywords if k]
                
                result['abstract'] = self._clean_abstract(doc.get('abstract', ''))
                result['doi'] = doc.get('doi', '')
            
            return result
        except Exception as e:
            print(f"Springer error: {e}")
            return {}

    def parse_elsevier_json(self, json_path: Path) -> Dict:
        """Parse Elsevier JSON with correct abstract extraction"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            result = {
                'title': '', 
                'authors': [], 
                'keywords': [], 
                'abstract': '', 
                'sections': {},
                'doi': '',
                'editor': 'Elsevier'
            }
            
            if 'full-text-retrieval-response' in data:
                response = data['full-text-retrieval-response']
                coredata = response.get('coredata', {})
                
                result['title'] = self._clean_text(coredata.get('dc:title', ''))
                result['doi'] = coredata.get('prism:doi', '') or coredata.get('dc:identifier', '')
                
                creators = coredata.get('dc:creator', [])
                if creators:
                    if isinstance(creators, list):
                        result['authors'] = [self._clean_text(str(c)) for c in creators if c]
                    else:
                        result['authors'] = [self._clean_text(str(creators))]
                
                subjects = coredata.get('dcterms:subject', [])
                if subjects:
                    subject_list = subjects if isinstance(subjects, list) else [subjects]
                    for subj in subject_list:
                        if isinstance(subj, dict):
                            kw_text = subj.get('$', '') or subj.get('text', '') or subj.get('value', '') or str(subj)
                        else:
                            kw_text = str(subj)
                        
                        if kw_text and isinstance(kw_text, str) and len(kw_text.strip()) > 0:
                            clean_kw = self._clean_text(kw_text)
                            if clean_kw:
                                result['keywords'].append(clean_kw)
                
                result['abstract'] = self._clean_abstract(coredata.get('dc:description', ''))
                
                original_text = response.get('originalText', '')
                if original_text:
                    pattern = r'<xocs:item-toc-section-title[^>]*>([^<]+)</xocs:item-toc-section-title>'
                    section_matches = re.findall(pattern, original_text)
                    
                    if section_matches:
                        for section_title in section_matches:
                            clean_title = self._clean_text(section_title)
                            if clean_title and len(clean_title) > 2:
                                result['sections'][clean_title] = ""
            
            elif 'search-results' in data:
                search_results = data['search-results']
                if search_results.get('entry'):
                    entry = search_results['entry'][0]
                    result['title'] = self._clean_text(entry.get('dc:title', ''))
                    result['doi'] = entry.get('prism:doi', '')
                    
                    authors = entry.get('author', [])
                    if authors:
                        author_names = []
                        for a in authors:
                            if isinstance(a, dict):
                                name = a.get('authname', str(a))
                            else:
                                name = str(a)
                            if name:
                                author_names.append(self._clean_text(name))
                        result['authors'] = author_names
                    
                    result['abstract'] = self._clean_abstract(entry.get('dc:description', ''))
            
            return result
            
        except Exception as e:
            print(f"Elsevier JSON error: {e}")
            return {}

    def parse_elsevier_xml(self, xml_path: Path) -> Dict:
        """Parse Elsevier XML - complete sections extraction"""
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not ('<' in content and content.strip()):
                return {}
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'Elsevier'
            }
            
            if BS4_AVAILABLE:
                soup = BeautifulSoup(content, 'xml')
                
                title_elem = soup.find(['ce:title', 'title', 'ce:article-title']) 
                if title_elem:
                    result['title'] = self._clean_text(title_elem.get_text())
                
                author_groups = soup.find_all('ce:author-group')
                for group in author_groups:
                    for author in group.find_all('ce:author'):
                        given = author.find('ce:given-name')
                        surname = author.find('ce:surname')
                        if given and surname:
                            full_name = f"{given.get_text().strip()} {surname.get_text().strip()}"
                            result['authors'].append(full_name)
                        elif surname:
                            result['authors'].append(surname.get_text().strip())
                
                keywords_container = soup.find('ce:keywords')
                if keywords_container:
                    for keyword in keywords_container.find_all('ce:keyword'):
                        kw_text_elem = keyword.find('ce:text')
                        if kw_text_elem:
                            result['keywords'].append(self._clean_text(kw_text_elem.get_text()))
                
                abstract_elem = soup.find(['ce:abstract', 'abstract'])
                if abstract_elem:
                    abstract_paras = abstract_elem.find_all(['ce:simple-para', 'ce:para', 'p'])
                    if abstract_paras:
                        abstract_text = ' '.join([p.get_text() for p in abstract_paras])
                    else:
                        abstract_text = abstract_elem.get_text()
                    result['abstract'] = self._clean_abstract(abstract_text)
                
                sections_container = soup.find('ce:sections')
                if sections_container:
                    for section in sections_container.find_all('ce:section', recursive=False):
                        title_elem = section.find('ce:section-title')
                        if title_elem:
                            section_title = self._clean_text(title_elem.get_text())
                            title_elem_copy = title_elem.extract()
                            section_content = self._clean_text(section.get_text())
                            
                            if section_title and len(section_title) > 2:
                                result['sections'][section_title] = section_content[:1000]
                
                doi_elem = soup.find('ce:doi') or soup.find(['prism:doi', 'dc:identifier'])
                if doi_elem:
                    result['doi'] = doi_elem.get_text().strip()
            
            return result
        except Exception as e:
            print(f"Elsevier XML error: {e}")
            return {}

    def parse_springer_xml(self, xml_path: Path) -> Dict:
        """Parse Springer XML - similar to Elsevier XML but for Springer"""
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not ('<' in content and content.strip()):
                return {}
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'Springer'
            }
            
            if BS4_AVAILABLE:
                soup = BeautifulSoup(content, 'xml')
                
                title_elem = (soup.find('ArticleTitle') or soup.find('ChapterTitle') or 
                             soup.find('title') or soup.find('dc:title'))
                if title_elem:
                    result['title'] = self._clean_text(title_elem.get_text())
                
                authors = []
                for author_group in soup.find_all('AuthorGroup'):
                    for author in author_group.find_all('Author'):
                        given = author.find('GivenName')
                        family = author.find('FamilyName')
                        if given and family:
                            authors.append(f"{given.get_text().strip()} {family.get_text().strip()}")
                        elif family:
                            authors.append(family.get_text().strip())
                
                if not authors:
                    for creator in soup.find_all('dc:creator'):
                        if creator.get_text():
                            authors.append(self._clean_text(creator.get_text()))
                
                result['authors'] = authors
                
                keywords = []
                for keyword_elem in soup.find_all(['Keyword', 'keyword']):
                    if keyword_elem.get_text():
                        keywords.append(self._clean_text(keyword_elem.get_text()))
                
                if not keywords:
                    for subject in soup.find_all('dc:subject'):
                        if subject.get_text():
                            keywords.append(self._clean_text(subject.get_text()))
                
                result['keywords'] = keywords
                
                abstract_elem = (soup.find('AbstractSection') or soup.find('Abstract') or soup.find('abstract'))
                if abstract_elem:
                    result['abstract'] = self._clean_abstract(abstract_elem.get_text())
                
                sections = {}
                for section in soup.find_all('Section'):
                    title_elem = section.find('SectionTitle')
                    if title_elem:
                        section_title = self._clean_text(title_elem.get_text())
                        if section_title and len(section_title) > 2:
                            title_elem.extract()
                            section_content = self._clean_text(section.get_text())
                            if section_content:
                                sections[section_title] = section_content[:1000]
                
                result['sections'] = sections
                
                doi_elem = (soup.find('ArticleDOI') or soup.find('ChapterDOI') or 
                           soup.find('doi') or soup.find('dc:identifier'))
                if doi_elem:
                    result['doi'] = doi_elem.get_text().strip()
            
            return result
        except Exception as e:
            print(f"Springer XML error: {e}")
            return {}

    def parse_mdpi_html(self, html_path: Path) -> Dict:
        """Parse MDPI HTML with sections"""
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            result = {
                'title': '',
                'authors': [],
                'keywords': [],
                'abstract': '',
                'sections': {},
                'doi': '',
                'editor': 'MDPI'
            }
            
            if BS4_AVAILABLE:
                soup = BeautifulSoup(content, 'html.parser')
                
                title_elem = soup.find('meta', {'name': 'citation_title'}) or soup.find('h1')
                if title_elem:
                    result['title'] = self._clean_text(title_elem.get('content') if title_elem.name == 'meta' else title_elem.get_text())
                
                for meta in soup.find_all('meta', {'name': 'citation_author'}):
                    author = meta.get('content', '')
                    if author:
                        result['authors'].append(self._clean_text(author))
                
                kw_meta = soup.find('meta', {'name': 'keywords'})
                if kw_meta:
                    keywords_text = kw_meta.get('content', '')
                    result['keywords'] = [self._clean_text(kw) for kw in re.split(r'[;,]', keywords_text) if kw.strip()]
                
                abstract_elem = soup.find(['div', 'section'], class_=re.compile(r'abstract', re.I))
                if abstract_elem:
                    abstract_text = abstract_elem.get_text()
                    result['abstract'] = self._clean_abstract(abstract_text)
                
                for i, heading in enumerate(soup.find_all(['h2', 'h3'])):
                    heading_text = self._clean_text(heading.get_text())
                    if heading_text and len(heading_text) > 3:
                        result['sections'][heading_text] = ""
                
                doi_elem = soup.find('meta', {'name': 'citation_doi'})
                if doi_elem:
                    result['doi'] = doi_elem.get('content', '')
            
            return result
        except Exception as e:
            print(f"MDPI error: {e}")
            return {}

    def find_file_with_pattern(self, folder: Path, pattern: str) -> Optional[Path]:
        """Find a file that contains the pattern in its name"""
        if not folder.exists():
            return None
        
        for ext in ['.json', '.xml', '.html']:
            exact_file = folder / f"{pattern}{ext}"
            if exact_file.exists():
                return exact_file
        
        for file in folder.iterdir():
            if pattern in file.name:
                return file
        
        return None

    def parse_article(self, article_info: Dict, query_name: str) -> Dict:
        """Parse a single article with enhanced OCR support"""
        doi = article_info['doi']
        editor = article_info['editor'].lower()
        path_folder = article_info['path_folder']
        available_formats = article_info['available_formats']
        
        print(f"  Processing {doi} ({article_info['editor']}) - {available_formats}")
        
        base_path = self.base_path / query_name / path_folder
        filename_base = doi.replace('/', '_').replace(':', '_').replace('.', '_')
        
        result = {
            'doi': doi, 'title': article_info.get('title', ''), 'editor': article_info['editor'],
            'authors': [], 'keywords': [], 'abstract': '', 'sections': {},
            'source_files': available_formats, 'parsing_success': False
        }
        
        if not base_path.exists():
            print(f"    Base path not found: {base_path}")
            return result
        
        # Load OCR sections with the correct filename
        publisher_lower = path_folder.lower()
        ocr_supported_publishers = ['wiley', 'mdpi', 'arxiv', 'acl', 'anthology']
        
        print(f"    Publisher: {path_folder} | OCR supported: {any(pub in publisher_lower for pub in ocr_supported_publishers)}")
        
        if any(pub in publisher_lower for pub in ocr_supported_publishers):
            correct_json_name = f"{filename_base}.json"
            correct_json_path = base_path / correct_json_name
            
            print(f"    Looking for OCR sections for: {correct_json_name}")
            
            ocr_sections = self._load_ocr_sections(correct_json_path, query_name, path_folder, doi)
            if ocr_sections:
                print(f"    Found {len(ocr_sections)} OCR sections: {list(ocr_sections.keys())}")
            else:
                print(f"    No OCR sections found")
        else:
            ocr_sections = {}
            print(f"    OCR not supported for publisher: {path_folder}")
        
        parsers = []
        
        # Special handling for Wiley
        if 'wiley' in editor:
            collected_data = self.load_collected_dois()
            if collected_data and doi in collected_data:
                collected_info = collected_data[doi]
                
                wiley_result = {
                    'title': collected_info.get('title', ''),
                    'authors': collected_info.get('authors', []),
                    'keywords': collected_info.get('keywords', []),
                    'abstract': self._clean_abstract(collected_info.get('abstract', '')),
                    'sections': {},
                    'doi': doi,
                    'editor': 'Wiley'
                }
                
                if wiley_result['title'] or wiley_result['authors'] or wiley_result['abstract']:
                    def create_wiley_collected_parser(data):
                        return lambda: data
                    
                    parsers.append(('wiley_collected_dois', create_wiley_collected_parser(wiley_result)))
            
            if 'json' in available_formats:
                json_folder = base_path / 'json'
                json_file = self.find_file_with_pattern(json_folder, filename_base)
                if json_file:
                    parsers.append(('wiley_json', lambda f=json_file, q=query_name: self.parse_wiley_json(f, q)))
        
        # Other publishers
        elif 'elsevier' in editor:
            if 'xml' in available_formats:
                xml_folder = base_path / 'xml'
                xml_file = self.find_file_with_pattern(xml_folder, filename_base)
                if xml_file:
                    parsers.append(('elsevier_xml', lambda f=xml_file: self.parse_elsevier_xml(f)))
            
            if 'json' in available_formats and not parsers:
                json_folder = base_path / 'json'
                json_file = self.find_file_with_pattern(json_folder, filename_base)
                if json_file:
                    parsers.append(('elsevier_json', lambda f=json_file: self.parse_elsevier_json(f)))
        
        else:
            if 'json' in available_formats:
                json_folder = base_path / 'json'
                json_file = self.find_file_with_pattern(json_folder, filename_base)
                
                if json_file:
                    if 'arxiv' in editor:
                        parsers.append(('arxiv_json', lambda f=json_file, q=query_name: self.parse_arxiv_json(f, q)))
                    elif 'springer' in editor:
                        parsers.append(('springer_json', lambda f=json_file: self.parse_springer_json(f)))
                    elif 'acl' in editor.lower() or 'anthology' in editor.lower():
                        parsers.append(('acl_json', lambda f=json_file, q=query_name: self.parse_acl_json(f, q)))
            
            if 'springer' in editor and not parsers:
                json_folder = base_path / 'json'
                json_file = self.find_file_with_pattern(json_folder, filename_base)
                if json_file:
                    parsers.append(('springer_json', lambda f=json_file: self.parse_springer_json(f)))
            
            if 'xml' in available_formats and 'springer' not in editor:
                xml_folder = base_path / 'xml'
                xml_file = self.find_file_with_pattern(xml_folder, filename_base)
                
                if xml_file and 'mdpi' in editor:
                    parsers.append(('mdpi_html', lambda f=xml_file: self.parse_mdpi_html(f)))
        
        # Execute main parsers
        parsing_success = False
        for parser_name, parser_func in parsers:
            try:
                print(f"    Trying parser: {parser_name}")
                parsed_data = parser_func()
                
                if parsed_data and (parsed_data.get('title') or parsed_data.get('authors') or parsed_data.get('abstract')):
                    result.update(parsed_data)
                    result['editor'] = article_info['editor']
                    parsing_success = True
                    print(f"    {parser_name}: extracted main metadata")
                    break
            except Exception as e:
                print(f"    Parser {parser_name} failed: {e}")
                continue
        
        # Special case for Wiley without metadata
        if not parsing_success and 'wiley' in editor and ocr_sections:
            collected_data = self.load_collected_dois()
            if collected_data and doi in collected_data:
                collected_info = collected_data[doi]
                if collected_info.get('title') or collected_info.get('authors'):
                    result['title'] = collected_info.get('title', '')
                    result['authors'] = collected_info.get('authors', [])
                    result['keywords'] = collected_info.get('keywords', [])
                    result['abstract'] = self._clean_abstract(collected_info.get('abstract', ''))
                    parsing_success = True
        
        # Merge OCR sections (always)
        if ocr_sections:
            result['sections'].update(ocr_sections)
            print(f"    Merged {len(ocr_sections)} OCR sections")
        
        # Merge data from collected_dois.csv
        if parsing_success:
            result = self.enhance_with_collected_data(result, doi, article_info['editor'])
        
        # Update statistics
        if parsing_success:
            result['parsing_success'] = True
            self.stats[query_name]['total_parsed'] += 1
            if result['authors']: self.stats[query_name]['authors_extracted'] += 1
            if result['keywords']: self.stats[query_name]['keywords_extracted'] += 1
            if result['abstract']: self.stats[query_name]['abstracts_extracted'] += 1
            if result['sections']: self.stats[query_name]['sections_extracted'] += 1
        else:
            self.stats[query_name]['parsing_failed'] += 1
            self.failed_files.append(f"{doi} ({article_info['editor']})")
            print(f"    Parsing failed for {doi}")
        
        return result

    def parse_query(self, query_name: str) -> List[Dict]:
        """Parse all articles for a query"""
        print(f"\nParsing query: '{query_name}'")
        articles = self.parse_index_csv(query_name)
        if not articles:
            return []
        
        results = []
        for article in articles:
            try:
                parsed = self.parse_article(article, query_name)
                results.append(parsed)
            except Exception as e:
                print(f"  Error parsing {article['doi']}: {e}")
                self.failed_files.append(f"{article['doi']} - Error: {str(e)[:50]}")
        
        return results

    def clean_results_for_output(self, results: List[Dict]) -> List[Dict]:
        """Clean results keeping only required fields with the correct structure"""
        cleaned_results = []
        
        for result in results:
            if not result.get('parsing_success', False):
                continue
            
            doi = result.get('doi', '')
            if not isinstance(doi, str):
                doi = str(doi)
            
            title = result.get('title', '')
            if not isinstance(title, str):
                title = str(title)
            
            authors = result.get('authors', [])
            if not isinstance(authors, list):
                authors = [str(authors)] if authors else []
            else:
                authors = [str(author) for author in authors if author]
            
            keywords = result.get('keywords', [])
            if not isinstance(keywords, list):
                keywords = [str(keywords)] if keywords else []
            else:
                keywords = [str(kw) for kw in keywords if kw]
            
            abstract = result.get('abstract', '')
            if not isinstance(abstract, str):
                abstract = str(abstract)
            
            editor = result.get('editor', '')
            if not isinstance(editor, str):
                editor = str(editor)
            
            clean_sections = OrderedDict()
            sections = result.get('sections', {})
            
            if isinstance(sections, dict):
                for section_key, section_value in sections.items():
                    if isinstance(section_value, dict):
                        section_name = section_value.get('title', section_key)
                        section_content = section_value.get('content', str(section_value))
                    else:
                        section_name = section_key
                        section_content = str(section_value)
                    
                    section_name_clean = str(section_name).strip()
                    if section_name_clean and section_name_clean.lower() != 'abstract':
                        clean_sections[section_name_clean] = str(section_content)
            
            cleaned_article = {
                'doi': doi,
                'title': title,
                'authors': authors,
                'keywords': keywords,
                'sections': dict(clean_sections),  # keep order but convert to normal dict
                'abstract': abstract,
                'editor': editor
            }
            
            cleaned_results.append(cleaned_article)
        
        return cleaned_results

    def show_archive_statistics(self):
        """Show complete archive statistics"""
        archive_path = self.base_path / "archive_clean"
        index_csv_path = archive_path / "index.csv"
        
        if not index_csv_path.exists():
            print("Archive empty - no file found")
            return
        
        try:
            with open(index_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
            
            if not data:
                print("Archive is empty")
                return
            
            # General statistics
            total_articles = len(data)
            editors = {}
            articles_with_sections = 0
            articles_with_abstract = 0
            total_sections = 0
            total_authors = 0
            
            for row in data:
                editor = row.get('editor', 'Unknown')
                editors[editor] = editors.get(editor, 0) + 1
                
                num_sezioni = int(row.get('num_sezioni', 0))
                if num_sezioni > 0:
                    articles_with_sections += 1
                    total_sections += num_sezioni
                
                if row.get('has_abstract') == 'Sì':
                    articles_with_abstract += 1
                
                total_authors += int(row.get('num_autori', 0))
            
            print(f"\nARCHIVE STATISTICS (overall)")
            print(f"{ '='*50}")
            print(f"Total articles: {total_articles}")
            print(f"With sections: {articles_with_sections} ({articles_with_sections/total_articles*100:.1f}%)")
            print(f"With abstract: {articles_with_abstract} ({articles_with_abstract/total_articles*100:.1f}%)")
            print(f"Total sections: {total_sections}")
            print(f"Total authors: {total_authors}")
            print(f"Avg sections per article: {total_sections/max(articles_with_sections,1):.1f}")
            print(f"Avg authors per article: {total_authors/total_articles:.1f}")
            
            print(f"\nDistribution by Editor:")
            for editor, count in sorted(editors.items(), key=lambda x: x[1], reverse=True):
                percentage = count/total_articles*100
                print(f"  {editor}: {count} ({percentage:.1f}%)")
            
        except Exception as e:
            print(f"Error reading archive statistics: {e}")

    def create_archive_clean(self, results: List[Dict], query_name: str):
        """Create archive_clean folder with individual JSON files and update index.csv incrementally"""
        print(f"\nCreating/updating archive_clean for '{query_name}'...")
        
        archive_path = self.base_path / "archive_clean"
        archive_path.mkdir(exist_ok=True)
        
        cleaned_results = self.clean_results_for_output(results)
        index_csv_path = archive_path / "index.csv"
        
        # Load existing index.csv
        existing_data = {}
        existing_count = 0
        
        if index_csv_path.exists():
            try:
                with open(index_csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        doi = row.get('doi', '').strip()
                        if doi:
                            existing_data[doi] = row
                            existing_count += 1
                print(f"  Loaded existing index.csv: {existing_count} entries")
            except Exception as e:
                print(f"  Error loading existing index.csv: {e}")
                existing_data = {}
        else:
            print(f"  Creating new index.csv")
        
        # Process new results
        new_files_created = 0
        updated_files = 0
        skipped_duplicates = 0
        
        for result in cleaned_results:
            doi = result.get('doi', '')
            if not doi:
                continue
            
            filename = f"{self._sanitize_filename(doi)}.json"
            json_path = archive_path / filename
            
            # Check if already in CSV
            if doi in existing_data:
                # Compare existing and current data
                existing_row = existing_data[doi]
                current_data = {
                    'titolo': result.get('title', ''),
                    'keywords': '; '.join(result.get('keywords', [])),
                    'num_sezioni': len(result.get('sections', {})),
                    'num_autori': len(result.get('authors', [])),
                    'editor': result.get('editor', ''),
                    'has_abstract': 'Sì' if result.get('abstract') else 'No'
                }
                
                # Skip if identical
                if (existing_row.get('titolo') == current_data['titolo'] and
                    existing_row.get('keywords') == current_data['keywords'] and
                    existing_row.get('num_sezioni') == str(current_data['num_sezioni']) and
                    existing_row.get('num_autori') == str(current_data['num_autori']) and
                    existing_row.get('editor') == current_data['editor'] and
                    existing_row.get('has_abstract') == current_data['has_abstract']):
                    
                    skipped_duplicates += 1
                    print(f"  Skipped (already up-to-date): {doi}")
                    continue
                else:
                    updated_files += 1
                    # Show changes
                    changes = []
                    if existing_row.get('num_sezioni') != str(current_data['num_sezioni']):
                        changes.append(f"sections: {existing_row.get('num_sezioni')}→{current_data['num_sezioni']}")
                    if existing_row.get('num_autori') != str(current_data['num_autori']):
                        changes.append(f"authors: {existing_row.get('num_autori')}→{current_data['num_autori']}")
                    if existing_row.get('has_abstract') != current_data['has_abstract']:
                        changes.append(f"abstract: {existing_row.get('has_abstract')}→{current_data['has_abstract']}")
                    
                    change_info = f" ({', '.join(changes)})" if changes else " (metadata)"
                    print(f"  Updated{change_info}: {filename} ({doi})")
            else:
                new_files_created += 1
                print(f"  New: {filename} ({doi})")
            
            # Save/update JSON file
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            # Update data for index
            existing_data[doi] = {
                'doi': doi,
                'nome_file': filename,
                'titolo': result.get('title', ''),
                'keywords': '; '.join(result.get('keywords', [])),
                'num_sezioni': len(result.get('sections', {})),
                'num_autori': len(result.get('authors', [])),
                'editor': result.get('editor', ''),
                'has_abstract': 'Sì' if result.get('abstract') else 'No'
            }
        
        # Save updated index.csv
        if existing_data:
            # Convert numeric values to strings for CSV
            final_data = []
            for doi, row_data in existing_data.items():
                row_copy = row_data.copy()
                row_copy['num_sezioni'] = str(row_copy['num_sezioni'])
                row_copy['num_autori'] = str(row_copy['num_autori'])
                final_data.append(row_copy)
            
            # Sort by DOI for consistency
            final_data.sort(key=lambda x: x['doi'])
            
            with open(index_csv_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['doi', 'nome_file', 'titolo', 'keywords', 'num_sezioni', 'num_autori', 'editor', 'has_abstract']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(final_data)
            
            total_count = len(final_data)
            print(f"  index.csv updated: {total_count} total entries")
            print(f"  Stats: {new_files_created} new | {updated_files} updated | {skipped_duplicates} skipped")
            print(f"  Archive path: {archive_path}")
        
        return archive_path, new_files_created + updated_files

    def save_results(self, results: List[Dict], query_name: str, output_file: str = None):
        """Save results to JSON with cleaned structure"""
        if not output_file:
            output_file = f"parsed_metadata_{query_name}.json"
        
        cleaned_results = self.clean_results_for_output(results)
        
        output_path = self.base_path / output_file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_results, f, indent=2, ensure_ascii=False)
        
        print(f"Results saved to: {output_path}")
        print(f"Saved {len(cleaned_results)} successfully parsed articles (out of {len(results)} total)")
        return output_path

    def print_statistics(self, query_name: str):
        """Print parsing statistics"""
        stats = self.stats[query_name]
        
        print(f"\n{'='*60}")
        print(f"PARSING STATISTICS - '{query_name}'")
        print(f"{ '='*60}")
        
        total = stats.get('total_parsed', 0) + stats.get('parsing_failed', 0)
        success_rate = (stats.get('total_parsed', 0) / total * 100) if total > 0 else 0
        
        print(f"Total: {total} | Success: {stats.get('total_parsed', 0)} ({success_rate:.1f}%) | Failed: {stats.get('parsing_failed', 0)}")
        print(f"Content: {stats.get('authors_extracted', 0)} authors | {stats.get('keywords_extracted', 0)} keywords | {stats.get('abstracts_extracted', 0)} abstracts | {stats.get('sections_extracted', 0)} sections")
        
        if self.failed_files:
            print(f"Failed: {', '.join(self.failed_files[:5])}{'...' if len(self.failed_files) > 5 else ''}")


def interactive_query_selection(parser: ScientificArticleParser) -> str:
    """Interactive query selection"""
    queries = parser.scan_available_queries()
    
    if not queries:
        print("No queries found!")
        return None
    
    print(f"\nAvailable queries:")
    for i, query in enumerate(queries, 1):
        try:
            articles = parser.parse_index_csv(query)
            article_count = len(articles)
            publishers = set()
            
            query_path = parser.base_path / query
            for item in query_path.iterdir():
                if item.is_dir() and item.name != '__pycache__':
                    publishers.add(item.name)
            
            publisher_list = ', '.join(sorted(publishers)) if publishers else 'N/A'
            print(f"  {i}. {query} ({article_count} articles | Publishers: {publisher_list})")
        except:
            print(f"  {i}. {query} (info not available)")
    
    print(f"  {len(queries)+1}. Show archive statistics only")
    
    while True:
        try:
            choice = input(f"\nSelect a query (1-{len(queries)+1}) or 'q' to quit: ").strip()
            if choice.lower() == 'q':
                return None
            
            choice_num = int(choice)
            if choice_num == len(queries) + 1:
                return "SHOW_STATS"
            elif 1 <= choice_num <= len(queries):
                return queries[choice_num - 1]
            else:
                print("Invalid choice!")
        except ValueError:
            print("Enter a valid number!")


def main():
    """
    MAIN EXECUTION - EDIT THESE PARAMETERS
    """
    
    # ============= USER CONFIGURATION =============
    BASE_PATH = "" # Set your path
    
    # Execution options
    AUTO_MODE = False  # True = automatically process the first query, False = interactive selection
    TEST_MODE = False  # True = only first 5 articles, False = all articles
    TARGET_QUERY = "lbvs"  # Specific query to process (used only if AUTO_MODE = True)
    
    # New options
    CREATE_ARCHIVE_CLEAN = True  # True = create archive_clean folder with individual JSONs
    
    # ===============================================
    
    print("Scientific Article Parser")
    print(f"Base Path: {BASE_PATH}")
    print(f"BeautifulSoup: {'yes' if BS4_AVAILABLE else 'no (install with: pip install beautifulsoup4)'}")
    print(f"Test Mode: {TEST_MODE}")
    print(f"Archive Clean: {CREATE_ARCHIVE_CLEAN}")
    print(f"OCR Sections: support for ArXiv, Wiley, ACL_Anthology, MDPI")
    print(f"SECTION COMBINATION: structured.json + raw OCR for maximum coverage")
    print(f"SMART ORDERING: preserves logical section order in the document")
    print(f"OCR Raw Text: improved extraction from raw text (markdown/txt)")
    print(f"Advanced filters: tables, formulas, algorithms, bibliography")
    print(f"Collected DOIs: metadata support from collected_dois.csv")
    print(f"Clean Abstracts: automatic removal of unwanted prefixes")
    print(f"Standardized Output: correct types, no abstract duplication")
    print(f"ACL FIX: corrected ACL Anthology section extraction")
    print(f"INCREMENTAL ARCHIVE: updates without overwriting")
    print(f"FULL STATISTICS: archive overview with details")
    print("="*60)
    
    # Initialize parser
    parser = ScientificArticleParser(BASE_PATH)
    
    # Query selection
    if AUTO_MODE:
        query_name = TARGET_QUERY
        print(f"Automatic mode: processing '{query_name}'")
    else:
        query_name = interactive_query_selection(parser)
        
    if not query_name:
        print("Exit...")
        return
    
    # Handle archive statistics option
    if query_name == "SHOW_STATS":
        print("\nShowing archive statistics...")
        parser.show_archive_statistics()
        return
    
    # Confirm
    if not AUTO_MODE:
        confirm = input(f"\nStart parsing '{query_name}'? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 's', 'si']:
            print("Operation canceled")
            return
    
    print(f"\nStarting parsing of '{query_name}'...")
    
    try:
        # Execute parsing
        results = parser.parse_query(query_name)
        
        if TEST_MODE and len(results) > 5:
            results = results[:5]
            print(f"Test mode: limited to 5 articles")
        
        if results:
            # Save general JSON results
            output_file = f"parsed_metadata_{query_name}.json"
            parser.save_results(results, query_name, output_file)
            
            # Create archive_clean
            if CREATE_ARCHIVE_CLEAN:
                archive_path, processed_files = parser.create_archive_clean(results, query_name)
                print(f"Archive clean updated: {processed_files} files processed (new + updated)")
            
            # Get cleaned results for examples
            cleaned_results = parser.clean_results_for_output(results)
            
            # Sample results
            print(f"\nSample results:")
            for i, result in enumerate(cleaned_results[:3]):
                title = result['title'][:60] + "..." if len(result['title']) > 60 else result['title']
                print(f"{i+1}. {title}")
                print(f"   {len(result['authors'])} authors | {len(result['keywords'])} keywords | {len(result['sections'])} sections | {len(result['abstract'])} chars")
                if result['sections']:
                    section_names = list(result['sections'].keys())
                    print(f"   Sections: {', '.join(section_names[:3])}{'...' if len(section_names) > 3 else ''}")
            
            # Final statistics for the query
            parser.print_statistics(query_name)
            
            # Overall archive statistics
            if CREATE_ARCHIVE_CLEAN:
                parser.show_archive_statistics()
            
            print(f"\nCOMPLETED! {len(cleaned_results)} parsed articles saved (out of {len(results)} processed)")
            print(f"Results saved in: {BASE_PATH}/{output_file}")
            
            if CREATE_ARCHIVE_CLEAN:
                print(f"Archive clean: {BASE_PATH}/archive_clean/ (updated incrementally)")
            
        else:
            print("No results obtained")
            
    except Exception as e:
        print(f"Error during parsing: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
