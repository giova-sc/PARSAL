# downloaders/utils.py

import fitz # PyMuPDF
import pdfplumber
import PyPDF2
from io import BytesIO
import re
from typing import Tuple

# Costanti di validazione
MIN_PDF_PAGES = 3
MIN_PDF_SIZE_BYTES = 30000
MIN_TEXT_CONTENT = 300

# Disponibilità delle librerie
PYMUPDF_AVAILABLE = True
PDFPLUMBER_AVAILABLE = True
PYPDF2_AVAILABLE = True

# def sanitize_filename(name: str) -> str:
#     """Rimuove i caratteri non validi per i nomi di file."""
#     return re.sub(r'[\\/*?:"<>|]', '_', name)

def sanitize_filename(name):
    if not name:
        name = "Unknown"
    return re.sub(r'[\\/*?:"<>|]', '_', str(name))



def validate_pdf_multi_library(pdf_content: bytes, doi: str) -> Tuple[bool, int, str]:
    """Valida un PDF usando più librerie per robustezza."""
    if not pdf_content or not pdf_content.startswith(b'%PDF-'):
        return False, 0, "invalid_header"
    if len(pdf_content) < MIN_PDF_SIZE_BYTES:
        return False, 0, "too_small"

    page_count = 0
    validation_method = "unknown"

    if PYMUPDF_AVAILABLE:
        try:
            with fitz.open(stream=pdf_content, filetype="pdf") as doc:
                page_count = len(doc)
                if page_count >= MIN_PDF_PAGES:
                    text_sample = "".join(doc[i].get_text() for i in range(min(3, page_count)))
                    if len(text_sample.strip()) > MIN_TEXT_CONTENT:
                        return True, page_count, "valid_pymupdf"
        except Exception:
            pass

    if PDFPLUMBER_AVAILABLE:
        try:
            with pdfplumber.open(BytesIO(pdf_content)) as pdf:
                page_count = len(pdf.pages)
                if page_count >= MIN_PDF_PAGES:
                     return True, page_count, "valid_pdfplumber"
        except Exception:
            pass
            
    if PYPDF2_AVAILABLE:
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content), strict=False)
            page_count = len(pdf_reader.pages)
            if page_count >= MIN_PDF_PAGES:
                return True, page_count, "valid_pypdf2"
        except Exception:
            pass
            
    return False, page_count, "validation_failed"