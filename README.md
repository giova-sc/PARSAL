# PARSAL (Pipeline for Automatic Retrieval and Structuring of Academic Literature)

PARSAL is a Python-based system for automated retrieval, downloading, and structuring of scientific literature.
It provides a modern graphical user interface and supports asynchronous downloading from major academic publishers.

**This project was developed as part of my Master’s thesis in collaboration with the Chilab research laboratory in University of Palermo.
The repository is published for portfolio and demonstration purposes, showcasing the architecture and implementation of the system.**


## 🚀 Key Features

*   **Multi-Publisher Support:** Search and download articles from:
    *   Elsevier (ScienceDirect)
    *   Springer
    *   Wiley
    *   MDPI
    *   ArXiv
    *   ACL Anthology
*   **Modern GUI:** User-friendly interface built with `customtkinter`, featuring dark mode support, real-time search feedback, and easy selection of articles.
*   **Asynchronous Performance:** Utilizes `asyncio` and `aiohttp` for high-speed, parallel searching and downloading of full-text articles (PDF, XML, JSON).
*   **Advanced Parsing:**
    *   Extracts structured metadata (Title, Authors, Abstract, Keywords, DOI).
    *   **OCR Integration:** Incorporates OCR results (via `olmocr` or raw text) to reconstruct article sections in logical order.
    *   Clean data export to JSON with standardized fields.
*   **Smart Filtering:** Search by keyword and publication year.

## 🛠️ Installation

### Prerequisites
*   Python 3.8 or higher
*   pip (Python package manager)
*   All required packages are listed in `requirements.txt`.
    Install them with:
   ```bash
   pip install -r requirements.txt
   ```
### Setup Steps

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/your-username/PARSAL.git
    cd PARSAL
    ```

2.  **Create a Virtual Environment (Recommended)**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

## 🖥️ Usage

1.  **Launch the Application**
    Run the main GUI script:
    ```bash
    python parsal_gui.py
    ```

2.  **Search for Articles**
    *   Enter a **keyword** (e.g., "machine learning").
    *   (Optional) Select a **Publication Year**.
    *   Choose the **Publishers** you want to query from the sidebar.
    *   Click the **Search (...)** button.

3.  **Download**
    *   Review the search results in the main list.
    *   Select the articles you wish to download (or use "Select All").
    *   Click **⬇️ Download** and choose a destination folder.

## 📂 Project Structure

```
PARSAL/
├── parsal_gui.py           # Main entry point (GUI Application)
├── download_fulltext.py    # Core logic for async searching and downloading
├── article_parser.py       # Advanced parsing logic and OCR integration
├── doi_mapping.py          # Utilities for DOI resolution
├── requirements.txt        # Project dependencies
├── chilab_logo.png         # Logo file
└── downloaders/            # Publisher-specific download modules
    ├── elsevier.py
    ├── springer.py
    ├── wiley.py
    ├── arxiv.py
    ├── mdpi.py
    └── acl.py
```

## ⚠️ Disclaimer

This tool is intended for educational and research purposes. Please respect the Terms of Service of the respective publishers. Automated scraping may be restricted or prohibited by some providers. Ensure you have the necessary access rights (e.g., university VPN or institutional subscription) to download full-text articles from subscription-based journals.
