#!/usr/bin/env python3
from typing import Optional

DOI_PREFIX_MAPPING = {
    # Elsevier (Scopus/ScienceDirect)
    "10.1016": "Elsevier",
    
    # Springer Nature (all variants)
    "10.1007": "Springer",
    "10.1038": "Nature Publishing Group",  # Nature journals
    "10.1057": "Springer",  # Palgrave Macmillan (owned by Springer)
    "10.1140": "Springer",  # European Physical Journal
    "10.1245": "Springer",  # Annals of Surgical Oncology
    "10.1617": "Springer",  # Materials and Structures
    "10.1186": "Springer (BioMed Central Ltd.)",  # BioMed Central
    "10.1208": "Springer",  # AAPS journals
    "10.1333": "Springer",  # Mammalian Genome
    "10.1365": "Springer",  # International Journal of Colorectal Disease
    
    # Wiley
    "10.1002": "Wiley",
    "10.1111": "Wiley",
    "10.1034": "Wiley",  # Some older Wiley journals
    "10.1046": "Wiley",  # Some older Wiley journals
    "10.1113": "Wiley",  # The Journal of Physiology
    "10.1155": "Wiley",  # Some Hindawi journals (now part of Wiley)
    "10.1196": "Wiley",  # Annals of the New York Academy of Sciences
    
    # MDPI
    "10.3390": "MDPI",
    
    # IEEE
    "10.1109": "IEEE",
    
    # Taylor & Francis
    "10.1080": "Taylor & Francis",
    "10.1081": "Taylor & Francis",
    "10.1076": "Taylor & Francis",
    "10.1300": "Taylor & Francis",
    
    # SAGE Publications
    "10.1177": "SAGE Publications",
    "10.1191": "SAGE Publications",
    
    # Oxford University Press
    "10.1093": "Oxford University Press",
    
    # Cambridge University Press
    "10.1017": "Cambridge University Press",
    
    # American Chemical Society (ACS)
    "10.1021": "American Chemical Society",
    
    # Public Library of Science (PLOS)
    "10.1371": "PLOS",
    
    # AAAS (Science)
    "10.1126": "Science (AAAS)",
    
    # PNAS
    "10.1073": "PNAS",
    
    # BMJ Publishing Group
    "10.1136": "BMJ Publishing Group",
    
    # American Medical Association
    "10.1001": "American Medical Association",
    
    # Frontiers Media
    "10.3389": "Frontiers Media",
    
    # EMBO Press
    "10.15252": "EMBO Press",
    
    # eLife Sciences Publications
    "10.7554": "eLife Sciences Publications",
    
    # Royal Society of Chemistry
    "10.1039": "Royal Society of Chemistry",
    
    # American Institute of Physics
    "10.1063": "American Institute of Physics",
    
    # American Physical Society
    "10.1103": "American Physical Society",
    
    # IOP Publishing
    "10.1088": "IOP Publishing",
    
    # Hindawi
    "10.1155": "Hindawi",  # Note: Some overlap with Wiley
    
    # Karger Publishers
    "10.1159": "Karger Publishers",
    
    # Mary Ann Liebert
    "10.1089": "Mary Ann Liebert",
    
    # Thieme Medical Publishers
    "10.1055": "Thieme Medical Publishers",
    
    # Annual Reviews
    "10.1146": "Annual Reviews",
    
    # Company of Biologists
    "10.1242": "Company of Biologists",
    
    # Cold Spring Harbor Laboratory Press
    "10.1101": "Cold Spring Harbor Laboratory Press",  # Also bioRxiv preprints
    
    # Rockefeller University Press
    "10.1083": "Rockefeller University Press",
    
    # American Society for Microbiology
    "10.1128": "American Society for Microbiology",
    
    # Proceedings of the National Academy of Sciences
    "10.1073": "PNAS",
    
    # Royal Society Publishing
    "10.1098": "Royal Society Publishing",
    "10.1042": "Royal Society Publishing",  # Portland Press
    
    # Acoustical Society of America
    "10.1121": "Acoustical Society of America",
    
    # American Diabetes Association
    "10.2337": "American Diabetes Association",
    
    # American Heart Association
    "10.1161": "American Heart Association",
    
    # Cell Press (Elsevier)
    "10.1016": "Elsevier",  # Cell journals are under Elsevier
    
    # Multidisciplinary Digital Publishing Institute (MDPI) - additional
    "10.3390": "MDPI",
    
    # Special cases for non-traditional identifiers
    # ArXiv preprints (not DOIs, but handled separately)
    # ACL Anthology (not DOIs, but handled separately)
}

# Additional mappings for publisher name variations
PUBLISHER_NAME_MAPPING = {
    # Springer variants
    "springer-verlag": "Springer",
    "springer verlag": "Springer",
    "springer science+business media": "Springer",
    "springer science and business media": "Springer",
    "springer nature": "Springer",
    "biomed central": "Springer (BioMed Central Ltd.)",
    "biomed central ltd": "Springer (BioMed Central Ltd.)",
    "bmc": "Springer (BioMed Central Ltd.)",
    
    # Wiley variants
    "john wiley & sons": "Wiley",
    "john wiley and sons": "Wiley",
    "wiley-blackwell": "Wiley",
    "wiley blackwell": "Wiley",
    
    # Elsevier variants
    "elsevier science": "Elsevier",
    "elsevier ltd": "Elsevier",
    "elsevier b.v.": "Elsevier",
    "elsevier bv": "Elsevier",
    
    # Other common variants
    "oxford univ press": "Oxford University Press",
    "oxford university press": "Oxford University Press",
    "cambridge univ press": "Cambridge University Press",
    "cambridge university press": "Cambridge University Press",
    "taylor and francis": "Taylor & Francis",
    "taylor & francis": "Taylor & Francis",
    "sage publications": "SAGE Publications",
    "sage": "SAGE Publications",
    "ieee": "IEEE",
    "mdpi ag": "MDPI",
    "multidisciplinary digital publishing institute": "MDPI",
}

def get_editor_from_doi(doi: str) -> Optional[str]:
    """
    Get standardized editor/publisher name from DOI
    """
    
    if not doi or not isinstance(doi, str):
        return None
    
    doi = doi.strip()
    
    # Handle special cases first
    if doi.startswith("arXiv:") or doi.startswith("arxiv:"):
        return "ArXiv"
    
    if doi.startswith("ACL:") or doi.startswith("acl:"):
        return "ACL Anthology"
    
    # Extract DOI prefix (first two parts after 10.)
    if not doi.startswith("10."):
        return None
    
    try:
        # Split DOI to get prefix
        parts = doi.split("/")
        if len(parts) < 2:
            return None
        
        prefix = parts[0]  # e.g., "10.1016"
        
        # Direct prefix lookup
        if prefix in DOI_PREFIX_MAPPING:
            return DOI_PREFIX_MAPPING[prefix]
        
        if len(parts[1]) > 0:
            # Some publishers use sub-prefixes
            extended_prefix = f"{prefix}/{parts[1][:3]}"  # First 3 chars of suffix
            
            # Special cases for extended prefixes
            extended_mappings = {
                "10.1007/978": "Springer",  # Springer books
                "10.1007/BF": "Springer",   # Older Springer format
                "10.1016/j.": "Elsevier",   # Elsevier journals
                "10.1016/S": "Elsevier",    # Elsevier series
                "10.1016/B": "Elsevier",    # Elsevier books
            }
            
            if extended_prefix in extended_mappings:
                return extended_mappings[extended_prefix]
        
        return None
        
    except Exception:
        return None

def get_editor_from_publisher_name(publisher_name: str) -> Optional[str]:
    """
    Get standardized editor name from publisher name string
    """
    
    if not publisher_name or not isinstance(publisher_name, str):
        return None
    
    publisher_lower = publisher_name.lower().strip()
    
    # Direct lookup
    if publisher_lower in PUBLISHER_NAME_MAPPING:
        return PUBLISHER_NAME_MAPPING[publisher_lower]
    
    # Partial matching for common publishers
    partial_matches = {
        "springer": "Springer",
        "elsevier": "Elsevier", 
        "wiley": "Wiley",
        "mdpi": "MDPI",
        "ieee": "IEEE",
        "taylor": "Taylor & Francis",
        "sage": "SAGE Publications",
        "oxford": "Oxford University Press",
        "cambridge": "Cambridge University Press",
        "nature": "Nature Publishing Group",
        "plos": "PLOS",
        "frontiers": "Frontiers Media",
    }
    
    for key, value in partial_matches.items():
        if key in publisher_lower:
            return value
    
    return None

def is_supported_publisher(doi_or_publisher: str) -> bool:
    """
    Check if a DOI or publisher name is supported
    """
    if not doi_or_publisher:
        return False
    
    # Try as DOI first
    editor = get_editor_from_doi(doi_or_publisher)
    if editor:
        return True
    
    # Try as publisher name
    editor = get_editor_from_publisher_name(doi_or_publisher)
    if editor:
        return True
    
    return False

def get_all_supported_prefixes() -> list:
    """
    Get list of all supported DOI prefixes
    """
    
    return list(DOI_PREFIX_MAPPING.keys())


def get_all_supported_publishers() -> list:
    """
    Get list of all supported publisher names
    """
    all_publishers = set(DOI_PREFIX_MAPPING.values())
    all_publishers.update(PUBLISHER_NAME_MAPPING.values())
    return sorted(list(all_publishers))

# Test function
def test_doi_mapping():
    """Test the DOI mapping functionality"""
    test_dois = [
        "10.1016/j.cell.2020.01.001",  # Elsevier
        "10.1007/s12345-020-01234-5",  # Springer
        "10.1002/adma.202001234",      # Wiley
        "10.3390/molecules25010001",   # MDPI
        "10.1038/s41586-020-2649-2",  # Nature
        "10.1093/nar/gkaa123",         # Oxford
        "10.1371/journal.pone.0123456", # PLOS
        "arXiv:2001.12345",            # ArXiv
        "ACL:2020.acl-main.123",       # ACL
        "10.9999/invalid.prefix",      # Invalid
    ]
    
    print("🧪 Testing DOI Mapping:")
    print("-" * 50)
    
    for doi in test_dois:
        editor = get_editor_from_doi(doi)
        status = "✅" if editor else "❌"
        print(f"{status} {doi:<35} → {editor or 'Not Found'}")
    
    print(f"\n📊 Supported Prefixes: {len(get_all_supported_prefixes())}")
    print(f"📚 Supported Publishers: {len(get_all_supported_publishers())}")

if __name__ == "__main__":
    test_doi_mapping()