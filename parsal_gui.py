#!/usr/bin/env python3
import customtkinter as ctk
import threading
import os
import re
from typing import List, Dict, Optional
from PIL import Image
import tkinter.messagebox
import tkinter.filedialog
from functools import partial
from datetime import datetime

# Import the downloader which now handles both search and download
try:
    from download_fulltext import EnhancedFullTextDownloader
    DOWNLOADER_AVAILABLE = True
except ImportError as e:
    print(f"❌ CRITICAL ERROR: Could not import 'download_fulltext.py'. Make sure the file is present. Details: {e}")
    DOWNLOADER_AVAILABLE = False

ctk.set_default_color_theme("dark-blue")
ctk.set_appearance_mode("system")
ABSTRACT_TRUNCATE_LENGTH = 250

class ModernPARSALApp(ctk.CTk):
    """
    Main application class for PARSAL.
    Provides a graphical interface to search and download scientific literature
    from multiple publishers using an asynchronous backend.
    """

    def __init__(self):
        super().__init__()
        self.title("PARSAL")
        self.geometry("1400x900")
        self.minsize(1200, 700)

        # Ensure the downloader backend is available before proceeding
        if not DOWNLOADER_AVAILABLE:
            tkinter.messagebox.showerror("Downloader Error", "The file 'download_fulltext.py' was not found or contains errors. The application will close.")
            self.after(100, self.destroy)
            return
        
        # Initialize the downloader manager
        self.download_manager = EnhancedFullTextDownloader(csv_file=None)

        # GUI State variables
        self.keyword_var = ctk.StringVar()
        self.year_var = ctk.StringVar()
        self.publisher_vars = {}
        self.results_count_var = ctk.StringVar(value="Ready to search")
        self.current_results = []
        self.article_selection_vars = {}
        self.select_all_var = ctk.BooleanVar()
        
        self.load_logo()
        self.setup_widgets()
        self.populate_dynamic_options()
        self.setup_bindings()
        self.check_workflow_progress()
        self.title("PARSAL")

    def load_logo(self):
        """
        Attempts to load the ChiLab logo from the local directory.
        If the file is missing, the application proceeds without it.
        """
        try:
            logo_path = "chilab_logo.png"
            if os.path.exists(logo_path):
                img = Image.open(logo_path)
                self.logo_image = ctk.CTkImage(light_image=img, dark_image=img, size=(50, 50))
                print("ChiLab logo loaded.")
            else:
                self.logo_image = None
                print("ChiLab logo not found.")
        except Exception as e:
            print(f"❌ Error loading logo: {e}")
            self.logo_image = None

    def populate_dynamic_options(self):
        """
        Populates the search filters (like years) with default values.
        Currently uses a static range but could be expanded to query a database.
        """
        available_years = ["All Years"] + [str(y) for y in range(datetime.now().year, 1980, -1)]
        if hasattr(self, 'year_combo'):
            self.year_combo.configure(values=available_years)
            self.year_var.set("All Years")

    def check_workflow_progress(self):
        """
        Dynamic UI state management.
        Enables or disables the search and download buttons based on user input
        and selection status to prevent invalid operations.
        """
        has_keyword = bool(self.keyword_var.get().strip())
        can_search = has_keyword
        
        if hasattr(self, 'search_button'):
            self.search_button.configure(state="normal" if can_search else "disabled")
            self.search_button.configure(text="🔍 Enter a keyword to search" if not can_search else "🔍 Start Search")
                
        if hasattr(self, 'download_button'):
            selected_count = self.get_selected_count()
            self.download_button.configure(state="normal" if selected_count > 0 else "disabled")

    def setup_bindings(self):
        """Sets up event listeners for real-time UI updates."""
        self.keyword_var.trace_add('write', lambda *args: self.check_workflow_progress())
        self.keyword_entry.bind('<Return>', self.start_search)
        self.select_all_var.trace_add('write', self.on_select_all_change)

    def on_select_all_change(self, *args):
        """Toggles selection for all articles currently displayed."""
        is_checked = self.select_all_var.get()
        for var in self.article_selection_vars.values():
            var.set(is_checked)
        self.update_selection_display()
        
    def on_article_selection_change(self, *args):
        """Updates the 'Select All' checkbox state based on individual selections."""
        self.update_selection_display()
        selected_count = self.get_selected_count()
        total_count = len(self.article_selection_vars)
        
        # Handle indeterminate state of select_all checkbox
        if 0 < selected_count < total_count:
            if self.select_all_var.get(): self.select_all_var.set(False)
        elif selected_count == total_count and total_count > 0:
            if not self.select_all_var.get(): self.select_all_var.set(True)
        elif selected_count == 0:
             if self.select_all_var.get(): self.select_all_var.set(False)

    def get_selected_count(self):
        """Returns the number of articles currently selected by the user."""
        return sum(1 for var in self.article_selection_vars.values() if var.get())

    def update_selection_display(self):
        """Updates the label showing how many articles are found vs selected."""
        selected_count = self.get_selected_count()
        total_count = len(self.current_results)
        self.results_count_var.set(f"Found {total_count} articles • {selected_count} selected")
        self.download_button.configure(state="normal" if selected_count > 0 else "disabled")

    def start_search(self, event=None):
        """Triggered by the Search button or Enter key."""
        if self.search_button.cget("state") == "normal":
            self.search_articles()

    def search_articles(self):
        """
        Validates inputs and initiates the search process.
        The actual API calls are performed in a background thread to keep the UI responsive.
        """
        keyword = self.keyword_var.get().strip()
        selected_publishers = [pub for pub, var in self.publisher_vars.items() if var.get()]
        
        # Parse and validate the selected year
        year_str = self.year_var.get()
        year_to_search = int(year_str) if year_str.isdigit() else None

        if not selected_publishers:
            tkinter.messagebox.showwarning("Warning", "Please select at least one publisher to search.")
            return
        
        # Visual feedback during search
        self.results_count_var.set(f"Searching on {len(selected_publishers)} publishers...")
        self.search_button.configure(state="disabled")
        self.update()

        # Execute search in a daemon thread to prevent blocking the main loop
        threading.Thread(
            target=self.run_api_search_worker,
            args=(keyword, selected_publishers, year_to_search),
            daemon=True
        ).start()

    def run_api_search_worker(self, keyword: str, publishers: List[str], year: Optional[int]):
        """
        Background worker that interacts with the downloader APIs.
        Results are passed back to the main thread via self.after for thread-safe UI updates.
        """
        try:
            if not hasattr(self.download_manager, 'search_live_apis'):
                self.after(0, tkinter.messagebox.showerror, "Error", "Function 'search_live_apis' not found in download_fulltext.py.")
                self.after(0, self.update_ui_with_results, [])
                return

            # Perform the actual network search
            results = self.download_manager.search_live_apis(keyword, publishers, year)
            
            # Dispatch UI update back to the main thread
            self.after(0, self.update_ui_with_results, results)

        except Exception as e:
            error_message = f"An error occurred during the API search: {e}"
            print(error_message)
            self.after(0, tkinter.messagebox.showerror, "Search Error", error_message)
            self.after(0, self.update_ui_with_results, [])

    def update_ui_with_results(self, results: List[Dict]):
        """Callback to refresh the UI results list once the search is complete."""
        self.current_results = results
        self.search_button.configure(state="normal")
        self.update_results_display()

    def setup_widgets(self):
        """Main UI layout definition using a grid system."""
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(1, weight=1)
        
        self.setup_header()
        
        # Main container for sidebar and results
        self.main_frame = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.main_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=20, pady=(15, 20))
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(1, weight=1)
        
        self.setup_search_sidebar()
        self.setup_results_area()

    def setup_header(self):
        """Creates the top bar with logo, title, and theme switcher."""
        header_frame = ctk.CTkFrame(self, height=70, corner_radius=0, fg_color=("gray90", "gray17"))
        header_frame.grid(row=0, column=0, columnspan=2, sticky="ew")
        header_frame.grid_columnconfigure(1, weight=1)
        
        logo_title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        logo_title_frame.grid(row=0, column=0, sticky="w", padx=20, pady=10)
        
        if self.logo_image:
            logo_label = ctk.CTkLabel(logo_title_frame, image=self.logo_image, text="")
            logo_label.pack(side="left", padx=(0, 15))
            
        title_frame = ctk.CTkFrame(logo_title_frame, fg_color="transparent")
        title_frame.pack(side="left", anchor="w")
        ctk.CTkLabel(title_frame, text="PARSAL", font=ctk.CTkFont(size=22, weight="bold")).pack(anchor="w")
        ctk.CTkLabel(title_frame, text="Automatic retrieval of scientific literature", font=ctk.CTkFont(size=12)).pack(anchor="w")
        
        theme_switcher_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        theme_switcher_frame.grid(row=0, column=2, sticky="e", padx=20, pady=10)
        ctk.CTkLabel(theme_switcher_frame, text="Dark Mode:", font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 10))
        
        theme_switch = ctk.CTkSwitch(
            theme_switcher_frame, 
            text="", 
            command=lambda: ctk.set_appearance_mode("Light" if ctk.get_appearance_mode() == "Dark" else "Dark"), 
            width=0
        )
        theme_switch.pack(side="left")
        if ctk.get_appearance_mode() == "Dark": theme_switch.select()

    def setup_search_sidebar(self):
        """Creates the left panel with search inputs and publisher filters."""
        sidebar_frame = ctk.CTkFrame(self.main_frame, width=320, corner_radius=10)
        sidebar_frame.grid(row=0, column=0, sticky="ns", padx=(0, 20))
        sidebar_frame.grid_rowconfigure(2, weight=1)
        sidebar_frame.grid_columnconfigure(0, weight=1)
        
        top_controls = ctk.CTkFrame(sidebar_frame, fg_color="transparent")
        top_controls.grid(row=0, column=0, sticky="new", padx=15, pady=(15, 0))
        
        # Keyword input
        ctk.CTkLabel(top_controls, text="Search Keyword", font=ctk.CTkFont(size=13, weight="bold")).pack(anchor="w", pady=(10, 5), fill="x")
        self.keyword_entry = ctk.CTkEntry(top_controls, textvariable=self.keyword_var, placeholder_text="e.g., drug discovery", height=36)
        self.keyword_entry.pack(fill="x", pady=(0, 15))
        
        # Year selection
        ctk.CTkLabel(top_controls, text="Publication Year", font=ctk.CTkFont(size=13, weight="bold")).pack(anchor="w", pady=(10, 5), fill="x")
        self.year_combo = ctk.CTkComboBox(top_controls, variable=self.year_var, values=[], height=36)
        self.year_combo.pack(fill="x", pady=(0, 10))
        
        # Publishers list
        ctk.CTkLabel(sidebar_frame, text="Publishers", font=ctk.CTkFont(size=16, weight="bold")).grid(row=1, column=0, sticky="ew", padx=15, pady=(10, 5))
        
        publishers_frame = ctk.CTkScrollableFrame(sidebar_frame, label_text="", fg_color="transparent")
        publishers_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=0)
        
        # Dynamically create checkboxes for each supported publisher
        available_publishers = list(self.download_manager.dispatch_table.keys())
        self.publisher_vars.clear()
        for publisher in available_publishers:
            self.publisher_vars[publisher] = ctk.BooleanVar()
            ctk.CTkCheckBox(publishers_frame, text=publisher, variable=self.publisher_vars[publisher]).pack(anchor="w", padx=5, pady=4, fill="x")
        
        self.search_button = ctk.CTkButton(sidebar_frame, text="...", command=self.start_search, height=40, font=ctk.CTkFont(size=14, weight="bold"))
        self.search_button.grid(row=3, column=0, sticky="sew", padx=15, pady=15)

    def setup_results_area(self):
        """Creates the main area where search results are displayed."""
        results_container = ctk.CTkFrame(self.main_frame, corner_radius=10)
        results_container.grid(row=0, column=1, sticky="nsew")
        results_container.grid_rowconfigure(1, weight=1)
        results_container.grid_columnconfigure(0, weight=1)
        
        # Results header (Select all & Download)
        header_frame = ctk.CTkFrame(results_container, fg_color="transparent", height=60)
        header_frame.grid(row=0, column=0, sticky="ew", padx=20, pady=10)
        header_frame.grid_columnconfigure(1, weight=1)
        
        self.select_all_checkbox = ctk.CTkCheckBox(header_frame, text="Select All", variable=self.select_all_var, font=ctk.CTkFont(weight="bold"))
        self.select_all_checkbox.grid(row=0, column=0, sticky="w", padx=(10, 0))
        
        ctk.CTkLabel(header_frame, textvariable=self.results_count_var, font=ctk.CTkFont(size=14), text_color=("gray40", "gray60")).grid(row=0, column=1, sticky="w", padx=20)
        
        self.download_button = ctk.CTkButton(
            header_frame, 
            text="⬇️ Download", 
            command=self.start_download_process, 
            height=36, font=ctk.CTkFont(size=13, weight="bold"), 
            state="disabled", 
            fg_color="#107C41", 
            hover_color="#0B572E"
        )
        self.download_button.grid(row=0, column=2, sticky="e", padx=(0, 10))
        
        # Scrollable area for article cards
        self.scrollable_frame = ctk.CTkScrollableFrame(results_container, corner_radius=8, fg_color=("gray92", "gray20"))
        self.scrollable_frame.grid(row=1, column=0, sticky="nsew", padx=20, pady=(0, 20))
        self.scrollable_frame.grid_columnconfigure(0, weight=1)
        
        # Bottom progress bar (initially hidden)
        self.progress_frame = ctk.CTkFrame(results_container, fg_color="transparent")
        self.progress_bar = ctk.CTkProgressBar(self.progress_frame)
        self.progress_status_label = ctk.CTkLabel(self.progress_frame, text="", font=ctk.CTkFont(size=11))

    def update_results_display(self):
        """Clears and rebuilds the article list in the UI."""
        for widget in self.scrollable_frame.winfo_children(): widget.destroy()
        self.article_selection_vars.clear()
        self.select_all_var.set(False)
        
        if not self.current_results:
            # Show empty state
            center_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="transparent")
            center_frame.pack(expand=True, fill="both", pady=100)
            ctk.CTkLabel(center_frame, text="📂", font=ctk.CTkFont(size=48)).pack(pady=(0, 10))
            ctk.CTkLabel(center_frame, text="No Articles Found", font=ctk.CTkFont(size=18, weight="bold")).pack()
            ctk.CTkLabel(center_frame, text="Try adjusting your search parameters.", font=ctk.CTkFont(size=12), text_color=("gray60", "gray40")).pack(pady=5)
        else:
            # Render a card for each article
            for i, article in enumerate(self.current_results): self.create_article_card(i, article)
        
        self.update_selection_display()

    def create_article_card(self, index: int, article: Dict):
        """
        Creates a visual card representing a scientific article.
        Includes title, authors, publisher, DOI, and abstract.
        """
        selection_var = ctk.BooleanVar()
        self.article_selection_vars[index] = selection_var
        selection_var.trace_add('write', self.on_article_selection_change)
        
        card_frame = ctk.CTkFrame(self.scrollable_frame, corner_radius=8, fg_color=("white", "gray28"))
        card_frame.pack(fill="x", pady=(0, 10), padx=5)
        
        main_content = ctk.CTkFrame(card_frame, fg_color="transparent")
        main_content.pack(fill="x", padx=15, pady=12)
        main_content.grid_columnconfigure(1, weight=1)
        
        # Checkbox for selection
        ctk.CTkCheckBox(main_content, text="", variable=selection_var, width=20).grid(row=0, column=0, rowspan=5, sticky="n", padx=(0, 15), pady=4)
        
        # Article Title
        ctk.CTkLabel(main_content, text=article.get('title', 'N/A'), font=ctk.CTkFont(size=16, weight="bold"), wraplength=700, justify="left", anchor="w").grid(row=0, column=1, sticky="w")
        
        # Year Badge
        if article.get('year'):
            ctk.CTkLabel(main_content, text=str(article.get('year')), font=ctk.CTkFont(size=11, weight="bold"), fg_color=("#E2E8F0", "#4A5568"), corner_radius=6, padx=8).grid(row=0, column=2, sticky="ne", padx=(10, 0), pady=2)
        
        # Metadata (Authors, Publisher, DOI)
        meta_frame = ctk.CTkFrame(main_content, fg_color="transparent")
        meta_frame.grid(row=1, column=1, columnspan=2, sticky="w", pady=(8, 10))
        
        authors = article.get('authors', [])
        author_text = "Authors: " + (', '.join(authors[:3]) + (f", +{len(authors) - 3}" if len(authors) > 3 else "") if authors else "N/A")
        ctk.CTkLabel(meta_frame, text=author_text, font=ctk.CTkFont(size=12)).pack(side="left")
        
        ctk.CTkLabel(meta_frame, text=f"•  Publisher: {article.get('editor', 'N/A')}", font=ctk.CTkFont(size=12)).pack(side="left", padx=5)
        ctk.CTkLabel(meta_frame, text=f"•  DOI: {article.get('doi', 'N/A')}", font=ctk.CTkFont(size=12)).pack(side="left", padx=5)
        
        # Snippet of the Abstract
        if article.get('abstract'):
            ctk.CTkLabel(main_content, text=article.get('abstract'), font=ctk.CTkFont(size=12), wraplength=750, justify="left", anchor="w").grid(row=2, column=1, columnspan=2, sticky="ew", pady=(0, 5))

    def get_selected_articles(self) -> List[Dict]:
        """Filters the current results to return only those checked by the user."""
        return [self.current_results[i] for i, var in self.article_selection_vars.items() if var.get()]

    def start_download_process(self):
        """
        Asks for a destination folder and starts the download thread.
        Shows the progress bar and disables controls during the operation.
        """
        selected_articles = self.get_selected_articles()
        if not selected_articles:
            tkinter.messagebox.showwarning("Warning", "No articles selected!")
            return
        
        # Ask user where to save the files
        output_dir = tkinter.filedialog.askdirectory(title="Select a folder for downloads")
        if not output_dir: return

        # Show progress UI
        self.progress_bar.pack(pady=(10,5), fill="x", padx=20)
        self.progress_status_label.pack(fill="x", padx=20)
        self.progress_frame.grid(row=2, column=0, sticky="ew", padx=20, pady=(0, 10))
        
        self.download_button.configure(state="disabled")
        self.search_button.configure(state="disabled")
        
        # Start download in a background thread
        threading.Thread(target=self.download_worker, args=(selected_articles, self.keyword_var.get(), output_dir), daemon=True).start()

    def download_worker(self, articles: List[Dict], keyword: str, output_dir: str):
        """
        Worker thread for article downloads.
        Provides periodic progress updates back to the UI thread.
        """
        def progress_callback(current, total, status):
            progress = float(current) / total if total > 0 else 0
            self.after(0, self.update_progress, progress, status)
            
        results = self.download_manager.download_selected_articles(articles, keyword or "search", output_dir, progress_callback)
        self.after(0, self.download_completed, results, keyword or "search", output_dir)

    def update_progress(self, progress, status):
        """Thread-safe update of the progress bar and status label."""
        self.progress_bar.set(progress)
        self.progress_status_label.configure(text=status)

    def download_completed(self, results, keyword, output_dir):
        """Cleans up the UI after a download process finishes."""
        self.progress_frame.grid_forget()
        self.check_workflow_progress()
        tkinter.messagebox.showinfo("Download Complete", f"Download finished for query '{keyword}'.\nFiles saved in: {output_dir}")

def main():
    """Application entry point."""
    try:
        print("🚀 Starting PARSAL...")
        app = ModernPARSALApp()
        if app.winfo_exists():
             app.mainloop()
    except Exception as e:
        print(f"Critical error: {e}")

if __name__ == "__main__":
    main()

def main():
    try:
        print("🚀 Starting PARSAL...")
        app = ModernPARSALApp()
        if app.winfo_exists():
             app.mainloop()
    except Exception as e:
        print(f"Critical error: {e}")

if __name__ == "__main__":
    main()