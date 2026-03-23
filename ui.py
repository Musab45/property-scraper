from __future__ import annotations

import json
import os
import queue
import re
import threading
import tkinter as tk
from copy import deepcopy
from datetime import datetime
from tkinter import filedialog, messagebox, simpledialog, ttk
from urllib.parse import parse_qs, parse_qsl, urlparse

from commercial_scraper import CommercialGuruScraper
from listing_scraper import DirectListingScraper, read_urls_from_file
from scraper import PropertyGuruScraper, ScraperConfig

PRESETS_FILE = "presets.json"
LAST_SETTINGS_FILE = "last_settings.json"

DISTRICTS = [f"D{i:02d}" for i in range(1, 29)]
BEDROOMS = ["1", "2", "3", "4", "5", "6", "7"]


class ScraperUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PropertyGuru Scraper")
        self.geometry("1180x760")
        self.minsize(1024, 680)

        self.worker_thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.event_queue: queue.Queue = queue.Queue()
        self.form_controls: list[ttk.Widget] = []
        self.status_var = tk.StringVar(value="Ready")
        self.active_scrapers: list[object] = []
        self.active_scrapers_lock = threading.Lock()
        self._last_output_folder: str = ""

        self._build_style()
        self._build_layout()
        self._bind_shortcuts()
        self._load_last_settings()
        self._refresh_preset_list()
        self.after(150, self._poll_events)

    def _build_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        self.configure(bg="#eef2f7")
        style.configure("Sidebar.TFrame", padding=12, background="#dde5ef")
        style.configure("Panel.TLabelframe", padding=10)
        style.configure("Title.TLabel", font=("Helvetica", 18, "bold"), background="#dde5ef")
        style.configure("HeroTitle.TLabel", font=("Helvetica", 20, "bold"))
        style.configure("HeroSub.TLabel", font=("Helvetica", 10))
        style.configure("MetricTitle.TLabel", font=("Helvetica", 10, "bold"))
        style.configure("MetricValue.TLabel", font=("Helvetica", 14, "bold"))
        style.configure("TButton", padding=(8, 6))

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=0)
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=1)

        sidebar = ttk.Frame(self, style="Sidebar.TFrame")
        sidebar.grid(row=0, column=0, sticky="ns")

        main = ttk.Frame(self, padding=12)
        main.grid(row=0, column=1, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.rowconfigure(4, weight=1)

        ttk.Label(sidebar, text="Scraper Controls", style="Title.TLabel").pack(anchor="w", pady=(0, 10))

        self.start_btn = ttk.Button(sidebar, text="Start", command=self.start_scrape)
        self.start_btn.pack(fill="x", pady=4)

        self.stop_btn = ttk.Button(sidebar, text="Stop", command=self.stop_scrape, state="disabled")
        self.stop_btn.pack(fill="x", pady=4)

        ttk.Label(sidebar, text="Site").pack(anchor="w", pady=(8, 0))
        self.scraper_type_var = tk.StringVar(value="Property Guru")
        self.scraper_type_combo = ttk.Combobox(
            sidebar,
            textvariable=self.scraper_type_var,
            values=["Property Guru", "Commercial Guru", "Both (Parallel)"],
            state="readonly",
        )
        self.scraper_type_combo.pack(fill="x", pady=4)
        self.form_controls.append(self.scraper_type_combo)

        ttk.Button(sidebar, text="Clear Logs", command=self.clear_logs).pack(fill="x", pady=4)
        ttk.Button(sidebar, text="Open Output Folder", command=self.open_output_folder).pack(fill="x", pady=4)

        ttk.Separator(sidebar).pack(fill="x", pady=10)

        self.preset_var = tk.StringVar(value="default")
        ttk.Label(sidebar, text="Preset Name").pack(anchor="w")
        self.preset_combo = ttk.Combobox(sidebar, textvariable=self.preset_var, state="normal")
        self.preset_combo.pack(fill="x", pady=(0, 6))

        ttk.Button(sidebar, text="Save Preset", command=self.save_preset).pack(fill="x", pady=2)
        ttk.Button(sidebar, text="Load Preset", command=self.load_preset).pack(fill="x", pady=2)
        ttk.Button(sidebar, text="Refresh Presets", command=self._refresh_preset_list).pack(fill="x", pady=2)

        ttk.Separator(sidebar).pack(fill="x", pady=10)
        ttk.Label(sidebar, text="Shortcuts", style="Title.TLabel").pack(anchor="w", pady=(0, 6))
        ttk.Label(sidebar, text="Cmd/Ctrl+Enter: Start").pack(anchor="w")
        ttk.Label(sidebar, text="Esc: Stop").pack(anchor="w")

        hero = ttk.Frame(main, padding=(12, 8))
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        hero.columnconfigure(0, weight=1)
        ttk.Label(hero, text="Real Estate Scraper Dashboard", style="HeroTitle.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            hero,
            text="Configure filters, run a scrape, and monitor progress in real time.",
            style="HeroSub.TLabel",
        ).grid(row=1, column=0, sticky="w")

        # --- Scrape mode selector ---
        self.scrape_mode_var = tk.StringVar(value="Filter Search")
        self.search_url_var = tk.StringVar()
        self.import_file_var = tk.StringVar()
        self.url_column_var = tk.StringVar(value="URL")
        self.import_output_folder_var = tk.StringVar(value=os.getcwd())

        mode_bar = ttk.LabelFrame(main, text="Scrape Mode", style="Panel.TLabelframe")
        mode_bar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for _mi, _mode_text in enumerate(["Filter Search", "URL Search", "File URL Import"]):
            ttk.Radiobutton(
                mode_bar, text=_mode_text, variable=self.scrape_mode_var,
                value=_mode_text, command=self._on_mode_change,
            ).grid(row=0, column=_mi, padx=12, pady=6, sticky="w")

        form_frame = ttk.LabelFrame(main, text="Search Filters", style="Panel.TLabelframe")
        form_frame.grid(row=2, column=0, sticky="ew")
        form_frame.columnconfigure(0, weight=1)
        form_frame.columnconfigure(1, weight=1)

        self.freetext_var = tk.StringVar()
        self.min_price_var = tk.StringVar(value="1000000")
        self.max_price_var = tk.StringVar(value="2000000")
        self.property_type_var = tk.StringVar()
        self.tenure_var = tk.StringVar()
        self.furnishing_var = tk.StringVar()
        self.extra_params_var = tk.StringVar()
        self.output_var = tk.StringVar(value=os.path.join(os.getcwd(), "scraped_listings.xlsx"))

        ttk.Label(form_frame, text="Free Text Display").grid(row=0, column=0, sticky="w")
        freetext_entry = ttk.Entry(form_frame, textvariable=self.freetext_var)
        freetext_entry.grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))
        self.form_controls.append(freetext_entry)

        ttk.Label(form_frame, text="Output Excel").grid(row=0, column=1, sticky="w")
        output_row = ttk.Frame(form_frame)
        output_row.grid(row=1, column=1, sticky="ew", pady=(0, 8))
        output_row.columnconfigure(0, weight=1)
        self.output_entry = ttk.Entry(output_row, textvariable=self.output_var)
        self.output_entry.grid(row=0, column=0, sticky="ew")
        self.output_browse_btn = ttk.Button(output_row, text="Browse", command=self.pick_output_file)
        self.output_browse_btn.grid(row=0, column=1, padx=(6, 0))
        self.form_controls.extend([self.output_entry, self.output_browse_btn])

        price_row = ttk.Frame(form_frame)
        price_row.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(price_row, text="Min Price").grid(row=0, column=0, sticky="w")
        min_entry = ttk.Entry(price_row, textvariable=self.min_price_var, width=14)
        min_entry.grid(row=1, column=0, padx=(0, 8))
        ttk.Label(price_row, text="Max Price").grid(row=0, column=1, sticky="w")
        max_entry = ttk.Entry(price_row, textvariable=self.max_price_var, width=14)
        max_entry.grid(row=1, column=1)
        self.form_controls.extend([min_entry, max_entry])

        meta_row = ttk.Frame(form_frame)
        meta_row.grid(row=2, column=1, sticky="ew", pady=(0, 8))
        for idx in range(3):
            meta_row.columnconfigure(idx, weight=1)

        ttk.Label(meta_row, text="Property Type (propertyTypeGroup)").grid(row=0, column=0, sticky="w")
        property_type_entry = ttk.Entry(meta_row, textvariable=self.property_type_var)
        property_type_entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
        ttk.Label(meta_row, text="Tenure").grid(row=0, column=1, sticky="w")
        tenure_entry = ttk.Entry(meta_row, textvariable=self.tenure_var)
        tenure_entry.grid(row=1, column=1, sticky="ew", padx=(0, 8))
        ttk.Label(meta_row, text="Furnishing").grid(row=0, column=2, sticky="w")
        furnishing_entry = ttk.Entry(meta_row, textvariable=self.furnishing_var)
        furnishing_entry.grid(row=1, column=2, sticky="ew")
        self.form_controls.extend([property_type_entry, tenure_entry, furnishing_entry])

        ttk.Label(form_frame, text="Extra Params (key=value&key=value)").grid(row=3, column=0, sticky="w")
        extra_entry = ttk.Entry(form_frame, textvariable=self.extra_params_var)
        extra_entry.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        self.form_controls.append(extra_entry)

        selectors = ttk.Frame(form_frame)
        selectors.grid(row=5, column=0, columnspan=2, sticky="ew")
        selectors.columnconfigure(0, weight=3)
        selectors.columnconfigure(1, weight=2)

        district_box = ttk.LabelFrame(selectors, text="District Codes")
        district_box.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        district_box.columnconfigure(0, weight=1)

        district_search_row = ttk.Frame(district_box)
        district_search_row.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        district_search_row.columnconfigure(1, weight=1)
        ttk.Label(district_search_row, text="Search:").grid(row=0, column=0, padx=(0, 6))
        self.district_search_var = tk.StringVar()
        district_search_entry = ttk.Entry(district_search_row, textvariable=self.district_search_var)
        district_search_entry.grid(row=0, column=1, sticky="ew")
        district_search_entry.bind("<KeyRelease>", lambda _: self._filter_district_checkboxes())
        self.form_controls.append(district_search_entry)

        district_grid = ttk.Frame(district_box)
        district_grid.grid(row=1, column=0, sticky="ew")

        district_action_row = ttk.Frame(district_box)
        district_action_row.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.select_all_districts_btn = ttk.Button(
            district_action_row, text="All", command=lambda: self._set_checkbox_group(self.district_vars, True)
        )
        self.select_all_districts_btn.grid(row=0, column=0, padx=(0, 4))
        self.clear_districts_btn = ttk.Button(
            district_action_row, text="None", command=lambda: self._set_checkbox_group(self.district_vars, False)
        )
        self.clear_districts_btn.grid(row=0, column=1)
        self.form_controls.extend([self.select_all_districts_btn, self.clear_districts_btn])

        self.district_vars: dict[str, tk.BooleanVar] = {}
        self.district_checks: dict[str, ttk.Checkbutton] = {}
        for idx, district in enumerate(DISTRICTS):
            var = tk.BooleanVar(value=district in {"D01", "D02"})
            self.district_vars[district] = var
            checkbox = ttk.Checkbutton(district_grid, text=district, variable=var)
            checkbox.grid(
                row=idx // 7, column=idx % 7, sticky="w", padx=2, pady=2
            )
            self.district_checks[district] = checkbox
            self.form_controls.append(checkbox)

        bed_box = ttk.LabelFrame(selectors, text="Bedrooms")
        bed_box.grid(row=0, column=1, sticky="ew")

        bed_action_row = ttk.Frame(bed_box)
        bed_action_row.grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 4))
        self.select_all_beds_btn = ttk.Button(
            bed_action_row, text="All", command=lambda: self._set_checkbox_group(self.bed_vars, True)
        )
        self.select_all_beds_btn.grid(row=0, column=0, padx=(0, 4))
        self.clear_beds_btn = ttk.Button(
            bed_action_row, text="None", command=lambda: self._set_checkbox_group(self.bed_vars, False)
        )
        self.clear_beds_btn.grid(row=0, column=1)
        self.form_controls.extend([self.select_all_beds_btn, self.clear_beds_btn])

        self.bed_vars: dict[str, tk.BooleanVar] = {}
        for idx, bed in enumerate(BEDROOMS):
            var = tk.BooleanVar(value=bed in {"2", "3"})
            self.bed_vars[bed] = var
            label = bed if bed != "7" else "6+"
            checkbox = ttk.Checkbutton(bed_box, text=label, variable=var)
            checkbox.grid(row=(idx // 4) + 1, column=idx % 4, sticky="w")
            self.form_controls.append(checkbox)

        # Save filter panel reference for show/hide on mode switch
        self._filter_panel = form_frame

        # -- URL Search panel (row=2, hidden by default) --
        url_panel = ttk.LabelFrame(main, text="URL Search", style="Panel.TLabelframe")
        url_panel.grid(row=2, column=0, sticky="ew")
        url_panel.columnconfigure(0, weight=1)
        url_panel.grid_remove()
        self._url_panel = url_panel

        ttk.Label(
            url_panel,
            text="Paste a full PropertyGuru or CommercialGuru search results URL:",
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))
        url_search_entry = ttk.Entry(url_panel, textvariable=self.search_url_var)
        url_search_entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
        url_clear_btn = ttk.Button(url_panel, text="Clear", command=lambda: self.search_url_var.set(""))
        url_clear_btn.grid(row=1, column=1)
        self.form_controls.extend([url_search_entry, url_clear_btn])

        ttk.Label(
            url_panel,
            text="Site is auto-detected from URL host in URL Search mode.",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(2, 2))

        ttk.Label(url_panel, text="Output Excel:").grid(row=3, column=0, sticky="w", pady=(8, 0))
        url_out_row = ttk.Frame(url_panel)
        url_out_row.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        url_out_row.columnconfigure(0, weight=1)
        url_out_entry = ttk.Entry(url_out_row, textvariable=self.output_var)
        url_out_entry.grid(row=0, column=0, sticky="ew")
        url_out_browse_btn = ttk.Button(url_out_row, text="Browse", command=self.pick_output_file)
        url_out_browse_btn.grid(row=0, column=1, padx=(6, 0))
        self.form_controls.extend([url_out_entry, url_out_browse_btn])

        # -- File URL Import panel (row=2, hidden by default) --
        file_panel = ttk.LabelFrame(main, text="File URL Import", style="Panel.TLabelframe")
        file_panel.grid(row=2, column=0, sticky="ew")
        file_panel.columnconfigure(1, weight=1)
        file_panel.grid_remove()
        self._file_panel = file_panel

        ttk.Label(
            file_panel,
            text="Select an Excel (.xlsx) or CSV file with listing URLs — one URL per row:",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))
        file_path_entry = ttk.Entry(file_panel, textvariable=self.import_file_var)
        file_path_entry.grid(row=1, column=0, sticky="ew", padx=(0, 6))
        file_browse_btn = ttk.Button(file_panel, text="Browse", command=self._pick_import_file)
        file_browse_btn.grid(row=1, column=1, sticky="w", padx=(0, 12))
        self.form_controls.extend([file_path_entry, file_browse_btn])

        ttk.Label(file_panel, text="URL column name:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        url_col_entry = ttk.Entry(file_panel, textvariable=self.url_column_var, width=20)
        url_col_entry.grid(row=3, column=0, sticky="w", pady=(0, 4))
        self.form_controls.append(url_col_entry)

        ttk.Label(file_panel, text="Output folder for Excel:").grid(row=2, column=1, sticky="w", pady=(8, 0))
        file_out_row = ttk.Frame(file_panel)
        file_out_row.grid(row=3, column=1, columnspan=2, sticky="ew", pady=(0, 4))
        file_out_row.columnconfigure(0, weight=1)
        file_out_entry = ttk.Entry(file_out_row, textvariable=self.import_output_folder_var)
        file_out_entry.grid(row=0, column=0, sticky="ew")
        file_out_browse_btn = ttk.Button(file_out_row, text="Browse", command=self._pick_import_output_folder)
        file_out_browse_btn.grid(row=0, column=1, padx=(6, 0))
        self.form_controls.extend([file_out_entry, file_out_browse_btn])

        advanced = ttk.LabelFrame(main, text="Advanced Settings", style="Panel.TLabelframe")
        advanced.grid(row=3, column=0, sticky="ew", pady=(10, 10))

        self.timeout_var = tk.StringVar(value="25")
        self.retries_var = tk.StringVar(value="2")
        self.max_pages_var = tk.StringVar(value="")
        self.headless_var = tk.BooleanVar(value=False)

        ttk.Label(advanced, text="Timeout (sec)").grid(row=0, column=0, sticky="w")
        timeout_entry = ttk.Entry(advanced, textvariable=self.timeout_var, width=10)
        timeout_entry.grid(row=1, column=0, padx=(0, 10))
        ttk.Label(advanced, text="Retries").grid(row=0, column=1, sticky="w")
        retries_entry = ttk.Entry(advanced, textvariable=self.retries_var, width=10)
        retries_entry.grid(row=1, column=1, padx=(0, 10))
        ttk.Label(advanced, text="Max Pages (optional)").grid(row=0, column=2, sticky="w")
        max_pages_entry = ttk.Entry(advanced, textvariable=self.max_pages_var, width=12)
        max_pages_entry.grid(row=1, column=2, padx=(0, 10))
        headless_check = ttk.Checkbutton(advanced, text="Headless", variable=self.headless_var)
        headless_check.grid(row=1, column=3, sticky="w")
        self.form_controls.extend([timeout_entry, retries_entry, max_pages_entry, headless_check])

        progress_frame = ttk.LabelFrame(main, text="Progress", style="Panel.TLabelframe")
        progress_frame.grid(row=4, column=0, sticky="nsew")
        progress_frame.columnconfigure(0, weight=1)
        progress_frame.rowconfigure(3, weight=1)

        metric_row = ttk.Frame(progress_frame)
        metric_row.grid(row=0, column=0, sticky="ew")
        for i in range(5):
            metric_row.columnconfigure(i, weight=1)

        self.current_page_label = tk.StringVar(value="0")
        self.total_pages_label = tk.StringVar(value="0")
        self.processed_label = tk.StringVar(value="0/0")
        self.elapsed_label = tk.StringVar(value="0s")
        self.error_label = tk.StringVar(value="0")

        self._metric_card(metric_row, 0, "Current Page", self.current_page_label)
        self._metric_card(metric_row, 1, "Total Pages", self.total_pages_label)
        self._metric_card(metric_row, 2, "Listings", self.processed_label)
        self._metric_card(metric_row, 3, "Elapsed", self.elapsed_label)
        self._metric_card(metric_row, 4, "Errors", self.error_label)

        self.progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(8, 8))

        ttk.Label(progress_frame, text="Live Logs", style="MetricTitle.TLabel").grid(
            row=2, column=0, sticky="w", pady=(0, 6)
        )

        log_frame = ttk.Frame(progress_frame)
        log_frame.grid(row=3, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(
            log_frame,
            wrap="word",
            height=20,
            state="disabled",
            bg="#111827",
            fg="#e5e7eb",
            insertbackground="#e5e7eb",
            relief="flat",
            padx=8,
            pady=8,
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        status_bar = ttk.Label(self, textvariable=self.status_var, anchor="w", padding=(10, 4))
        status_bar.grid(row=1, column=0, columnspan=2, sticky="ew")

    def _metric_card(self, parent: ttk.Frame, column: int, title: str, value_var: tk.StringVar) -> None:
        card = ttk.LabelFrame(parent, text="", padding=(8, 6))
        card.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 6, 0))
        ttk.Label(card, text=title, style="MetricTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(card, textvariable=value_var, style="MetricValue.TLabel").grid(row=1, column=0, sticky="w")

    def _bind_shortcuts(self) -> None:
        self.bind_all("<Control-Return>", lambda _: self.start_scrape())
        self.bind_all("<Command-Return>", lambda _: self.start_scrape())
        self.bind_all("<Escape>", lambda _: self.stop_scrape())

    def _set_checkbox_group(self, variables: dict[str, tk.BooleanVar], selected: bool) -> None:
        for var in variables.values():
            var.set(selected)

    def _filter_district_checkboxes(self) -> None:
        query = self.district_search_var.get().strip().upper()
        for district, checkbox in self.district_checks.items():
            if not query or query in district:
                checkbox.grid()
            else:
                checkbox.grid_remove()

    def _refresh_preset_list(self) -> None:
        preset_names: list[str] = []
        if os.path.exists(PRESETS_FILE):
            try:
                with open(PRESETS_FILE, "r", encoding="utf-8") as f:
                    presets = json.load(f)
                preset_names = sorted(presets.keys())
            except Exception:
                preset_names = []
        self.preset_combo.configure(values=preset_names)

    def pick_output_file(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Select output Excel file",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile="scraped_listings.xlsx",
        )
        if path:
            self.output_var.set(path)

    def _collect_selected_districts(self) -> list[str]:
        return [code for code, var in self.district_vars.items() if var.get()]

    def _collect_selected_bedrooms(self) -> list[str]:
        return [bed for bed, var in self.bed_vars.items() if var.get()]

    def _parse_extra_params(self, raw: str) -> dict[str, list[str]]:
        parsed: dict[str, list[str]] = {}
        if not raw.strip():
            return parsed

        for key, value in parse_qsl(raw.strip(), keep_blank_values=True):
            parsed.setdefault(key, []).append(value)
        return parsed

    def _validate_form(self) -> tuple[bool, str]:
        mode = self.scrape_mode_var.get()

        if mode == "URL Search":
            url = self.search_url_var.get().strip()
            if not url:
                return False, "Please paste a search URL."
            if not url.startswith("http"):
                return False, "URL must start with http:// or https://"
            if not self.output_var.get().strip():
                return False, "Output Excel path is required."
            return True, ""

        if mode == "File URL Import":
            fp = self.import_file_var.get().strip()
            if not fp:
                return False, "Please select an Excel or CSV file."
            if not os.path.isfile(fp):
                return False, f"File not found: {fp}"
            if not self.url_column_var.get().strip():
                return False, "URL column name cannot be empty."
            if not self.import_output_folder_var.get().strip():
                return False, "Please select an output folder."
            return True, ""

        # Filter Search mode
        if not self.freetext_var.get().strip():
            return False, "Free Text Display is required."

        selected_scraper = self.scraper_type_var.get()
        if selected_scraper in {"Property Guru", "Both (Parallel)"}:
            districts = self._collect_selected_districts()
            if not districts:
                return False, "Select at least one district code."

            bedrooms = self._collect_selected_bedrooms()
            if not bedrooms:
                return False, "Select at least one bedroom option."

        try:
            min_price = int(self.min_price_var.get().strip())
            max_price = int(self.max_price_var.get().strip())
        except ValueError:
            return False, "Min Price and Max Price must be integers."

        if min_price < 0 or max_price < 0:
            return False, "Prices cannot be negative."

        if min_price > max_price:
            return False, "Min Price cannot be greater than Max Price."

        try:
            timeout = int(self.timeout_var.get().strip())
            retries = int(self.retries_var.get().strip())
        except ValueError:
            return False, "Timeout and Retries must be integers."

        if timeout <= 0:
            return False, "Timeout must be greater than 0."

        if retries < 0:
            return False, "Retries cannot be negative."

        max_pages_raw = self.max_pages_var.get().strip()
        if max_pages_raw:
            try:
                max_pages = int(max_pages_raw)
            except ValueError:
                return False, "Max Pages must be an integer."
            if max_pages <= 0:
                return False, "Max Pages must be greater than 0."

        output_path = self.output_var.get().strip()
        if not output_path:
            return False, "Output Excel path is required."

        try:
            self._parse_extra_params(self.extra_params_var.get())
        except Exception:
            return False, "Extra Params must use key=value&key=value format."

        return True, ""

    def _build_config_from_url(self, raw_url: str, output_path: str) -> tuple[ScraperConfig, str]:
        """Parse a full search-results URL into a ScraperConfig + auto-detected site name."""
        parsed = urlparse(raw_url.strip())
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        params = parse_qs(parsed.query, keep_blank_values=True)

        # Auto-detect site from hostname
        host = parsed.netloc.lower()
        if "commercialguru" in host:
            site = "Commercial Guru"
        elif "propertyguru" in host:
            site = "Property Guru"
        else:
            raise ValueError(
                "Unsupported URL host. Please use a PropertyGuru or CommercialGuru search URL."
            )

        # Keys that map to explicit ScraperConfig fields (case-insensitive match)
        KNOWN_LOWER = {
            "_freetextdisplay", "districtcode", "bedrooms",
            "minprice", "maxprice", "propertytypegroup",
            "tenure", "furnishing", "page",
        }

        def _get(key: str) -> list[str]:
            """Case-insensitive param lookup, returning list of values."""
            for k, v in params.items():
                if k.lower() == key.lower():
                    return v
            return []

        freetext = (_get("_freetextDisplay") or [""])[0]
        district_codes = _get("districtCode")
        bedrooms = _get("bedrooms")

        try:
            min_price = int((_get("minPrice") or ["0"])[0])
        except (ValueError, IndexError):
            min_price = 0

        try:
            max_price = int((_get("maxPrice") or ["99999999"])[0])
        except (ValueError, IndexError):
            max_price = 99999999

        property_type = (_get("propertyTypeGroup") or [""])[0]
        tenure = (_get("tenure") or [""])[0]
        furnishing = (_get("furnishing") or [""])[0]

        # Everything not mapped and not 'page' goes into extra_params (preserve original casing)
        extra_params: dict[str, list[str]] = {}
        for key, values in params.items():
            if key.lower() not in KNOWN_LOWER:
                extra_params[key] = values

        try:
            timeout_sec = int(self.timeout_var.get().strip())
        except ValueError:
            timeout_sec = 25
        try:
            retries = int(self.retries_var.get().strip())
        except ValueError:
            retries = 2
        max_pages_raw = self.max_pages_var.get().strip()
        max_pages = int(max_pages_raw) if max_pages_raw else None

        config = ScraperConfig(
            freetext_display=freetext,
            district_codes=list(district_codes),
            bedrooms=list(bedrooms),
            min_price=min_price,
            max_price=max_price,
            output_csv=output_path,
            property_type=property_type,
            tenure=tenure,
            furnishing=furnishing,
            extra_params=extra_params,
            timeout_sec=timeout_sec,
            retries=retries,
            headless=self.headless_var.get(),
            max_pages=max_pages,
            base_url=base_url,
        )
        return config, site

    def _build_config(self) -> ScraperConfig:
        max_pages = self.max_pages_var.get().strip()
        return ScraperConfig(
            freetext_display=self.freetext_var.get().strip(),
            district_codes=self._collect_selected_districts(),
            bedrooms=self._collect_selected_bedrooms(),
            min_price=int(self.min_price_var.get().strip()),
            max_price=int(self.max_price_var.get().strip()),
            output_csv=self.output_var.get().strip(),
            property_type=self.property_type_var.get().strip(),
            tenure=self.tenure_var.get().strip(),
            furnishing=self.furnishing_var.get().strip(),
            extra_params=self._parse_extra_params(self.extra_params_var.get()),
            timeout_sec=int(self.timeout_var.get().strip()),
            retries=int(self.retries_var.get().strip()),
            headless=self.headless_var.get(),
            max_pages=int(max_pages) if max_pages else None,
        )

    def _build_scraper(self, scraper_name: str, config: ScraperConfig, tag: str):
        def prefixed_log(msg: str) -> None:
            self.event_queue.put(("log", f"[{tag}] {msg}"))

        if scraper_name == "Commercial Guru":
            return CommercialGuruScraper(
                config=config,
                log_callback=prefixed_log,
                progress_callback=lambda data: self.event_queue.put(("progress", data)),
                stop_requested=lambda: self.stop_event.is_set(),
            )

        return PropertyGuruScraper(
            config=config,
            log_callback=prefixed_log,
            progress_callback=lambda data: self.event_queue.put(("progress", data)),
            stop_requested=lambda: self.stop_event.is_set(),
        )

    def _clean_base_output_name(self, existing_path: str) -> str:
        existing = os.path.basename(existing_path.strip())
        base = os.path.splitext(existing)[0] if existing else "scrape_results"

        # Strip repeated trailing segments like _propertyguru_YYYYMMDD_HHMMSS.
        pattern = re.compile(r"_(?:propertyguru|commercialguru)_\d{8}_\d{6}$", re.IGNORECASE)
        while True:
            cleaned = pattern.sub("", base)
            if cleaned == base:
                break
            base = cleaned

        base = base.strip("._-")
        return base or "scrape_results"

    def _prompt_output_targets(self, mode: str) -> dict[str, str] | None:
        default_name = self._clean_base_output_name(self.output_var.get())

        raw_name = simpledialog.askstring(
            "Excel Name",
            "Enter base name for Excel file(s):",
            initialvalue=default_name,
            parent=self,
        )
        if not raw_name:
            return None

        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_name.strip()).strip("._-")
        if not safe_name:
            messagebox.showerror("Invalid Name", "Please enter a valid file name.")
            return None

        initial_dir = os.path.dirname(self.output_var.get().strip()) or os.getcwd()
        folder = filedialog.askdirectory(
            title="Select output folder",
            initialdir=initial_dir,
            mustexist=True,
        )
        if not folder:
            return None

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if mode == "Both (Parallel)":
            outputs = {
                "Property Guru": os.path.join(folder, f"{safe_name}_propertyguru_{ts}.xlsx"),
                "Commercial Guru": os.path.join(folder, f"{safe_name}_commercialguru_{ts}.xlsx"),
            }
            self.output_var.set(outputs["Property Guru"])
            return outputs

        suffix = "propertyguru" if mode == "Property Guru" else "commercialguru"
        path = os.path.join(folder, f"{safe_name}_{suffix}_{ts}.xlsx")
        self.output_var.set(path)
        return {mode: path}

    def _clone_config_with_output(self, base_config: ScraperConfig, output_path: str) -> ScraperConfig:
        cfg = deepcopy(base_config)
        cfg.output_csv = output_path
        return cfg

    def _set_running_state(self, running: bool) -> None:
        self.start_btn.configure(state="disabled" if running else "normal")
        self.stop_btn.configure(state="normal" if running else "disabled")
        for control in self.form_controls:
            try:
                control.configure(state="disabled" if running else "normal")
            except Exception:
                pass
        self.preset_combo.configure(state="disabled" if running else "normal")
        self.status_var.set("Running scrape..." if running else "Ready")
        if not running:
            self._on_mode_change()

    def start_scrape(self) -> None:
        ok, message = self._validate_form()
        if not ok:
            messagebox.showerror("Validation Error", message)
            return

        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Running", "Scraper is already running.")
            return

        scrape_mode = self.scrape_mode_var.get()

        if scrape_mode == "File URL Import":
            import_file = self.import_file_var.get().strip()
            url_col = self.url_column_var.get().strip()
            out_folder = self.import_output_folder_var.get().strip()

            try:
                urls = read_urls_from_file(import_file, url_col)
            except Exception as exc:
                messagebox.showerror("File Read Error", str(exc))
                return

            if not urls:
                messagebox.showerror(
                    "No URLs Found",
                    f"No rows starting with 'http' were found in column '{url_col}'.\n"
                    "Check the column name and file contents.",
                )
                return

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(import_file))[0]
            safe_base = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._-") or "scraped"
            output_excel = os.path.join(out_folder, f"{safe_base}_listings_{ts}.xlsx")

            try:
                timeout_sec = int(self.timeout_var.get().strip())
            except ValueError:
                timeout_sec = 25
            try:
                retries = int(self.retries_var.get().strip())
            except ValueError:
                retries = 2

            driver_config = ScraperConfig(
                freetext_display="",
                district_codes=[],
                bedrooms=[],
                min_price=0,
                max_price=0,
                output_csv=output_excel,
                timeout_sec=timeout_sec,
                retries=retries,
                headless=self.headless_var.get(),
            )

            self.stop_event.clear()
            self.clear_progress()
            self._last_output_folder = out_folder
            self._set_running_state(True)
            self._append_log(
                f"Starting File URL Import — {len(urls)} URL(s) from: {import_file}"
            )
            self._append_log(f"Output Excel: {output_excel}")
            self._save_last_settings()

            def _file_worker() -> None:
                try:
                    scraper = DirectListingScraper(
                        urls=urls,
                        config=driver_config,
                        log_callback=lambda msg: self.event_queue.put(("log", msg)),
                        progress_callback=lambda d: self.event_queue.put(("progress", d)),
                        stop_requested=lambda: self.stop_event.is_set(),
                    )
                    with self.active_scrapers_lock:
                        self.active_scrapers = [scraper]
                    result = scraper.run()
                    result["output_csv"] = output_excel
                    result["mode"] = "file_import"
                    result["cancelled"] = self.stop_event.is_set()
                    self.event_queue.put(("done", result))
                except Exception as exc:
                    self.event_queue.put(("error", str(exc)))
                finally:
                    with self.active_scrapers_lock:
                        self.active_scrapers = []

            self.worker_thread = threading.Thread(target=_file_worker, daemon=True)
            self.worker_thread.start()
            return

        # --- Build config and determine target scraper ---
        if scrape_mode == "URL Search":
            try:
                config, mode = self._build_config_from_url(
                    self.search_url_var.get().strip(), ""
                )
            except Exception as exc:
                messagebox.showerror("URL Parse Error", str(exc))
                return
            self._append_log(f"Parsed URL → auto-detected site: {mode}")
            self._append_log("URL Search ignores the Site selector and uses URL hostname.")
            if config.freetext_display:
                self._append_log(f"Free text: {config.freetext_display}")
            if config.district_codes:
                self._append_log(f"Districts: {', '.join(config.district_codes)}")
            if config.extra_params:
                extras = ", ".join(f"{k}={v}" for k, vs in config.extra_params.items() for v in vs)
                self._append_log(f"Extra params: {extras}")
        else:
            mode = self.scraper_type_var.get()
            config = self._build_config()

        output_targets = self._prompt_output_targets(mode)
        if not output_targets:
            self._append_log("Run canceled: output name/folder was not provided.")
            return

        # For URL Search the output path isn't set until after the file dialog
        if scrape_mode == "URL Search":
            first_path = next(iter(output_targets.values()))
            config = self._clone_config_with_output(config, first_path)

        self.stop_event.clear()
        self.clear_progress()
        self._last_output_folder = (
            os.path.dirname(next(iter(output_targets.values()))) or os.getcwd()
        )
        self._set_running_state(True)
        self._append_log(f"Starting scrape ({mode} • {scrape_mode})...")
        for tag, path in output_targets.items():
            self._append_log(f"[{tag}] Excel output: {path}")
        self._save_last_settings()

        def worker() -> None:
            try:
                is_parallel = mode == "Both (Parallel)"

                if is_parallel:
                    specs = ["Property Guru", "Commercial Guru"]
                else:
                    specs = [mode]

                results: dict[str, dict] = {}
                errors: list[str] = []
                threads: list[threading.Thread] = []
                worker_state_lock = threading.Lock()
                with self.active_scrapers_lock:
                    self.active_scrapers = []

                def run_one(tag: str) -> None:
                    try:
                        output_path = output_targets[tag]
                        cfg = self._clone_config_with_output(config, output_path)
                        scraper = self._build_scraper(tag, cfg, tag)
                        with self.active_scrapers_lock:
                            self.active_scrapers.append(scraper)
                        result = scraper.run()
                        result["output_csv"] = output_path
                        with worker_state_lock:
                            results[tag] = result
                    except Exception as exc:
                        if self.stop_event.is_set():
                            self.event_queue.put(("log", f"[{tag}] Stopped by user."))
                        else:
                            with worker_state_lock:
                                errors.append(f"{tag}: {exc}")

                for tag in specs:
                    t = threading.Thread(target=run_one, args=(tag,), daemon=True)
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                if errors:
                    raise RuntimeError(" | ".join(errors))

                if is_parallel:
                    summary = {
                        "total_pages": sum(r.get("total_pages", 0) for r in results.values()),
                        "processed": sum(r.get("processed", 0) for r in results.values()),
                        "total_links": sum(r.get("total_links", 0) for r in results.values()),
                        "errors": sum(r.get("errors", 0) for r in results.values()),
                        "elapsed": max((r.get("elapsed", 0) for r in results.values()), default=0),
                        "details": results,
                        "cancelled": self.stop_event.is_set(),
                    }
                else:
                    summary = next(iter(results.values()), {
                        "total_pages": 0,
                        "processed": 0,
                        "total_links": 0,
                        "errors": 0,
                        "elapsed": 0,
                        "output_csv": config.output_csv,
                    })
                    summary["cancelled"] = self.stop_event.is_set()

                self.event_queue.put(("done", summary))
            except Exception as exc:
                self.event_queue.put(("error", str(exc)))
            finally:
                with self.active_scrapers_lock:
                    self.active_scrapers = []

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def stop_scrape(self) -> None:
        if not (self.worker_thread and self.worker_thread.is_alive()):
            return
        self.stop_event.set()
        with self.active_scrapers_lock:
            active = list(self.active_scrapers)
        for scraper in active:
            request_stop = getattr(scraper, "request_stop", None)
            if callable(request_stop):
                request_stop(force=True)
        self.status_var.set("Stop requested...")
        self._append_log("Stop requested. Terminating browser session now.")

    def clear_logs(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def clear_progress(self) -> None:
        self.current_page_label.set("0")
        self.total_pages_label.set("0")
        self.processed_label.set("0/0")
        self.elapsed_label.set("0s")
        self.error_label.set("0")
        self.progress_bar.configure(value=0, maximum=1)

    def _append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _poll_events(self) -> None:
        try:
            while True:
                kind, payload = self.event_queue.get_nowait()
                if kind == "log":
                    self._append_log(payload)
                elif kind == "progress":
                    self._update_progress(payload)
                elif kind == "done":
                    self._set_running_state(False)
                    if payload.get("cancelled"):
                        self.status_var.set("Stopped")
                    details = payload.get("details")
                    if isinstance(details, dict):
                        self._append_log("Completed in parallel mode:")
                        for tag, result in details.items():
                            self._append_log(
                                f"{tag}: Listings {result.get('processed', 0)}/{result.get('total_links', 0)}, "
                                f"Errors {result.get('errors', 0)}, Elapsed {result.get('elapsed', 0)}s, "
                                f"Excel {result.get('output_csv', '')}"
                            )
                    elif payload.get("mode") == "file_import":
                        self._append_log(
                            f"File import complete. "
                            f"URLs scraped: {payload.get('processed', 0)}/{payload.get('total_links', 0)}, "
                            f"Errors: {payload.get('errors', 0)}, "
                            f"Elapsed: {payload.get('elapsed', 0)}s"
                        )
                        self._append_log(f"Excel: {payload.get('output_csv', '')}")
                    else:
                        self._append_log(
                            "Completed. "
                            f"Pages: {payload.get('total_pages', 0)}, "
                            f"Listings: {payload.get('processed', 0)}/{payload.get('total_links', 0)}, "
                            f"Errors: {payload.get('errors', 0)}, "
                            f"Elapsed: {payload.get('elapsed', 0)}s"
                        )
                        self._append_log(f"Excel: {payload.get('output_csv', self.output_var.get())}")
                    if payload.get("cancelled"):
                        self._append_log("Run stopped by user.")
                    else:
                        self.status_var.set("Completed")
                        if payload.get("mode") == "file_import":
                            msg = (
                                f"{payload.get('processed', 0)}/{payload.get('total_links', 0)} "
                                f"URL(s) scraped, {payload.get('errors', 0)} error(s).\n"
                                f"Excel: {payload.get('output_csv', '')}"
                            )
                            messagebox.showinfo("Import Complete", msg)
                        else:
                            messagebox.showinfo("Done", "Scraping finished.")
                elif kind == "error":
                    self._set_running_state(False)
                    self._append_log(f"Error: {payload}")
                    self.status_var.set("Error")
                    messagebox.showerror("Scraper Error", payload)
        except queue.Empty:
            pass

        self.after(150, self._poll_events)

    def _update_progress(self, data: dict) -> None:
        current_page = data.get("current_page", 0)
        total_pages = data.get("total_pages", 0)
        processed = data.get("listings_processed", 0)
        total_listings = data.get("total_listings", 0)
        errors = data.get("error_count", 0)
        elapsed = data.get("elapsed", 0)

        self.current_page_label.set(str(current_page))
        self.total_pages_label.set(str(total_pages))
        self.processed_label.set(f"{processed}/{total_listings}")
        self.error_label.set(str(errors))
        self.elapsed_label.set(f"{elapsed}s")

        max_val = max(total_listings, 1)
        self.progress_bar.configure(maximum=max_val)
        self.progress_bar.configure(value=min(processed, max_val))

    def open_output_folder(self) -> None:
        folder = (
            self._last_output_folder
            or os.path.dirname(self.output_var.get().strip())
            or os.getcwd()
        )
        if not os.path.isdir(folder):
            folder = os.getcwd()
        try:
            os.startfile(folder)  # Windows
        except Exception:
            try:
                import subprocess
                subprocess.Popen(["explorer", folder])
            except Exception:
                pass

    def _get_form_data(self) -> dict:
        return {
            "scrape_mode": self.scrape_mode_var.get(),
            "search_url": self.search_url_var.get(),
            "import_file_path": self.import_file_var.get(),
            "url_column": self.url_column_var.get(),
            "import_output_folder": self.import_output_folder_var.get(),
            "freetext_display": self.freetext_var.get(),
            "scraper_type": self.scraper_type_var.get(),
            "district_codes": self._collect_selected_districts(),
            "bedrooms": self._collect_selected_bedrooms(),
            "min_price": self.min_price_var.get(),
            "max_price": self.max_price_var.get(),
            "property_type": self.property_type_var.get(),
            "tenure": self.tenure_var.get(),
            "furnishing": self.furnishing_var.get(),
            "extra_params": self.extra_params_var.get(),
            "output_csv": self.output_var.get(),
            "timeout_sec": self.timeout_var.get(),
            "retries": self.retries_var.get(),
            "max_pages": self.max_pages_var.get(),
            "headless": self.headless_var.get(),
        }

    def _apply_form_data(self, data: dict) -> None:
        self.scrape_mode_var.set(data.get("scrape_mode", "Filter Search"))
        self.search_url_var.set(data.get("search_url", ""))
        self.import_file_var.set(data.get("import_file_path", ""))
        self.url_column_var.set(data.get("url_column", "URL"))
        self.import_output_folder_var.set(data.get("import_output_folder", os.getcwd()))
        self.scraper_type_var.set(data.get("scraper_type", "Property Guru"))
        self.freetext_var.set(data.get("freetext_display", ""))
        self.min_price_var.set(str(data.get("min_price", "1000000")))
        self.max_price_var.set(str(data.get("max_price", "2000000")))
        self.property_type_var.set(data.get("property_type", ""))
        self.tenure_var.set(data.get("tenure", ""))
        self.furnishing_var.set(data.get("furnishing", ""))
        self.extra_params_var.set(data.get("extra_params", ""))
        self.output_var.set(data.get("output_csv", os.path.join(os.getcwd(), "scraped_listings.xlsx")))
        self.timeout_var.set(str(data.get("timeout_sec", "25")))
        self.retries_var.set(str(data.get("retries", "2")))
        self.max_pages_var.set(str(data.get("max_pages", "")))
        self.headless_var.set(bool(data.get("headless", False)))

        selected_districts = set(data.get("district_codes", []))
        for code, var in self.district_vars.items():
            var.set(code in selected_districts)

        selected_beds = set(str(x) for x in data.get("bedrooms", []))
        for bed, var in self.bed_vars.items():
            var.set(bed in selected_beds)
        self._on_mode_change()

    def save_preset(self) -> None:
        name = self.preset_var.get().strip() or simpledialog.askstring("Preset", "Preset name")
        if not name:
            return

        presets = {}
        if os.path.exists(PRESETS_FILE):
            try:
                with open(PRESETS_FILE, "r", encoding="utf-8") as f:
                    presets = json.load(f)
            except Exception:
                presets = {}

        presets[name] = self._get_form_data()
        with open(PRESETS_FILE, "w", encoding="utf-8") as f:
            json.dump(presets, f, indent=2)

        self._append_log(f"Preset saved: {name}")

    def load_preset(self) -> None:
        if not os.path.exists(PRESETS_FILE):
            messagebox.showwarning("No Presets", "No presets file found.")
            return

        with open(PRESETS_FILE, "r", encoding="utf-8") as f:
            presets = json.load(f)

        if not presets:
            messagebox.showwarning("No Presets", "Presets file is empty.")
            return

        name = self.preset_var.get().strip()
        if not name:
            name = simpledialog.askstring("Load Preset", "Preset name")

        if not name or name not in presets:
            available = ", ".join(sorted(presets.keys()))
            messagebox.showwarning("Preset Missing", f"Preset not found. Available: {available}")
            return

        self._apply_form_data(presets[name])
        self._append_log(f"Preset loaded: {name}")
        self.status_var.set(f"Loaded preset: {name}")

    def _save_last_settings(self) -> None:
        with open(LAST_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(self._get_form_data(), f, indent=2)

    def _load_last_settings(self) -> None:
        if not os.path.exists(LAST_SETTINGS_FILE):
            self.freetext_var.set("D01 Boat Quay / Raffles Place / Marina, D02 Chinatown / Tanjong Pagar")
            return

        try:
            with open(LAST_SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._apply_form_data(data)
            self.status_var.set("Loaded last settings")
        except Exception:
            pass


    def _on_mode_change(self) -> None:
        mode = self.scrape_mode_var.get()
        if mode == "Filter Search":
            self._filter_panel.grid()
            self._url_panel.grid_remove()
            self._file_panel.grid_remove()
            self.scraper_type_combo.configure(state="readonly")
        elif mode == "URL Search":
            self._filter_panel.grid_remove()
            self._url_panel.grid()
            self._file_panel.grid_remove()
            self.scraper_type_combo.configure(state="disabled")
        elif mode == "File URL Import":
            self._filter_panel.grid_remove()
            self._url_panel.grid_remove()
            self._file_panel.grid()
            self.scraper_type_combo.configure(state="disabled")

    def _pick_import_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select Excel or CSV file with listing URLs",
            filetypes=[
                ("Excel files", "*.xlsx"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.import_file_var.set(path)

    def _pick_import_output_folder(self) -> None:
        folder = filedialog.askdirectory(
            title="Select output folder for Excel",
            initialdir=self.import_output_folder_var.get() or os.getcwd(),
            mustexist=True,
        )
        if folder:
            self.import_output_folder_var.set(folder)


def run_app() -> None:
    app = ScraperUI()
    app.mainloop()
