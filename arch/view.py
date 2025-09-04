import sys
import os
import pandas as pd
import re
import json
from datetime import datetime
import numpy as np
import csv

# Importy dla PyQt6
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableView, QFileDialog, QSplitter, QComboBox, QMessageBox,
    QInputDialog, QDialog, QTextEdit, QLabel, QDialogButtonBox,
    QAbstractItemView, QCheckBox, QDockWidget, QListWidget, QListWidgetItem
)
from PyQt6.QtGui import QAction, QKeySequence, QColor, QFont
from PyQt6.QtCore import QAbstractTableModel, Qt, QModelIndex, QItemSelection, QItemSelectionModel

# Poprawny backend Matplotlib dla PyQt6
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
from matplotlib.widgets import RectangleSelector
from matplotlib.dates import num2date
import matplotlib.dates as mdates

"""
================================================================================
 Aplikacja do interaktywnej korekty danych czasowych z plików CSV
================================================================================

---------------------------------
 Przeznaczenie
---------------------------------
Aplikacja okienkowa napisana w Pythonie z użyciem biblioteki PyQt6,
umożliwiająca interaktywną wizualizację i korektę znaczników czasu (timestamp)
w zbiorach danych pochodzących z wielu plików CSV jednocześnie. Główne
zastosowanie to naprawa ciągłości chronologicznej w danych z rejestratorów DataTaker,
w których doszło do nagłej zmiany systemowego zegara czasu rzeczywistego,
oraz ogólna analiza i czyszczenie danych czasowych.

---------------------------------
 Główne Funkcje
---------------------------------
--- Zarządzanie Danymi ---
* Wczytywanie danych z jednego lub wielu plików CSV.
* Dodawanie kolejnych plików do już załadowanego zbioru bez utraty danych.
* Dwie metody ładowania plików: przez systemowe okno dialogowe lub przez
  wklejenie listy ścieżek dostępu.
* Możliwość całkowitego wyczyszczenia pamięci aplikacji z danych.
* Zaawansowane opcje zapisu: "Zapisz jako..." (do nowego, połączonego pliku)
  oraz "Nadpisz oryginalne" (zapisuje zmiany z powrotem do plików źródłowych).

--- Panel Plików i Widoczność ---
* Dedykowany panel boczny z listą wszystkich załadowanych plików.
* Automatyczne przypisywanie unikalnych kolorów do serii danych z każdego pliku.
* Checkboxy przy każdej nazwie pliku pozwalające na dynamiczne
  ukrywanie i pokazywanie danych serii na wykresie i w tabeli.
* Zmiana kolejności serii danych na wykresie poprzez przeciąganie ("drag-and-drop")
  plików na liście i zatwierdzanie przyciskiem "Zastosuj kolejność".
* Automatyczne pogrubianie nazwy pliku, którego dane są aktualnie
  widoczne w przewijanym oknie tabeli.

--- Interaktywny Wykres i Tabela ---
* Interaktywna wizualizacja danych w formie wykresu czasowego.
* Pełna nawigacja na wykresie (zoom, przesuwanie, powrót do domu) za pomocą
  myszy oraz skrótów klawiaturowych (Z - Zoom, P - Pan, D - Home, etc.).
* Zachowanie poziomu zoomu podczas zaznaczania i korygowania danych.
* Przycisk "Dopasuj Widok" do resetowania zoomu i dopasowania osi do
  aktualnie widocznych danych.
* Opcjonalne etykiety (tooltips) wyświetlające dokładne dane punktu po
  najechaniu na niego myszką (włączane za pomocą checkboxa).
* Synchronizacja przewijania tabeli z widokiem na wykresie.

--- Manipulacja Danymi ---
* Zaznaczanie punktów do modyfikacji bezpośrednio na wykresie (pojedynczo lub
  prostokątem) oraz w tabeli (pojedyncze kliknięcia).
* Możliwość trwałego usuwania zaznaczonych punktów (po potwierdzeniu).
* Precyzyjna korekta czasu dla zaznaczonych wierszy przez dodanie lub
  odjęcie określonego interwału (dni, godziny, minuty).
* Funkcja obliczania różnicy czasu między dwoma zaznaczonymi punktami.
* Przycisk do szybkiego czyszczenia całego zaznaczenia.
* Automatyczne, szczegółowe logowanie operacji korekty do pliku `tscorrections.jsonl`
  w czytelnym formacie JSON Lines, idealnym do audytu i dalszego przetwarzania.

---------------------------------
 Wymagania Systemowe
---------------------------------
* Python w wersji 3.8+
* Biblioteki, które można zainstalować za pomocą pip:
  
  pip install pandas matplotlib PyQt6 numpy

---------------------------------
 Sposób Użycia
---------------------------------
1.  **Instalacja:** Upewnij się, że masz zainstalowane wszystkie wymagane
    biblioteki, wykonując powyższą komendę w terminalu.

2.  **Uruchomienie:** Uruchom skrypt z terminala za pomocą komendy:
    `python3 view.py` (lub `python view.py` w zależności od konfiguracji)

3.  **Ładowanie Danych:**
    * Użyj przycisków w panelu "Załadowane Pliki" po prawej stronie.
    * "Wczytaj..." otwiera systemowe okno wyboru plików.
    * "Wklej ścieżki..." otwiera okno do wklejenia listy ścieżek.
    * Kolejne operacje wczytywania dodają nowe pliki do już istniejących.
    * Użyj "Wyczyść wszystko", aby zacząć od nowa.

4.  **Zarządzanie Widocznością i Kolejnością:**
    * W panelu "Załadowane Pliki" odznaczaj checkboxy, aby tymczasowo ukryć
      dane z wybranych plików na wykresie i w tabeli.
    * Przeciągnij nazwy plików, aby zmienić ich kolejność, a następnie kliknij
      "Zastosuj kolejność", aby odświeżyć dane i kolejność rysowania na wykresie.

5.  **Interakcja:**
    * Wybierz kolumnę do wizualizacji z listy rozwijanej na górnym pasku.
    * Użyj paska narzędzi nad wykresem lub skrótów klawiaturowych do nawigacji.
    * Kliknij "Dopasuj Widok", aby zresetować zoom do aktualnie widocznych serii.
    * Zaznacz "Pokaż etykiety", aby aktywować podpowiedzi na wykresie.
    * Klikaj na punkty (na wykresie lub w tabeli), aby zaznaczyć je do korekty.
    * Użyj "Usuń zaznaczone", aby trwale usunąć punkty z sesji.

6.  **Logi i Zapis:**
    * Po każdej operacji korekty czasu, w folderze z aplikacją zostanie
      stworzony/zaktualizowany plik `tscorrections.jsonl`.
    * Użyj przycisku "Zapisz dane...", aby zapisać efekt swojej pracy,
      nadpisując oryginalne pliki lub tworząc nowy, połączony plik.
      
7.  **Wyjaśnienie pól loga (tscorrections.jsonl):**
**log_timestamp**: Dokładny czas wykonania operacji, w standardowym formacie ISO 8601.
**timedelta_applied_str**: Czytelna dla człowieka reprezentacja zastosowanego/
    przesunięcia.
**timedelta_total_seconds**: To samo przesunięcie, ale wyrażone w sekundach./
    Do dalszych obliczeń w innych programach (wartość ujemna oznacza odjęcie czasu).
**affected_files**: Lista wszystkich oryginalnych plików, których dotyczyła dana korekta.
**corrected_row_indices**: Precyzyjna lista indeksów wierszy, które zostały zmodyfikowane.

---------------------------------
 Autorstwo i Współpraca
---------------------------------
Marek Urbaniak
Skrypt powstał w wyniku współpracy z Pomocnikiem w programowaniu Gemini.

---------------------------------
 Wersja / Data
---------------------------------
Wersja: 2.0
Data: 11 czerwca 2025

"""

CORR_FILENAME = "tscorrections.jsonl"
CORR_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), CORR_FILENAME)
# print(f"Ścieżka do loga: {CORR_FILE_PATH}")

# --- Klasa okna dialogowego do wklejania ścieżek ---
class PathInputDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Wklej ścieżki do plików")
        self.setMinimumSize(500, 300)
        layout = QVBoxLayout(self)
        info_label = QLabel("Wklej poniżej ścieżki do plików CSV (każda w nowej linii):", self)
        layout.addWidget(info_label)
        self.text_edit = QTextEdit(self)
        layout.addWidget(self.text_edit)
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, self)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    def get_paths(self):
        text = self.text_edit.toPlainText()
        paths = [line.strip() for line in text.splitlines() if line.strip()]
        return paths

# --- Model danych dla tabeli ---
class PandasModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), file_colors=None, master_df_ref=None):
        super().__init__()
        self._df = df
        self.file_colors = file_colors if file_colors is not None else {}
        self.master_df_ref = master_df_ref if master_df_ref is not None else pd.DataFrame()

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        try:
            if not index.isValid(): return None
            row, col = index.row(), index.column()
            if not (0 <= row < len(self._df) and 0 <= col < self._df.shape[1]): return None
            if role == Qt.ItemDataRole.DisplayRole:
                value = self._df.iloc[row, col]
                return str(value)
            if role == Qt.ItemDataRole.BackgroundRole:
                master_index = self._df.index[row]
                if not self.master_df_ref.empty and 'do_korekty' in self.master_df_ref.columns and self.master_df_ref.at[master_index, 'do_korekty']:
                    return QColor(255, 250, 205)
                if 'oryginalny_plik' in self._df.columns:
                    source_file = self._df.iloc[row]['oryginalny_plik']
                    if source_file in self.file_colors:
                        base_color = QColor(self.file_colors[source_file])
                        pale_color = base_color.lighter(185)
                        return pale_color
            return None
        except Exception as e:
            print(f"BŁĄD KRYTYCZNY w PandasModel.data: {e}, wiersz: {index.row()}, kolumna: {index.column()}")
            return None
            
    def rowCount(self, parent=QModelIndex()): return self._df.shape[0]
    def columnCount(self, parent=QModelIndex()): return self._df.shape[1]
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        try:
            if role == Qt.ItemDataRole.DisplayRole:
                if orientation == Qt.Orientation.Horizontal: return str(self._df.columns[section])
                if orientation == Qt.Orientation.Vertical: return str(self._df.index[section])
            return None
        except IndexError: return None
            
    def set_dataframes(self, df, master_df):
        self.beginResetModel()
        self._df = df
        self.master_df_ref = master_df
        self.endResetModel()

# --- Główne okno aplikacji ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Aplikacja do korygowania dat")
        self.setGeometry(100, 100, 1300, 800)
        self.df = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.rect_selector = None
        self.file_colors = {}
        self.scatter_points = None
        self.plot_line = None
        self.ax = None
        self.tooltip = None
        self.original_paths = []
        self.setup_ui()

    def setup_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        self._create_menu()
        top_panel = QHBoxLayout()
        self.column_selector = QComboBox()
        self.column_selector.currentTextChanged.connect(self.on_column_change)
        self.tooltip_checkbox = QCheckBox("Pokaż etykiety")
        self.tooltip_checkbox.stateChanged.connect(self.on_tooltip_toggle)
        self.btn_fit_zoom = QPushButton("Dopasuj Widok")
        self.btn_fit_zoom.clicked.connect(self.redraw_plot)
        self.btn_export = QPushButton("Eksportuj zaznaczenie")
        self.btn_export.setStyleSheet("background-color: #0000FF; color: white; font-weight: bold;")
        self.btn_export.clicked.connect(self.export_selection)
        self.btn_clear_selection = QPushButton("Wyczyść zaznaczenie")
        self.btn_clear_selection.clicked.connect(self.clear_selection)
        self.btn_calc_diff = QPushButton("Oblicz różnicę")
        self.btn_calc_diff.clicked.connect(self.calculate_time_difference)
        self.btn_correct = QPushButton("Koryguj zaznaczone")
        self.btn_correct.clicked.connect(self.correct_data)
        self.btn_delete = QPushButton("Usuń zaznaczone")
        self.btn_delete.clicked.connect(self.delete_selected_rows)
        self.btn_delete.setStyleSheet("background-color: #F08080; color: white; font-weight: bold;")
        self.btn_save = QPushButton("Zapisz dane...")
        self.btn_save.clicked.connect(self.save_data)
        top_panel.addWidget(self.column_selector)
        top_panel.addStretch()
        top_panel.addWidget(self.tooltip_checkbox)
        top_panel.addWidget(self.btn_fit_zoom)
        top_panel.addWidget(self.btn_export)
        top_panel.addWidget(self.btn_clear_selection)
        top_panel.addWidget(self.btn_calc_diff)
        top_panel.addWidget(self.btn_correct)
        top_panel.addWidget(self.btn_delete)
        top_panel.addWidget(self.btn_save)
        plot_container_widget = QWidget()
        plot_layout = QVBoxLayout(plot_container_widget)
        plot_layout.setContentsMargins(0, 0, 0, 0)
        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        self.canvas.mpl_connect('pick_event', self.on_plot_pick)
        self.canvas.mpl_connect('motion_notify_event', self.on_plot_hover)
        self.toolbar = NavigationToolbar(self.canvas, self)
        plot_layout.addWidget(self.toolbar)
        plot_layout.addWidget(self.canvas)
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(plot_container_widget)
        self.table_view = QTableView()
        self.model = PandasModel(file_colors=self.file_colors, master_df_ref=self.df)
        self.table_view.setModel(self.model)
        
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.selectionModel().selectionChanged.connect(self.on_table_selection_changed)

        splitter.addWidget(self.table_view)
        splitter.setSizes([500, 300])
        main_layout.addLayout(top_panel)
        main_layout.addWidget(splitter)
        file_dock = QDockWidget("Załadowane Pliki", self)
        file_list_container = QWidget()
        file_list_layout = QVBoxLayout(file_list_container)
        self.file_list_widget = QListWidget()
        self.file_list_widget.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.file_list_widget.itemChanged.connect(self.on_file_visibility_changed)
        file_list_layout.addWidget(self.file_list_widget)
        button_panel = QHBoxLayout()
        btn_load_dialog_dock = QPushButton("Wczytaj...")
        btn_load_dialog_dock.clicked.connect(self.open_file_dialog)
        btn_load_paste_dock = QPushButton("Wklej ścieżki...")
        btn_load_paste_dock.clicked.connect(self.open_path_paste_dialog)
        btn_apply_order = QPushButton("Zastosuj kolejność")
        btn_apply_order.clicked.connect(self.apply_new_file_order)
        btn_clear_all_dock = QPushButton("Wyczyść wszystko")
        btn_clear_all_dock.clicked.connect(self.clear_all_data)
        button_panel.addWidget(btn_load_dialog_dock)
        button_panel.addWidget(btn_load_paste_dock)
        button_panel.addStretch()
        button_panel.addWidget(btn_apply_order)
        button_panel.addWidget(btn_clear_all_dock)
        file_list_layout.addLayout(button_panel)
        file_dock.setWidget(file_list_container)
        self.addDockWidget(Qt.DockWidgetArea.RightDockWidgetArea, file_dock)
        self.table_view.verticalScrollBar().valueChanged.connect(self.update_visible_files_font)

    def _create_menu(self):
        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("Plik")
        paste_paths_action = QAction("Wczytaj ze ścieżek...", self)
        paste_paths_action.triggered.connect(self.open_path_paste_dialog)
        file_menu.addAction(paste_paths_action)
        load_files_action = QAction("Wczytaj z okna dialogowego...", self)
        load_files_action.triggered.connect(self.open_file_dialog)
        file_menu.addAction(load_files_action)
        file_menu.addSeparator()
        save_action = QAction("Zapisz dane...", self)
        save_action.triggered.connect(self.save_data)
        file_menu.addAction(save_action)
        file_menu.addSeparator()
        exit_action = QAction("Zakończ", self)
        exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
    def _update_filtered_data(self):
        visible_files = []
        for i in range(self.file_list_widget.count()):
            item = self.file_list_widget.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                visible_files.append(item.data(Qt.ItemDataRole.UserRole))
        if not self.df.empty:
            self.filtered_df = self.df[self.df['oryginalny_plik'].isin(visible_files)]
        else:
            self.filtered_df = pd.DataFrame()
        self.model.set_dataframes(self.filtered_df, self.df)

    def on_file_visibility_changed(self, item):
        self._update_filtered_data()
        self.redraw_plot()
        self.update_visible_files_font()

    def on_column_change(self):
        self.redraw_plot()
        
    def refresh_all_views(self, preserve_zoom=False):
        self._update_filtered_data()
        selection_model = self.table_view.selectionModel()
        selection_model.blockSignals(True)
        selection_model.clear()
        selected_master_indices = self.df.index[self.df['do_korekty']]
        rows_to_select_in_view = self.filtered_df.index.get_indexer_for(selected_master_indices)
        rows_to_select_in_view = rows_to_select_in_view[rows_to_select_in_view != -1]
        selection = QItemSelection()
        for row in rows_to_select_in_view:
            top_left = self.model.index(row, 0)
            bottom_right = self.model.index(row, self.model.columnCount() - 1)
            selection.select(top_left, bottom_right)
        if not selection.isEmpty():
            selection_model.select(selection, QItemSelectionModel.SelectionFlag.Rows)
        selection_model.blockSignals(False)
        if preserve_zoom:
            self.update_plot_data()
        else:
            self.redraw_plot()
        self.table_view.viewport().update()
        
    def on_table_selection_changed(self, selected: QItemSelection, deselected: QItemSelection):
        selected_view_rows = {index.row() for index in self.table_view.selectionModel().selectedRows()}
        if not self.filtered_df.empty:
            selected_master_indices = set(self.filtered_df.index[list(selected_view_rows)])
            self.df['do_korekty'] = self.df.index.isin(selected_master_indices)
            self.update_plot_data()
            self.table_view.viewport().update()

    def on_plot_pick(self, event):
        if self.toolbar.mode: return
        if len(event.ind) > 0:
            view_index = event.ind[0]
            if 0 <= view_index < len(self.filtered_df):
                master_index = self.filtered_df.index[view_index]
                self.df.at[master_index, 'do_korekty'] = not self.df.at[master_index, 'do_korekty']
                self.refresh_all_views(preserve_zoom=True)
                self.scroll_table_to_row(view_index)

    def on_rect_select(self, eclick, erelease):
        if self.toolbar.mode: return
        x1, y1, x2, y2 = eclick.xdata, eclick.ydata, erelease.xdata, erelease.ydata
        if None in [x1, y1, x2, y2]: return
        self.rect_selector.set_visible(False)
        time_column, plot_column = self.df.columns[0], self.column_selector.currentText()
        if not plot_column: return
        time_min = num2date(min(x1, x2)).replace(tzinfo=None)
        time_max = num2date(max(x1, x2)).replace(tzinfo=None)
        y_min, y_max = min(y1, y2), max(y1, y2)
        df_to_check = self.filtered_df
        time_data = pd.to_datetime(df_to_check[time_column]).dt.tz_localize(None)
        mask_in_filtered = (time_data >= time_min) & (time_data <= time_max) & (df_to_check[plot_column] >= y_min) & (df_to_check[plot_column] <= y_max)
        master_indices_to_select = df_to_check.index[mask_in_filtered]
        self.df.loc[master_indices_to_select, 'do_korekty'] = True
        self.refresh_all_views(preserve_zoom=True)
        if not master_indices_to_select.empty:
            try:
                view_index_to_scroll = df_to_check.index.get_loc(master_indices_to_select[0])
                self.scroll_table_to_row(view_index_to_scroll)
            except KeyError: pass
            
    def on_tooltip_toggle(self):
        if not self.tooltip_checkbox.isChecked() and self.tooltip:
            self.tooltip.set_visible(False)
            self.canvas.draw_idle()

    def on_plot_hover(self, event):
        if not self.tooltip_checkbox.isChecked() or not hasattr(self, 'ax') or self.ax is None or self.filtered_df.empty or self.scatter_points is None: return
        is_visible = self.tooltip.get_visible()
        if event.inaxes == self.ax:
            contains, ind = self.scatter_points.contains(event)
            if contains:
                view_index = ind["ind"][0]
                if 0 <= view_index < len(self.filtered_df):
                    master_index = self.filtered_df.index[view_index]
                    time_column, plot_column = self.df.columns[0], self.column_selector.currentText()
                    if not plot_column: return
                    timestamp, value = self.df.at[master_index, time_column], self.df.at[master_index, plot_column]
                    tooltip_text = f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')}\nWartość: {value:.2f}"
                    self.tooltip.set_text(tooltip_text)
                    self.tooltip.xy = (mdates.date2num(timestamp), value)
                    self.tooltip.set_visible(True)
                    self.canvas.draw_idle()
                    return
        if is_visible:
            self.tooltip.set_visible(False)
            self.canvas.draw_idle()

    def clear_all_data(self):
        reply = QMessageBox.question(self, "Potwierdzenie", "Czy na pewno chcesz usunąć wszystkie załadowane dane?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.df = pd.DataFrame()
            self.file_colors.clear()
            self._update_filtered_data()
            self.populate_file_list()
            self.redraw_plot()
            
    def clear_selection(self):
        if self.df.empty: return
        self.df['do_korekty'] = False
        self.refresh_all_views(preserve_zoom=True)

    def delete_selected_rows(self):
        if self.df.empty or not self.df['do_korekty'].any():
            QMessageBox.information(self, "Informacja", "Nie zaznaczono żadnych wierszy do usunięcia.")
            return
        rows_to_delete_mask = self.df['do_korekty']
        count = rows_to_delete_mask.sum()
        reply = QMessageBox.question(self, "Potwierdzenie usunięcia", f"Czy na pewno chcesz trwale usunąć {count} wiersz(y)?\n\nTej operacji nie można cofnąć.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            indices_to_drop = self.df.index[rows_to_delete_mask]
            self.df.drop(indices_to_drop, inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            self.refresh_all_views()
            QMessageBox.information(self, "Sukces", f"Usunięto {count} wiersz(y).")

    def apply_new_file_order(self):
        if self.df.empty: return
        new_order = [self.file_list_widget.item(i).data(Qt.ItemDataRole.UserRole) for i in range(self.file_list_widget.count())]
        from pandas.api.types import CategoricalDtype
        cat_type = CategoricalDtype(categories=new_order, ordered=True)
        self.df['oryginalny_plik'] = self.df['oryginalny_plik'].astype(cat_type)
        self.df.sort_values('oryginalny_plik', inplace=True)
        self.df.reset_index(drop=True, inplace=True)
        self.df['oryginalny_plik'] = self.df['oryginalny_plik'].astype(object)
        self.refresh_all_views()
        QMessageBox.information(self, "Sukces", "Zastosowano nową kolejność serii danych.")

    def correct_data(self):
        rows_to_correct_mask = self.df['do_korekty']
        if not rows_to_correct_mask.any():
            QMessageBox.information(self, "Informacja", "Nie zaznaczono żadnych wierszy do korekty.")
            return
        num_rows_to_correct = rows_to_correct_mask.sum()
        text, ok = QInputDialog.getText(self, 'Korekta Czasu', "Wprowadź przesunięcie czasu...")
        if ok and text:
            timedelta = self._parse_timedelta_string(text)
            if timedelta is None: QMessageBox.warning(self, "Błąd formatu", f"Nieprawidłowy format: '{text}'."); return
            try:
                log_timestamp = datetime.now().isoformat()
                corrected_indices = self.df.index[rows_to_correct_mask].tolist()
                affected_files = self.df.loc[rows_to_correct_mask, 'oryginalny_plik'].unique().tolist()
                log_entry = {"log_timestamp": log_timestamp,"correction_details": {"timedelta_applied_str": str(timedelta),"timedelta_total_seconds": timedelta.total_seconds(),"affected_files": affected_files,"corrected_row_indices": corrected_indices}}
                with open(CORR_FILE_PATH, "a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f"Błąd podczas zapisywania logu JSON: {e}")
            time_column = self.df.columns[0]
            self.df.loc[rows_to_correct_mask, time_column] += timedelta
            self.df.loc[rows_to_correct_mask, 'do_korekty'] = False
            self.refresh_all_views(preserve_zoom=True)
            QMessageBox.information(self, "Sukces", f"Zastosowano korektę ({text}) dla {num_rows_to_correct} wierszy.")

    def redraw_plot(self):
        self.ax = None
        self.scatter_points = None
        self.plot_line = None
        self.figure.clear()
        self.ax = self.figure.add_subplot(111)
        self.ax.callbacks.connect('xlim_changed', self.update_visible_files_font)
        df_to_plot = self.filtered_df
        if df_to_plot.empty or not self.column_selector.currentText():
            self.canvas.draw()
            return
        time_column, plot_column = df_to_plot.columns[0], self.column_selector.currentText()
        colors = ['magenta' if self.df.at[idx, 'do_korekty'] else self.file_colors.get(row['oryginalny_plik'], '#808080') for idx, row in df_to_plot.iterrows()]
        self.plot_line, = self.ax.plot(df_to_plot[time_column], df_to_plot[plot_column], '-', color='lightgray', zorder=1, alpha=0.5)
        self.scatter_points = self.ax.scatter(df_to_plot[time_column], df_to_plot[plot_column], c=colors, picker=True, pickradius=5, zorder=2, alpha=0.7)
        self.ax.set_title(f"Wykres dla: {plot_column}")
        self.ax.set_xlabel("Czas")
        self.ax.set_ylabel("Wartość")
        self.ax.grid(True)
        if not df_to_plot[time_column].dropna().empty:
            min_date, max_date = df_to_plot[time_column].min(), df_to_plot[time_column].max()
            date_range = max_date - min_date
            padding = date_range * 0.05 if date_range.total_seconds() > 0 else pd.Timedelta(days=1)
            self.ax.set_xlim(min_date - padding, max_date + padding)
        self.tooltip = self.ax.annotate("", xy=(0,0), xytext=(20,20), textcoords="offset points", bbox=dict(boxstyle="round,pad=0.5", fc="yellow", alpha=0.8), arrowprops=dict(arrowstyle="->"))
        self.tooltip.set_visible(False)
        self.tooltip.set_zorder(10)
        self.rect_selector = RectangleSelector(self.ax, self.on_rect_select, useblit=False, button=[1], minspanx=5, minspany=5, spancoords='pixels', interactive=True, props=dict(facecolor='red', alpha=0.2))
        self.figure.tight_layout()
        self.canvas.draw()
    
    def update_plot_data(self):
        df_to_plot = self.filtered_df
        if df_to_plot.empty or self.scatter_points is None or self.plot_line is None: return
        colors = ['magenta' if self.df.at[idx, 'do_korekty'] else self.file_colors.get(row['oryginalny_plik'], '#808080') for idx, row in df_to_plot.iterrows()]
        self.scatter_points.set_color(colors)
        time_column, plot_column = df_to_plot.columns[0], self.column_selector.currentText()
        if not plot_column: return
        self.plot_line.set_data(df_to_plot[time_column], df_to_plot[plot_column])
        x_coords_numeric = mdates.date2num(df_to_plot[time_column])
        y_coords = df_to_plot[plot_column]
        new_offsets = np.c_[x_coords_numeric, y_coords]
        self.scatter_points.set_offsets(new_offsets)
        self.canvas.draw_idle()

    def load_data_from_paths(self, file_paths: list):
        if not file_paths: return
        newly_added_paths = [p for p in file_paths if p not in self.file_colors]
        start_color_index = len(self.file_colors)
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        custom_nan_values = ["OverRange", "UnderRange", "NAN", "INF", "-INF", ""]
        NON_NUMERIC = ['Timestamp', 'TIMESTAMP', 'oryginalny_plik','group_id', 'source_file', 'interval', 'TZ', '5M METAR Tab.4678', '1M METAR Tab.4678', '5MMETARTab4678', '1MMETARTab4678', 'source_filename', 'source_filepath']  # Dodaj inne, jeśli trzeba

        for i, path in enumerate(newly_added_paths):
            self.file_colors[path] = colors[(start_color_index + i) % len(colors)]
        try:
            new_df_list = []
            for file in file_paths:
                if not os.path.exists(file): raise FileNotFoundError(f"Plik nie został znaleziony: {file}")
                df = pd.read_csv(file, low_memory=False, na_values=custom_nan_values)
                numeric_cols = [col for col in df.columns if col not in NON_NUMERIC]
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
                df['oryginalny_plik'] = file
                new_df_list.append(df)
            newly_loaded_df = pd.concat(new_df_list, ignore_index=True)
            self.df = pd.concat([self.df, newly_loaded_df]).reset_index(drop=True)
            time_column = self.df.columns[0]
            self.df[time_column] = pd.to_datetime(self.df[time_column], errors='coerce')
            self.df.dropna(subset=[time_column], inplace=True)
            self.df.reset_index(drop=True, inplace=True)
            if 'do_korekty' not in self.df.columns:
                self.df['do_korekty'] = False
            else:
                self.df.loc[self.df['do_korekty'].isna(), 'do_korekty'] = False
            self.df['do_korekty'] = self.df['do_korekty'].astype(bool)
            self.update_column_selector()
            self.populate_file_list()
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Błąd wczytywania", f"Nie udało się wczytać plików.\n\nBłąd: {e}")

    def populate_file_list(self):
        self.file_list_widget.blockSignals(True)
        self.file_list_widget.clear()
        for path in self.file_colors.keys():
            item = QListWidgetItem(os.path.basename(path))
            item.setForeground(QColor(self.file_colors[path]))
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked)
            item.setData(Qt.ItemDataRole.UserRole, path)
            self.file_list_widget.addItem(item)
        self.file_list_widget.blockSignals(False)
        self.on_file_visibility_changed(None)

    def update_visible_files_font(self, _=None):
        if self.filtered_df.empty or not self.table_view.isVisible(): return
        rect = self.table_view.viewport().rect()
        top_left = self.table_view.indexAt(rect.topLeft())
        bottom_right = self.table_view.indexAt(rect.bottomRight())
        start_row = top_left.row() if top_left.isValid() else 0
        end_row = bottom_right.row() if bottom_right.isValid() else -1
        if start_row == -1 or end_row == -1 : return
        visible_df_slice = self.filtered_df.iloc[start_row : end_row + 1]
        if visible_df_slice.empty: return
        visible_files = set(visible_df_slice['oryginalny_plik'].unique())
        for i in range(self.file_list_widget.count()):
            item = self.file_list_widget.item(i)
            item_path = item.data(Qt.ItemDataRole.UserRole)
            font = item.font()
            font.setBold(item_path in visible_files)
            item.setFont(font)
            
    def on_plot_xlim_changed(self, ax):
        self.update_visible_files_font()
        if self.filtered_df.empty or self.toolbar.mode: return
        xmin, _ = ax.get_xlim()
        visible_start_time = num2date(xmin).replace(tzinfo=None)
        time_column = self.df.columns[0]
        time_data = pd.to_datetime(self.filtered_df[time_column]).dt.tz_localize(None)
        try:
            view_index = time_data.searchsorted(visible_start_time)
            if view_index < len(self.filtered_df):
                self.scroll_table_to_row(int(view_index))
        except Exception: pass
        
    def scroll_table_to_row(self, view_row_number):
        if not (0 <= view_row_number < self.model.rowCount()): return
        index = self.model.index(int(view_row_number), 0)
        self.table_view.scrollTo(index, QAbstractItemView.ScrollHint.PositionAtCenter)

    def update_column_selector(self):
        cols_to_exclude = {'do_korekty', 'oryginalny_plik'}
        all_cols = self.df.columns.tolist()
        filtered_cols = [col for col in all_cols if pd.api.types.is_numeric_dtype(self.df[col]) and col not in cols_to_exclude]
        self.column_selector.clear()
        self.column_selector.addItems(filtered_cols)

    def open_path_paste_dialog(self):
        dialog = PathInputDialog(self)
        if dialog.exec():
            paths = dialog.get_paths()
            if paths: self.load_data_from_paths(paths)
            else: QMessageBox.warning(self, "Brak danych", "Nie wklejono żadnych prawidłowych ścieżek.")

    def open_file_dialog(self):
        file_names, _ = QFileDialog.getOpenFileNames(self, "Wybierz pliki CSV", "", "Pliki CSV (*.csv)")
        if file_names: self.load_data_from_paths(file_names)

    def _parse_timedelta_string(self, text: str):
        text = text.strip()
        if not text: return None
        pattern = r"(-?)\s*(\d+)\s+(\d{1,2}):(\d{1,2})|(-?)\s*(\d{1,2}):(\d{1,2})"
        match = re.fullmatch(pattern, text)
        if not match: return None
        try:
            if match.group(1) is not None:
                sign, days, hours, minutes = -1 if match.group(1) == '-' else 1, int(match.group(2)), int(match.group(3)), int(match.group(4))
            else:
                sign, days, hours, minutes = -1 if match.group(5) == '-' else 1, 0, int(match.group(6)), int(match.group(7))
            return pd.Timedelta(days=days, hours=hours, minutes=minutes) * sign
        except (ValueError, TypeError): return None

    def calculate_time_difference(self):
        selected_indices = self.df.index[self.df['do_korekty']].tolist()
        if len(selected_indices) != 2:
            QMessageBox.information(self, "Informacja", "Proszę zaznaczyć dokładnie dwa wiersze.")
            return
        time_column = self.df.columns[0]
        t1 = self.df.loc[selected_indices[0], time_column]
        t2 = self.df.loc[selected_indices[1], time_column]
        delta = abs(t2 - t1)
        QMessageBox.information(self, "Różnica czasu", f"Obliczona różnica czasu wynosi:\n\n{delta}")
    
    def export_selection(self):
        """Eksportuje znaczniki czasu z zaznaczonych wierszy do schowka lub pliku."""
        
        selected_df = self.df[self.df['do_korekty']]
        if selected_df.empty:
            QMessageBox.information(self, "Informacja", "Nie zaznaczono żadnych wierszy do eksportu.")
            return

        time_column = self.df.columns[0]
        timestamps = selected_df[time_column]
        
        # ZMIANA: Modyfikujemy sposób formatowania i łączenia tekstu
        # 1. Tworzymy listę dat w pożądanym formacie
        formatted_stamps = [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]
        # 2. Każdą datę ujmujemy w pojedynczy cudzysłów
        quoted_stamps = [f"'{s}'" for s in formatted_stamps]
        # 3. Łączymy wszystko w jedną linię, oddzielając elementy przecinkiem i spacją
        output_string = ', '.join(quoted_stamps)

        # Reszta funkcji (pytanie o schowek/plik) pozostaje bez zmian
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Eksportuj zaznaczenie")
        msg_box.setText(f"Wyeksportowano {len(quoted_stamps)} znaczników czasu.\n\nWybierz miejsce docelowe:")
        msg_box.setIcon(QMessageBox.Icon.Question)
        
        btn_clipboard = msg_box.addButton("Kopiuj do schowka", QMessageBox.ButtonRole.ActionRole)
        btn_save_file = msg_box.addButton("Zapisz do pliku...", QMessageBox.ButtonRole.ActionRole)
        msg_box.addButton("Anuluj", QMessageBox.ButtonRole.RejectRole)
        
        msg_box.exec()
        
        clicked_button = msg_box.clickedButton()

        if clicked_button == btn_clipboard:
            clipboard = QApplication.clipboard()
            clipboard.setText(output_string)
            QMessageBox.information(self, "Sukces", "Znaczniki czasu zostały skopiowane do schowka.")

        elif clicked_button == btn_save_file:
            file_path, _ = QFileDialog.getSaveFileName(self, "Zapisz znaczniki czasu jako...", "", "Pliki tekstowe (*.txt);;Wszystkie pliki (*)")
            if file_path:
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(output_string)
                    QMessageBox.information(self, "Sukces", f"Znaczniki czasu zostały zapisane w pliku:\n{file_path}")
                except Exception as e:
                    QMessageBox.critical(self, "Błąd zapisu", f"Nie udało się zapisać pliku.\nBłąd: {e}")
                    
    def save_data(self):
        if self.df.empty:
            QMessageBox.warning(self, "Brak danych", "Nie ma żadnych danych do zapisania.")
            return
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Zapisywanie danych")
        msg_box.setText("Jak chcesz zapisać skorygowane dane?")
        msg_box.setIcon(QMessageBox.Icon.Question)
        btn_save_as = msg_box.addButton("Zapisz jako...", QMessageBox.ButtonRole.ActionRole)
        btn_overwrite = msg_box.addButton("Nadpisz oryginalne", QMessageBox.ButtonRole.ActionRole)
        btn_cancel = msg_box.addButton("Anuluj", QMessageBox.ButtonRole.RejectRole)
        msg_box.exec()
        clicked_button = msg_box.clickedButton()
        
        cols_to_drop = ['do_korekty', 'oryginalny_plik', 'master_id']
        
        def custom_save(df_to_save, file_path):
            # Tworzymy kopię, aby nie modyfikować oryginalnego DataFrame w pamięci
            df_copy = df_to_save.drop(columns=cols_to_drop, errors='ignore').copy()
            
            # Znajdujemy pierwszą kolumnę (zakładamy, że to data)
            time_column = df_copy.columns[0]

            # NAPRAWA: Ręcznie formatujemy kolumnę daty do pożądanego formatu tekstowego
            # %f daje mikrosekundy (6 cyfr), więc ucinamy do 3, aby uzyskać milisekundy
            df_copy[time_column] = pd.to_datetime(df_copy[time_column]).dt.strftime('%Y/%m/%d %H:%M:%S.%f').str[:-3]

            # Tworzymy ręcznie sformatowany nagłówek w cudzysłowach
            quoted_header = ','.join([f'"{col}"' for col in df_copy.columns])
            
            # Zapisujemy dane do CSV bez nagłówka i bez specjalnego cytowania, bo data jest już tekstem
            csv_data = df_copy.to_csv(index=False, header=False, quoting=csv.QUOTE_MINIMAL)
            
            # Łączymy nagłówek z danymi i zapisujemy do pliku
            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                f.write(quoted_header + '\n')
                f.write(csv_data)

        if clicked_button == btn_save_as:
            file_path, _ = QFileDialog.getSaveFileName(self, "Zapisz jako...", "", "Pliki CSV (*.csv)")
            if file_path:
                try:
                    custom_save(self.df, file_path)
                    QMessageBox.information(self, "Sukces", f"Dane zapisane w:\n{file_path}")
                except Exception as e:
                    QMessageBox.critical(self, "Błąd zapisu", f"Błąd: {e}")

        elif clicked_button == btn_overwrite:
            original_files = self.df['oryginalny_plik'].unique()
            reply = QMessageBox.warning(self, "Potwierdzenie nadpisania", f"Czy na pewno chcesz nadpisać {len(original_files)} plik(i)?\n\nTA OPERACJA JEST NIEODWRACALNA!", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    for file_path in original_files:
                        group_df = self.df[self.df['oryginalny_plik'] == file_path]
                        custom_save(group_df, file_path)
                    QMessageBox.information(self, "Sukces", "Oryginalne pliki nadpisane.")
                except Exception as e:
                    QMessageBox.critical(self, "Błąd zapisu", f"Błąd: {e}")

    def keyPressEvent(self, event):
        key = event.key()
        if key == Qt.Key.Key_Z: self.toolbar.zoom()
        elif key == Qt.Key.Key_P: self.toolbar.pan()
        elif key == Qt.Key.Key_D: self.toolbar.home()
        elif key == Qt.Key.Key_S: self.toolbar.save_figure()
        elif key == Qt.Key.Key_Left: self.toolbar.back()
        elif key == Qt.Key.Key_Right: self.toolbar.forward()
        elif key == Qt.Key.Key_Escape:
            if self.toolbar.mode == 'zoom rect': self.toolbar.zoom()
            elif self.toolbar.mode == 'pan/zoom': self.toolbar.pan()
        else:
            super().keyPressEvent(event)

# # --- Główny blok uruchomieniowy ---
# if __name__ == '__main__':
#     app = QApplication(sys.argv)
#     main_win = MainWindow()
#     main_win.show()
#     sys.exit(app.exec())
    
# --- Główny blok uruchomieniowy ---
if __name__ == '__main__':
    # ROZWIĄZANIE PROBLEMU z pluginem 'cocoa' na macOS
    import os
    from pathlib import Path
    import sys
    
    # Znajdujemy ścieżkę do folderu 'site-packages', gdzie zainstalowane są biblioteki
    # i ustawiamy zmienną środowiskową QT_PLUGIN_PATH
    try:
        from PyQt6 import QtCore
        # Pobieramy ścieżkę do biblioteki QtCore
        lib_path = Path(QtCore.__file__).parent
        # Ścieżka do pluginów jest zazwyczaj w 'Qt6/plugins'
        plugin_path = lib_path / "Qt6" / "plugins"
        os.environ['QT_PLUGIN_PATH'] = str(plugin_path)
        print(f"INFO: Ustawiono ścieżkę pluginów Qt na: {plugin_path}")
    except Exception as e:
        print(f"OSTRZEŻENIE: Nie udało się automatycznie ustawić ścieżki do pluginów Qt. Błąd: {e}")

    # Uruchamiamy aplikację
    app = QApplication(sys.argv)
    main_win = MainWindow()
    main_win.show()
    sys.exit(app.exec())