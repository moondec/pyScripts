# -*- coding: utf-8 -*-

"""
================================================================================
  Uniwersalny Skrypt do Agregacji Danych (Wersja 8.0 - Zunifikowana)
================================================================================
Opis:
    Wersja 8.0 stanowi kompletną unifikację i refaktoryzację poprzednich
    skryptów (split.py i splitSQ.py). Łączy w sobie najlepsze cechy obu
    wersji, tworząc jedno, spójne narzędzie do przetwarzania danych pomiarowych.
    Skrypt oferuje elastyczny wybór formatu wyjściowego (SQLite, CSV lub oba)
    i implementuje zaawansowaną logikę hierarchii źródeł danych.

Architektura:
    - Centralizacja konfiguracji: Wszystkie reguły (mapowanie, kalibracja,
      korekty czasu, flagi jakości) są zarządzane z poziomu globalnych
      słowników.
    - Hierarchia źródeł danych: Skrypt w pierwszej kolejności przetwarza
      surowe dane z loggerów (TOA5, TOB1, CSV). Następnie, ewentualne luki
      w danych (wartości NaN) są uzupełniane danymi z plików .MAT.
    - Dwa potoki przetwarzania:
        1. Pliki binarne (TOA5/TOB1) są przetwarzane indywidualnie, z użyciem
           filtra `filter_by_realistic_date_range`.
        2. Pliki CSV są przetwarzane w partiach, z użyciem funkcji korekty
           chronologii `correct_and_report_chronology`.
    - Elastyczny zapis wyników: Możliwość zapisu do bazy SQLite, plików CSV
      lub obu formatów jednocześnie, kontrolowana argumentem `--output-format`.
    - Zaawansowane logowanie i cache: Mechanizm logowania z kontrolą poziomu
      oraz cache oparty na sumie kontrolnej i dacie modyfikacji pliku w celu
      unikania ponownego przetwarzania.

Główne funkcjonalności:
    - Obsługa wielu formatów wejściowych: TOB1, TOA5, CSV i .MAT.
    - Dekodowanie formatów binarnych (w tym FP2).
    - Zaawansowane zarządzanie strefami czasowymi i korekty manualne.
    - Kalibracja danych na podstawie reguł dla poszczególnych sensorów.
    - Automatyczne flagowanie jakości danych (reguły czasowe i zakresowe).
    - Równoległe przetwarzanie plików w celu maksymalizacji wydajności.
    - Wbudowany tryb testowy (`--run-tests`) do weryfikacji spójności wyników.

Wymagania:
    - Python 3.10+
    - Biblioteki: pandas, pytest, scipy, tqdm
      pip install pandas pytest scipy tqdm

Uruchamianie:
    Skrypt należy uruchamiać z wiersza poleceń.

    Składnia podstawowa:
    python unified_script.py -i <katalog_wejsciowy> -o <katalog_wyjsciowy> -fid <id_grupy> --db-path <sciezka_do_bazy> [opcje]

    Nowe i zaktualizowane argumenty:
      -i, --input_dir       (Wymagany) Ścieżka do katalogu z danymi wejściowymi.
      -o, --output_dir      (Wymagany) Ścieżka do katalogu wyjściowego (dla CSV, logów, cache).
      -fid, --file_id       (Wymagany) Identyfikator grupy do przetworzenia.
      --db-path             (Opcjonalny) Ścieżka do pliku bazy SQLite. Wymagany dla formatu 'sqlite' lub 'both'.
      -j, --jobs            (Opcjonalny) Liczba równoległych procesów. Domyślnie: liczba rdzeni CPU.
      --log-level           (Opcjonalny) Poziom logowania: DEBUG, INFO, WARNING, ERROR. Domyślnie: INFO.
      --output-format       (Opcjonalny) Format wyjściowy: 'sqlite', 'csv', 'both'. Domyślnie: 'sqlite'.
      --no-cache            (Opcjonalny) Wyłącza użycie cache. Ale nie nadpisze wyników. Zapisane wyniki mają priorytet.
      --run-tests           (Opcjonalny) Uruchamia tryb testowy do weryfikacji spójności wyników. (do poprawy)
      --overwrite           (Opcjonalny) Uruchamia tryb nadpisywania. Powinno być z '--no-cache' jeśli cache istnieje inaczej nie nadpisze.
    
    Przykłady użycia:
      # Przetwarzanie i zapis do bazy SQLite (domyślnie)
      python unified_script.py -i /dane/tuczno -o /wyniki -fid TU_MET_30min --db-path /wyniki/tuczno.db

      # Przetwarzanie i zapis tylko do plików CSV
      python unified_script.py -i /dane/tuczno -o /wyniki/tuczno_csv -fid TU_MET_30min --output-format csv

      # Przetwarzanie i zapis do obu formatów
      python unified_script.py -i /dane/tuczno -o /wyniki/tuczno_csv -fid TU_MET_30min --db-path /wyniki/tuczno.db --output-format both

--------------------------------------------------------------------------------
    Autor: Marek Urbaniak
    Wersja: 8.0 - Zunifikowana
    Data ostatniej modyfikacji: 25.08.2025
--------------------------------------------------------------------------------
"""

import argparse
import json
import logging
import math
import multiprocessing
import os
import re
import sqlite3
import sqlalchemy
import struct
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, UTC
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
import pytest
from scipy.io import loadmat
from tqdm import tqdm

# import słowników config
from config import *

# --- Globalne definicje ---
CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00')
STRUCT_FORMAT_MAP = {'ULONG':'L', 'IEEE4':'f', 'IEEE8':'d', 'LONG':'l', 'BOOL':'?', 'SHORT':'h', 'USHORT':'H', 'BYTE':'b'}
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / 'logs'
CACHE_FILE_PATH = LOGS_DIR / ".cache_split.json"
LOG_FILE_PATH = LOGS_DIR / "log_split.txt"
CHRONOLOGY_LOG_FILENAME = LOGS_DIR / "log_chronology_correction.txt"
chronology_logger = None

# --- MODUŁY POMOCNICZE I LOGOWANIA ---
def setup_logging(level: str = 'INFO'):
    """Konfiguruje system logowania."""
    # check if handlers are already attached
    # this prevents duplicate logs in interactive environments
    if logging.getLogger().handlers:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # Pobieramy ścieżkę do samego folderu z pełnej ścieżki do pliku
    log_directory = os.path.dirname(LOG_FILE_PATH)
    
    # Tworzymy folder, jeśli nie istnieje. `exist_ok=True` zapobiega błędowi,
    # jeśli folder już został utworzony.
    os.makedirs(log_directory, exist_ok=True)
    
    file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def load_cache() -> Dict[str, Any]:
    """Wczytuje cache przetworzonych plików."""
    if CACHE_FILE_PATH.exists():
        try:
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}

def save_cache(data: Dict[str, Any]):
    """Zapisuje cache przetworzonych plików."""
    try:
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        logging.error(f"Nie można zapisać pliku cache: {e}")

def is_file_in_cache(file_path: Path, cache: dict[str, any]) -> bool:
    """Sprawdza, czy plik jest w cache i czy jego metadane się zgadzają."""
    file_key = str(file_path)
    if file_key not in cache:
        return False
    try:
        file_stat = file_path.stat()
        cached_info = cache[file_key]
        if cached_info.get('mtime') == file_stat.st_mtime and cached_info.get('size') == file_stat.st_size:
            return True
    except FileNotFoundError:
        return False
    return False

def update_cache(processed_files: list[Path], cache: dict[str, any]):
    """Aktualizuje słownik cache o informacje o przetworzonych plikach."""
    for file_path in processed_files:
        try:
            file_stat = file_path.stat()
            cache[str(file_path)] = {
                'mtime': file_stat.st_mtime,
                'size': file_stat.st_size,
                'processed_at': datetime.now().isoformat()
            }
        except FileNotFoundError:
            continue

def setup_chronology_logger():
    """Konfiguruje dedykowany logger do zapisywania informacji o korektach chronologii."""
    global chronology_logger
    chronology_logger = logging.getLogger('chronology')
    chronology_logger.propagate = False
    chronology_logger.setLevel(logging.INFO)
    
    if not chronology_logger.handlers:
        handler = logging.FileHandler(CHRONOLOGY_LOG_FILENAME, mode='a', encoding='utf-8')
        # Używamy prostego formatowania, aby plik był w formacie CSV
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        chronology_logger.addHandler(handler)
        
        # Jeśli plik jest nowy, dodaj nagłówek (nowy, rozszerzony format)
        if handler.stream.tell() == 0:
            chronology_logger.info("LogDate;SourceFilePth;BlockStartIndex;BlockEndIndex;OriginalStartTS;OriginalEndTS;CorrectedStartTS;CorrectedEndTS")
            
def parse_header_line(line):
    # function to parse a header line from a TOA5/TOB1 file
    return [field.strip() for field in line.strip('"').split('","')]

# --- FUNKCJE OBSŁUGI DANYCH WYJŚCIOWYCH ---

def initialize_database(db_path: Path):
    """Tworzy strukturę relacyjnej bazy danych z tabelami metadanych."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Tabela stacji (unikalne lokalizacje)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL
            )
        """)

        # Tabela grup pomiarowych
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                group_id TEXT PRIMARY KEY,
                station_id TEXT NOT NULL,
                interval TEXT,
                FOREIGN KEY (station_id) REFERENCES stations (station_id)
            )
        """)
        
        conn.commit()
        conn.close()
        logging.info(f"Relacyjna baza danych zainicjalizowana w: {db_path}")
    except Exception as e:
        logging.error(f"Nie można zainicjalizować bazy danych: {e}")
        raise

def add_missing_columns(df: pd.DataFrame, conn, table_name: str):
    """
    Dynamicznie dodaje brakujące kolumny do określonej tabeli, używając SQLAlchemy.
    Wersja 7.24: Usunięto zagnieżdżoną transakcję, aby uniknąć błędu InvalidRequestError.
    """
    try:
        inspector = sqlalchemy.inspect(conn)
        existing_cols_info = inspector.get_columns(table_name)
        existing_cols = {col['name'] for col in existing_cols_info}
        
        missing_cols = set(df.columns) - existing_cols

        if not missing_cols:
            return

        # Usunięto blok 'with conn.begin()'. Operacje wykonają się w transakcji nadrzędnej.
        for col in missing_cols:
            if col.endswith('_flag'):
                sql_type = "INTEGER"
            elif pd.api.types.is_integer_dtype(df[col]):
                sql_type = "INTEGER"
            elif pd.api.types.is_numeric_dtype(df[col]):
                sql_type = "REAL"
            else:
                sql_type = "TEXT"
            
            alter_sql = sqlalchemy.text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {sql_type}')
            conn.execute(alter_sql)
            logging.debug(f"Dodano nową kolumnę '{col}' do tabeli '{table_name}'")

    except Exception as e:
        logging.error(f"Nie udało się dodać kolumn do tabeli '{table_name}': {e}")
        raise

def save_dataframe_to_sqlite(df: pd.DataFrame, config: dict, lock: multiprocessing.Lock):
    """
    Zapisuje dane do bazy SQLite, zapewniając poprawny format TIMESTAMP dla zewnętrznych narzędzi.
    Wersja 7.43 FINAL: Poprawiona kolejność operacji dla prawidłowego formatowania daty.
    """
    if df.empty:
        return

    group_id = config['file_id']
    db_path = Path(config['db_path'])
    overwrite_mode = config.get('overwrite', False)
    coords = STATION_COORDINATES.get(group_id, {'lat': None, 'lon': None})
    station_id = group_id.split('_')[0]
    table_name = f"data_{group_id}"
    temp_table_name = f"temp_{group_id}"

    with lock:
        try:
            engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
            with engine.connect() as conn:
                
                # Krok 1: Wstępne oczyszczenie i przygotowanie nowych danych
                df_clean = _enforce_numeric_types(df.copy())

                # Krok 2: Utworzenie tabeli docelowej, jeśli nie istnieje
                if not conn.dialect.has_table(conn, table_name):
                    # Zdefiniuj schemat na podstawie CZYSTYCH danych wejściowych
                    cols_with_types = [f'"{col}" {("INTEGER" if col.endswith("_flag") or pd.api.types.is_integer_dtype(dtype) else "REAL" if pd.api.types.is_numeric_dtype(dtype) else "TEXT")}' 
                                       for col, dtype in df_clean.dtypes.items() if col != 'TIMESTAMP']
                    create_sql = f'CREATE TABLE "{table_name}" (TIMESTAMP TEXT PRIMARY KEY, {", ".join(cols_with_types)})'
                    conn.execute(sqlalchemy.text(create_sql))
                    conn.commit()
                    logging.info(f"Utworzono nową tabelę danych: {table_name}")
                
                # Krok 3: Odczytaj istniejące dane z bazy
                existing_df = pd.DataFrame()
                timestamps_to_check = df_clean['TIMESTAMP'].dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
                if timestamps_to_check:
                    chunk_size = 900
                    all_existing_dfs = []
                    for i in range(0, len(timestamps_to_check), chunk_size):
                        chunk_ts = timestamps_to_check[i:i + chunk_size]
                        placeholders = ", ".join([f"'{ts}'" for ts in chunk_ts])
                        query = f'SELECT * FROM "{table_name}" WHERE TIMESTAMP IN ({placeholders})'
                        try:
                           chunk = pd.read_sql_query(query, conn)
                           chunk['TIMESTAMP'] = pd.to_datetime(chunk['TIMESTAMP'])
                           all_existing_dfs.append(chunk)
                        except Exception: # Błąd jeśli tabela jest pusta lub odpytanie nie zadziała
                            pass 
                    if all_existing_dfs:
                        existing_df = pd.concat(all_existing_dfs, ignore_index=True)

                # Krok 4: Połącz dane zgodnie z trybem i ponownie oczyść typy
                if not existing_df.empty:
                    df_indexed = df_clean.set_index('TIMESTAMP')
                    existing_df_indexed = existing_df.set_index('TIMESTAMP')
                    if overwrite_mode:
                        merged_df = df_indexed.combine_first(existing_df_indexed)
                    else:
                        merged_df = existing_df_indexed.combine_first(df_indexed)
                    df_to_save = merged_df.reset_index()
                else:
                    df_to_save = df_clean.copy()
                
                df_to_save = _enforce_numeric_types(df_to_save)
                add_missing_columns(df_to_save, conn, table_name)

                # Krok 5: Zapisz dane do tabeli tymczasowej
                dtype_map = {col: sqlalchemy.types.Integer for col in df_to_save.columns if col.endswith('_flag')}
                df_to_save.to_sql(temp_table_name, conn, if_exists='replace', index=False, dtype=dtype_map, chunksize=2000)

                # --- POCZĄTEK KLUCZOWEJ ZMIANY ---
                # Krok 6: Sformatuj TIMESTAMP na TEXT Zgodny z ISO 8601 w tabeli tymczasowej
                format_sql = f'UPDATE "{temp_table_name}" SET TIMESTAMP = strftime("%Y-%m-%dT%H:%M:%S", TIMESTAMP);'
                conn.execute(sqlalchemy.text(format_sql))
                # --- KONIEC KLUCZOWEJ ZMIANY ---

                # Krok 7: Wykonaj operację INSERT OR REPLACE
                columns_str = ", ".join([f'"{c}"' for c in df_to_save.columns])
                upsert_sql = f'INSERT OR REPLACE INTO "{table_name}" ({columns_str}) SELECT {columns_str} FROM "{temp_table_name}"'
                conn.execute(sqlalchemy.text(upsert_sql))

                # Krok 8: Usuń tabelę tymczasową
                conn.execute(sqlalchemy.text(f'DROP TABLE "{temp_table_name}"'))
                conn.commit()

                # Krok 9: Zaktualizuj tabele metadanych
                with conn.begin():
                    conn.execute(sqlalchemy.text("INSERT OR IGNORE INTO stations (station_id, name, latitude, longitude) VALUES (:sid, :name, :lat, :lon)"), 
                                   {"sid": station_id, "name": station_id, "lat": coords['lat'], "lon": coords['lon']})
                    conn.execute(sqlalchemy.text("INSERT OR IGNORE INTO groups (group_id, station_id, interval) VALUES (:gid, :sid, :intv)"), 
                                   {"gid": group_id, "sid": station_id, "intv": config.get('interval', 'N/A')})
                
                logging.info(f"Zapisano/zaktualizowano {len(df_to_save)} wierszy w tabeli '{table_name}'.")
        except Exception as e:
            logging.error(f"Krytyczny błąd zapisu do bazy danych dla grupy '{group_id}': {e}", exc_info=True)
            
def save_dataframe_to_csv(final_df: pd.DataFrame, year: int, config: dict, lock: multiprocessing.Lock):
    """
    Zapisuje ramkę danych do pliku CSV z logiką 'uzupełnij' lub 'nadpisz'.
    Wersja 7.10: Usunięto modyfikacje inplace, aby uniknąć efektów ubocznych.
    """
    if final_df.empty:
        return

    group_id = config['file_id']
    output_dir = Path(config['output_dir'])
    output_filepath = output_dir / str(year) / f"{group_id}.csv"
    overwrite_mode = config.get('overwrite', False)

    with lock:
        try:
            output_filepath.parent.mkdir(parents=True, exist_ok=True)
            
            existing_df = pd.DataFrame()
            if output_filepath.exists():
                try:
                    temp_df = pd.read_csv(output_filepath, parse_dates=['TIMESTAMP'], low_memory=False)
                    if 'TIMESTAMP' in temp_df.columns:
                        existing_df = temp_df
                except Exception:
                    logging.warning(f"Błąd odczytu {output_filepath.name}. Plik zostanie nadpisany.")
            
            if not existing_df.empty:
                # Użyj kopii, aby nie modyfikować oryginalnej ramki danych
                df_indexed = final_df.set_index('TIMESTAMP')
                existing_df_indexed = existing_df.set_index('TIMESTAMP')

                if overwrite_mode:
                    merged_df = df_indexed.combine_first(existing_df_indexed)
                else:
                    merged_df = existing_df_indexed.combine_first(df_indexed)
                
                df_to_save = merged_df.reset_index()
            else:
                df_to_save = final_df.copy()

            all_columns = sorted([col for col in df_to_save.columns if col != 'TIMESTAMP'])
            all_columns.insert(0, 'TIMESTAMP')
            
            df_to_save = df_to_save[all_columns]
            df_to_save.sort_values(by='TIMESTAMP', inplace=True)

            logging.info(f"Zapisywanie {len(df_to_save)} wierszy do pliku CSV: {output_filepath}")
            df_to_save.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')

        except Exception as e:
            logging.error(f"Błąd podczas zapisu do pliku CSV {output_filepath}: {e}", exc_info=True)


# --- FUNKCJE PRZETWARZANIA DANYCH (WSPÓLNE) ---
def decode_csi_fs2_float(raw_short_int_le: int) -> float:
    uint16_val_le=raw_short_int_le&0xFFFF;byte_stream_0=uint16_val_le&0xFF;byte_stream_1=(uint16_val_le>>8)&0xFF;fs_word=(byte_stream_0<<8)|byte_stream_1
    POS_INFINITY_FS2,NEG_INFINITY_FS2,NOT_A_NUMBER_FS2=0x1FFF,0x9FFF,0x9FFE
    if fs_word==POS_INFINITY_FS2:return float('inf')
    elif fs_word==NEG_INFINITY_FS2:return float('-inf')
    elif fs_word==NOT_A_NUMBER_FS2:return float('nan')
    is_negative=(fs_word&0x8000)!=0;mantissa_val=fs_word&0x1FFF;exponent_val=(fs_word&0x6000)>>13;rtn=float(mantissa_val)
    if mantissa_val!=0:
        for _ in range(exponent_val):rtn/=10.0
    else:rtn=0.0
    if is_negative and mantissa_val!=0:rtn*=-1.0
    return rtn
    
def get_tob1_metadata(file_path):
    try:
        with open(file_path,'r',encoding='latin-1')as f:header_lines=[f.readline().strip()for _ in range(5)]
        if not header_lines[0].startswith('"TOB1"'):return None
        col_names_from_header=parse_header_line(header_lines[1]);binary_formats_str_from_header=parse_header_line(header_lines[4])
        struct_pattern_prefix='<';actual_col_names_to_use,actual_struct_pattern_parts,fp2_column_names=[],[],[]
        for i,original_fmt_str in enumerate(binary_formats_str_from_header):
            fmt_str_upper=original_fmt_str.upper();base_col_name=col_names_from_header[i]if i<len(col_names_from_header)else f"DataCol_idx{i}";fmt_code=None
            if fmt_str_upper.startswith("ASCII("):
                try:
                    length_str=fmt_str_upper[len("ASCII("):-1]
                    if length_str.isdigit():length=int(length_str)
                    if length>0:fmt_code=f"{length}s"
                except ValueError:pass
            elif fmt_str_upper=="FP2":fmt_code='h';fp2_column_names.append(base_col_name)
            elif fmt_str_upper=="SECNANO":fmt_code=None
            else:fmt_code=STRUCT_FORMAT_MAP.get(fmt_str_upper)
            if fmt_code:actual_struct_pattern_parts.append(fmt_code);actual_col_names_to_use.append(base_col_name)
        if not actual_struct_pattern_parts:return None
        final_struct_pattern=struct_pattern_prefix+"".join(actual_struct_pattern_parts)
        if len(actual_col_names_to_use)!=len(actual_struct_pattern_parts):return None
        return actual_col_names_to_use,final_struct_pattern,5,fp2_column_names
    except Exception:return None

def read_tob1_data(file_path: Path, metadata: tuple) -> pd.DataFrame:
    """
    (Wersja 2.1) Zawiera poprawkę na błąd typu danych przekazywanych
    do dekodera FP2, co zapewnia stabilność przy przetwarzaniu binarnym.
    """
    col_names, struct_pattern, num_header_lines, fp2_cols = metadata
    all_chunks = []
    try:
        record_size = struct.calcsize(struct_pattern)
        if record_size == 0: return pd.DataFrame()

        with open(file_path, 'rb') as f:
            for _ in range(num_header_lines): f.readline()
            
            chunk_size_rows = 100_000
            while True:
                records_in_chunk = []
                for _ in range(chunk_size_rows):
                    chunk_bytes = f.read(record_size)
                    if not chunk_bytes or len(chunk_bytes) < record_size:
                        break
                    records_in_chunk.append(struct.unpack(struct_pattern, chunk_bytes))

                if not records_in_chunk: break

                chunk_df = pd.DataFrame(records_in_chunk, columns=col_names)
                
                if not chunk_df.empty and fp2_cols:
                    for fp2_col_name in fp2_cols:
                        if fp2_col_name in chunk_df.columns:
                            # === POCZĄTEK POPRAWKI ===
                            # Krok 1: Konwertuj na typ numeryczny, zamieniając błędy na NaN
                            numeric_series = pd.to_numeric(chunk_df[fp2_col_name], errors='coerce')
                            # Krok 2: Wypełnij ewentualne NaN zerem i rzutuj na typ integer
                            integer_series = numeric_series.fillna(0).astype(int)
                            # Krok 3: Dopiero teraz zastosuj funkcję dekodującą
                            chunk_df[fp2_col_name] = integer_series.apply(decode_csi_fs2_float)
                            # === KONIEC POPRAWKI ===
                
                if 'SECONDS' in chunk_df.columns and 'NANOSECONDS' in chunk_df.columns:
                    secs = pd.to_numeric(chunk_df['SECONDS'], errors='coerce')
                    nanos = pd.to_numeric(chunk_df['NANOSECONDS'], errors='coerce')
                    chunk_df['TIMESTAMP'] = CAMPBELL_EPOCH + pd.to_timedelta(secs, unit='s') + pd.to_timedelta(nanos, unit='ns')
                
                all_chunks.append(chunk_df)

                if len(records_in_chunk) < chunk_size_rows: break

        if not all_chunks: return pd.DataFrame()
        
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df['source_file'] = str(file_path.resolve())
        return final_df

    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu TOB1 z {file_path.name}: {e}", exc_info=True)
        return pd.DataFrame()

def get_toa5_metadata(file_path: Path) -> tuple[list[str], int] | None:
    """Parses a TOA5 file header."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            header_lines = [f.readline().strip() for _ in range(4)]
        if not header_lines[0].startswith('"TOA5"'): return None
        return parse_header_line(header_lines[1]), 4
    except Exception as e:
        logging.error(f"Błąd parsowania nagłówka TOA5 w {file_path.name}: {e}")
        return None

def read_toa5_data(file_path: Path, metadata: tuple) -> pd.DataFrame:
    """
    (Wersja 2.1) Wczytuje dane TOA5 w porcjach (chunks), aby oszczędzać pamięć
    przy bardzo dużych plikach.
    """
    col_names, num_header_lines = metadata
    all_chunks = []
    try:
        # Użyj chunksize, aby wczytywać plik porcjami po 100 000 wierszy
        chunk_iterator = pd.read_csv(
            file_path, skiprows=num_header_lines, header=None, names=col_names,
            na_values=['"NAN"', 'NAN', '"INF"', '""', ''], quotechar='"',
            encoding='latin-1', on_bad_lines='warn',
            chunksize=100_000
        )

        for chunk_df in chunk_iterator:
            if 'TIMESTAMP' in chunk_df.columns:
                # Wykonaj czyszczenie dat dla każdej porcji
                timestamps_str = chunk_df['TIMESTAMP'].astype(str)
                cleaned_timestamps_str = timestamps_str.str.replace('.0-', '-', regex=False)
                chunk_df['TIMESTAMP'] = pd.to_datetime(cleaned_timestamps_str, errors='coerce')
                all_chunks.append(chunk_df)
        
        if not all_chunks:
            return pd.DataFrame()

        # Połącz wszystkie przetworzone porcje w jedną ramkę
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.dropna(subset=['TIMESTAMP'], inplace=True)
        final_df['source_file'] = str(file_path.resolve())
        final_df = clean_column_names(final_df)
        return final_df
    except Exception as e:
        logging.error(f"Błąd odczytu danych TOA5 z {file_path.name}: {e}")
        return pd.DataFrame()

def read_simple_csv_data(file_path: Path) -> pd.DataFrame:
    """
    Wczytuje dane CSV, pomijając zdefiniowane kolumny i obsługując niestandardowe wartości NaN.
    """
    all_chunks = []
    try:
        # Zdefiniuj listę wartości, które mają być traktowane jako NaN (brak danych)
        custom_nan_values = ["OverRange", "UnderRange", "NAN", "INF", "-INF", ""]

        chunk_iterator = pd.read_csv(
            file_path,
            header=0,
            low_memory=False, 
            encoding='latin-1',
            on_bad_lines='warn',
            chunksize=100_000,
            na_values=custom_nan_values, # <-- KLUCZOWA ZMIANA
            usecols=lambda col_name: col_name not in COLUMNS_TO_EXCLUDE_FROM_CSV
        )
        
        for chunk_df in chunk_iterator:
            if 'Timestamp' not in chunk_df.columns and 'TIMESTAMP' not in chunk_df.columns:
                continue 
            
            if 'Timestamp' in chunk_df.columns:
                chunk_df.rename(columns={'Timestamp': 'TIMESTAMP'}, inplace=True)

            chunk_df['TIMESTAMP'] = pd.to_datetime(chunk_df['TIMESTAMP'], errors='coerce')
            all_chunks.append(chunk_df)

        if not all_chunks:
            return pd.DataFrame()
            
        final_df = pd.concat(all_chunks, ignore_index=True)
        final_df.dropna(subset=['TIMESTAMP'], inplace=True)
        
        final_df['source_filename'] = file_path.name
        final_df['original_row_index'] = final_df.index
        final_df['source_filepath'] = str(file_path.resolve())
        
        final_df = clean_column_names(final_df)
        return final_df
    except Exception as e:
        logging.error(f"Krytyczny błąd odczytu SimpleCSV z {file_path.name}: {e}")
        return pd.DataFrame()
        
def matlab_to_datetime(matlab_datenum: float) -> datetime:
    """Konwertuje numer seryjny daty z MATLABa na obiekt datetime Pythona."""
    return datetime.fromordinal(int(matlab_datenum)) + timedelta(days=matlab_datenum % 1) - timedelta(days=366)

def rename_group_id(group_id):
    if group_id.startswith('TL1'):
        return 'TR' + group_id[3:]
    elif group_id.startswith('TL2'):
        return 'TR2' + group_id[3:]
    else:
        return group_id
        
def load_matlab_data(year: int, config: dict) -> pd.DataFrame:
    """
    (Wersja 3.2) Zawiera poprawki na warunek interwału (<= 5s) oraz
    inteligentne odczytywanie nazwy zmiennej z plików .mat.
    """
    group_id = config['file_id']
    main_input_path_str = config.get('main_input_path')
    group_id = rename_group_id(group_id)
    if not main_input_path_str:
        return pd.DataFrame()

    try:
        parts = group_id.split('_', 1)
        if len(parts) < 2: return pd.DataFrame()
        station_code, matlab_folder_name = parts
        base_project_path = Path(main_input_path_str).parent.parent
        matlab_base_path = base_project_path / f"met-data_{station_code.upper()}"
        data_path = matlab_base_path / str(year) / matlab_folder_name / "zero_level"
        logging.debug(f"Ścieżka do .MAT: {data_path}")
    except Exception:
        return pd.DataFrame()

    if not data_path.exists():
        return pd.DataFrame()

    interval = config.get('interval', '')
    # === POCZĄTEK POPRAWKI #1: Poprawny warunek interwału ===
    is_monthly = ('sec' in interval or 's' in interval) and int(re.sub(r'\D', '', interval)) <= 5
    # === KONIEC POPRAWKI #1 ===
    
    monthly_dfs = []
    months_to_check = range(1, 13) if is_monthly else [None]

    for month in months_to_check:
        try:
            time_file_name = f"tv_{month:02d}.mat" if month else "tv.mat"
            time_file_path = data_path / time_file_name
            if not time_file_path.exists(): continue

            # === POCZĄTEK POPRAWKI #2: Inteligentne odczytywanie klucza ===
            mat_contents = loadmat(time_file_path)
            time_key = next((k for k in mat_contents if not k.startswith('__')), None)
            if not time_key:
                logging.warning(f"Nie znaleziono zmiennej z danymi w pliku czasu: {time_file_path.name}")
                continue
            time_vector_raw = mat_contents[time_key].flatten()
            # === KONIEC POPRAWKI #2 ===
            
            timestamps = [matlab_to_datetime(t) for t in time_vector_raw]
            
            var_files = [f for f in data_path.glob('*.mat') if not f.name.startswith('tv')]
            matlab_data = {'TIMESTAMP': timestamps}
            
            time_file_name_stem = "tv" if not month else f"tv_{month:02d}"
            
            for var_file in var_files:
                var_name = var_file.stem
                clean_var_name = var_name.rsplit('_', 1)[0] if month else var_name

                # Pomiń pliki wektora czasu w tej pętli, przetworzyliśmy je wcześniej
                if clean_var_name == 'tv':
                    continue

                try:
                    mat_contents = loadmat(var_file)
                except Exception as e:
                    logging.warning(f"Nie można odczytać pliku {var_file.name}, pomijam. Błąd: {e}")
                    continue

                data_key = next((k for k in mat_contents if not k.startswith('__')), None)
                if data_key:
                    data_array = mat_contents[data_key]

                    # Sprawdź, czy tablica jest 2-wymiarowa i ma więcej niż 1 kolumnę
                    if data_array.ndim == 2 and data_array.shape[1] > 1:
                        if len(timestamps) == data_array.shape[0]:
                             # Scenariusz 1: Liczba wierszy pasuje do wektora czasu - prawdopodobnie dane są w kolumnach
                             logging.warning(f"Plik {var_file.name} zawiera {data_array.shape[1]} kolumn. Wybieram ostatnią.")
                             data = data_array[:, -1]
                        else:
                             # Scenariusz 2: Liczba wierszy nie pasuje - prawdopodobnie dane są w wierszach
                             logging.warning(f"Plik {var_file.name} ma wymiary {data_array.shape}, ale wygląda na wierszowy. Spłaszczam.")
                             data = data_array.flatten()
                    else:
                        data = data_array.flatten()
                    
                    matlab_data[clean_var_name] = data
            
            monthly_dfs.append(pd.DataFrame(matlab_data))

        except KeyError as e:
             logging.warning(f"Błąd klucza podczas wczytywania danych MATLABa dla {year}/{month or ''}: {e}")
             continue
        except Exception as e:
            logging.warning(f"Inny błąd podczas wczytywania danych MATLABa dla {year}/{month or ''}: {e}")
            continue

    if not monthly_dfs:
        return pd.DataFrame()

    final_df = pd.concat(monthly_dfs, ignore_index=True)
    final_df = clean_column_names(final_df)
    return final_df
    
def apply_timezone_correction(ts_series_naive: pd.Series, file_id: str) -> pd.Series:
    """
    Stosuje korekty stref czasowych i zawsze zwraca serię w formacie "naiwnym".
    Wersja 7.11: Poprawia błąd, który błędnie nadawał strefę GMT, gdy brakowało reguły.
    """
    if ts_series_naive.empty:
        return ts_series_naive
    
    config_entry = TIMEZONE_CORRECTIONS.get(file_id)
    final_config = None
    if isinstance(config_entry, str):
        final_config = TIMEZONE_CORRECTIONS.get(config_entry)
    elif isinstance(config_entry, dict):
        final_config = config_entry

    if not final_config:
        # --- POCZĄTEK POPRAWKI ---
        # Jeśli nie ma reguły, po prostu upewnij się, że dane są w formacie datetime i zwróć je jako naiwne.
        return pd.to_datetime(ts_series_naive)
        # --- KONIEC POPRAWKI ---

    source_tz = final_config['source_tz']
    post_correction_tz = final_config['post_correction_tz']
    target_tz = final_config['target_tz']
    correction_end_date = pd.to_datetime(final_config['correction_end_date'])

    pre_mask = ts_series_naive <= correction_end_date
    post_mask = ~pre_mask

    pre_series = ts_series_naive[pre_mask].dt.tz_localize(source_tz, ambiguous='NaT', nonexistent='NaT')
    post_series = ts_series_naive[post_mask].dt.tz_localize(post_correction_tz, ambiguous='NaT', nonexistent='NaT')

    series_to_concat = [s for s in [pre_series, post_series] if not s.empty]
    
    if not series_to_concat:
        return pd.Series(dtype='datetime64[ns]')

    combined_series = pd.to_datetime(pd.concat(series_to_concat).sort_index(), errors='coerce', utc=True)

    if combined_series.empty:
        return combined_series

    # Zawsze zwracaj dane bez strefy czasowej (naiwne)
    return combined_series.dt.tz_convert(target_tz).dt.tz_localize(None)

def apply_manual_time_shifts(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """(Wersja 2.0) Poprawiona, aby działać na naiwnych znacznikach czasu."""
    config_entry = MANUAL_TIME_SHIFTS.get(file_id)
    rules = None
    if isinstance(config_entry, str):
        rules = MANUAL_TIME_SHIFTS.get(config_entry)
    elif isinstance(config_entry, list):
        rules = config_entry

    if not rules or df.empty:
        return df
    
    df_out = df.copy()
    for rule in rules:
        try:
            # Tworzymy naiwne daty do porównania
            start_ts = pd.to_datetime(rule['start'])
            end_ts = pd.to_datetime(rule['end'])
            offset = pd.Timedelta(hours=rule['offset_hours'])
            mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)
            if mask.any():
                df_out.loc[mask, 'TIMESTAMP'] += offset
        except Exception as e:
            logging.warning(f"Błąd reguły manualnej dla '{file_id}': {e}.")
    return df_out

# W unified_script.py - ZASTĄP CAŁĄ TĘ FUNKCJĘ

# W unified_script.py - ZASTĄP CAŁĄ TĘ FUNKCJĘ

def apply_calibration(df: pd.DataFrame, file_id: str) -> pd.DataFrame:
    """
    (Wersja Ostateczna) Stosuje reguły kalibracyjne z gwarancją, że dane
    spoza zdefiniowanego okresu pozostają nietknięte.
    """
    station_name = STATION_MAPPING_FOR_CALIBRATION.get(file_id)
    if not station_name or station_name not in CALIBRATION_RULES_BY_STATION:
        return df

    column_rules = CALIBRATION_RULES_BY_STATION[station_name]
    df_calibrated = df.copy()

    # Przetwarzanie specjalnych reguł (np. _SWAP_RADIATION)
    for col_name, rules_list in column_rules.items():
        if not col_name.startswith('_'):
            continue
        
        for rule in rules_list:
            if rule.get('type') == 'formula_swap':
                try:
                    start_ts = pd.to_datetime(rule['start'])
                    end_ts = pd.to_datetime(rule['end'])
                    mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                    
                    if not mask.any():
                        continue

                    logging.info(f"Stosowanie reguły zamiany kanałów '{col_name}' dla grupy '{file_id}'.")

                    # --- POCZĄTEK NOWEJ, BEZPIECZNEJ I UPROSZCZONEJ LOGIKI ---

                    # Krok 1: Obliczenia w tle (do kolumn tymczasowych)
                    # Obliczamy nowe wartości i zapisujemy je w osobnych, tymczasowych kolumnach.
                    # Jest to niezbędne, aby wszystkie formuły w regule używały spójnych danych wejściowych.
                    swaps = rule.get('swaps', {})
                    temp_cols = list(swaps.keys())
                    for temp_col, expression in swaps.items():
                        df_calibrated.loc[mask, temp_col] = df_calibrated.loc[mask].eval(expression)

                    # Krok 2: PRECYZYJNE WSTAWIENIE DANYCH
                    # Bezpośrednio przypisujemy obliczone wartości do docelowych kolumn,
                    # ale tylko dla wierszy objętych maską czasową.
                    final_mapping = rule.get('final_mapping', {})
                    for final_col, temp_col in final_mapping.items():
                        if final_col in df_calibrated.columns and temp_col in df_calibrated.columns:
                            # To jest najbardziej bezpośrednia i bezpieczna metoda w pandas.
                            # Mówi ona: "dla kolumny `final_col`, w wierszach `mask`,
                            # wstaw wartości z kolumny `temp_col` z tych samych wierszy `mask`".
                            df_calibrated.loc[mask, final_col] = df_calibrated.loc[mask, temp_col]

                    # Krok 3: Bezpieczne usunięcie kolumn tymczasowych
                    # Po wstawieniu danych, tymczasowe kolumny są już niepotrzebne.
                    df_calibrated.drop(columns=temp_cols, inplace=True, errors='ignore')

                    # --- KONIEC NOWEJ LOGIKI ---

                except Exception as e:
                    logging.warning(f"Błąd reguły zamiany kanałów '{col_name}': {e}", exc_info=True)

    # Przetwarzanie standardowych reguł kalibracyjnych (bez zmian, ta część działała poprawnie)
    for col_name, rules_list in column_rules.items():
        if col_name.startswith('_'):
            continue

        if col_name not in df_calibrated.columns:
            continue

        for rule in rules_list:
            try:
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
                mask = (df_calibrated['TIMESTAMP'] >= start_ts) & (df_calibrated['TIMESTAMP'] <= end_ts)
                
                if not mask.any():
                    continue

                rule_type = rule.get('type', 'simple')

                if rule_type == 'simple':
                    df_calibrated[col_name] = pd.to_numeric(df_calibrated[col_name], errors='coerce')
                    multiplier = float(rule.get('multiplier', 1.0))
                    addend = float(rule.get('addend', 0.0))
                    df_calibrated.loc[mask, col_name] = (df_calibrated.loc[mask, col_name] * multiplier) + addend
                
                elif rule_type == 'formula':
                    expression = rule.get('expression')
                    if not expression:
                        continue
                    
                    constants = rule.get('constants', {})
                    df_calibrated.loc[mask, col_name] = df_calibrated[mask].eval(
                        expression,
                        local_dict=constants
                    )
            except Exception as e:
                logging.warning(f"Błąd standardowej reguły kalibracji dla '{col_name}': {e}")

    return df_calibrated

def apply_value_range_flags(df: pd.DataFrame) -> pd.DataFrame:
    # apply quality flags for values outside of defined ranges
    if df.empty or not VALUE_RANGE_FLAGS: return df
    df_out = df.copy()
    for prefix, range_dict in VALUE_RANGE_FLAGS.items():
        target_cols = [col for col in df_out.columns if str(col).startswith(prefix)]
        for col_name in target_cols:
            numeric_col = pd.to_numeric(df_out[col_name], errors='coerce')
            min_val, max_val = range_dict.get('min', -float('inf')), range_dict.get('max', float('inf'))
            out_of_range_mask = (numeric_col < min_val) | (numeric_col > max_val)
            if out_of_range_mask.any():
                flag_col_name = f"{col_name}_flag"
                if flag_col_name not in df_out.columns: df_out[flag_col_name] = 0
                df_out[flag_col_name] = pd.to_numeric(df_out[flag_col_name], errors='coerce').fillna(0).astype(int)
                df_out.loc[out_of_range_mask, flag_col_name] = 4
    return df_out

def apply_quality_flags(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Dodaje flagi jakości, używając dwuetapowego systemu słowników.
    Wersja 7.9: Poprawia błąd, wskazując na właściwy słownik reguł (QUALITY_FLAGS).
    """
    group_id = config.get('file_id')
    if not group_id or df.empty:
        return df

    # Krok 1: Znajdź nazwę zestawu reguł dla danej grupy
    ruleset_name = STATION_MAPPING_FOR_QC.get(group_id)
    if not ruleset_name:
        return df  # Celowy brak reguł dla tej grupy

    # --- POCZĄTEK POPRAWKI ---
    # Krok 2: Pobierz właściwy słownik z regułami z QUALITY_FLAGS (a nie STATION_MAPPING_FOR_QC)
    station_rules = QUALITY_FLAGS.get(ruleset_name)
    # --- KONIEC POPRAWKI ---
    if not station_rules:
        logging.warning(f"Nie znaleziono definicji reguł '{ruleset_name}' w QUALITY_FLAGS dla grupy '{group_id}'.")
        return df

    df_out = df.copy()
    # ... (reszta funkcji pozostaje bez zmian) ...
    for col_to_flag, rules_list in station_rules.items():
        if col_to_flag == '*':
            target_cols = [c for c in df_out.select_dtypes(include='number').columns if not c.endswith('_flag')]
        elif col_to_flag in df_out.columns:
            target_cols = [col_to_flag]
        else:
            continue

        for rule in rules_list:
            try:
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
                final_mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)

                filename_filter = rule.get('filename_contains')
                if filename_filter:
                    if 'source_file' in df_out.columns:
                        file_mask = df_out['source_file'].str.contains(filename_filter, na=False, regex=False)
                        final_mask &= file_mask
                    else:
                        continue
                
                if final_mask.any():
                    for col_name in target_cols:
                        flag_col_name = f"{col_name}_flag"
                        if flag_col_name not in df_out.columns:
                            df_out[flag_col_name] = 0
                        
                        df_out.loc[final_mask, flag_col_name] = rule['flag_value']
            except Exception as e:
                logging.warning(f"Błąd reguły flagowania dla '{group_id}' (kolumna: {col_to_flag}): {e}")
                
    return df_out

def align_timestamp(df: pd.DataFrame, force_interval: str) -> pd.DataFrame:
    """Rounds timestamps to a specified frequency."""
    if df.empty or not force_interval: return df
    try:
        df.loc[:, 'TIMESTAMP'] = df['TIMESTAMP'].dt.round(freq=force_interval)
    except Exception as e:
        logging.error(f"Błąd podczas wyrównywania czasu do interwału '{force_interval}': {e}")
    return df

def filter_by_realistic_date_range(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """
    (WERSJA DOCELOWA 2.2) Usuwa wiersze z nierealistycznymi znacznikami czasu.
    Ujednolica strefy czasowe przed porównaniem, aby uniknąć błędów.
    """
    if df.empty or 'TIMESTAMP' not in df.columns:
        return df

    try:
        # Krok 1: Wstępne czyszczenie i konwersja (bez zmian)
        timestamps_str = df['TIMESTAMP'].astype(str)
        cleaned_timestamps_str = timestamps_str.str.replace('.0-', '-', regex=False)
        timestamps_series = pd.to_datetime(cleaned_timestamps_str, errors='coerce')

        # Krok 2: Wstępne filtrowanie na podstawie poprawności dat
        valid_mask = timestamps_series.notna()
        df = df[valid_mask]
        timestamps = timestamps_series[valid_mask]

        if timestamps.empty:
            return df

        # === POCZĄTEK KLUCZOWEJ ZMIANY ===

        # Krok 3: Ujednolicenie danych do strefy UTC na potrzeby porównania
        if timestamps.dt.tz is None:
            # Jeśli dane są "naiwne", przypisujemy im strefę UTC
            timestamps_utc = timestamps.dt.tz_localize('UTC')
        else:
            # Jeśli dane już mają strefę, konwertujemy je do UTC
            timestamps_utc = timestamps.dt.tz_convert('UTC')

        # Krok 4: Wyznaczenie mediany z danych w UTC
        median_year = timestamps_utc.dt.year.median()

        # Krok 5: Stworzenie dat granicznych, RÓWNIEŻ w strefie UTC
        start_date_utc = pd.Timestamp(f'{int(median_year) - 2}-01-01', tz='UTC')
        end_date_utc = pd.Timestamp(f'{int(median_year) + 2}-12-31', tz='UTC')

        # Krok 6: Wykonanie bezpiecznego porównania (obie strony są w UTC)
        final_mask = (timestamps_utc >= start_date_utc) & (timestamps_utc <= end_date_utc)
        
        # === KONIEC KLUCZOWEJ ZMIANY ===

        # Zastosuj ostateczną maskę do przefiltrowanego już DataFrame
        filtered_df = df.loc[final_mask]

        rows_removed = len(df) - len(filtered_df)
        if rows_removed > 0:
             logging.info(f"Filtr dat (mediana): Usunięto {rows_removed} wierszy spoza zakresu {int(median_year)}±2 lata w pliku {file_path.name}.")

        return filtered_df
        
    except Exception as e:
        logging.warning(f"Błąd podczas filtrowania po dacie (mediana) dla pliku {file_path.name}: {e}")
        return df

def correct_and_report_chronology(
    df: pd.DataFrame,
    context_name: str,
    known_interval: str,
    timestamp_col: str = 'TIMESTAMP',
    tolerance: str = '2s'
) -> pd.DataFrame:
    """
    Koryguje chronologię, budując nową, monotoniczną oś czasu z uwzględnieniem tolerancji.

    Algorytm utrzymuje "oczekiwany" znacznik czasu. Ufa oryginalnym znacznikom,
    jeśli mieszczą się one w zadanym progu tolerancji wokół oczekiwanego punktu.
    Gdy napotka poprawny znacznik, który wyprzedza oczekiwaną oś czasu (np. po
    przerwie w danych), resynchronizuje się do tej nowej, poprawnej wartości.
    """
    if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        return df

    try:
        interval_td = pd.to_timedelta(known_interval)
        tolerance_td = pd.to_timedelta(tolerance)
    except ValueError as e:
        logging.error(f"Nieprawidłowy format interwału lub tolerancji: {e}")
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)

    original_ts = pd.to_datetime(df_corrected[timestamp_col], errors='coerce').to_numpy()
    corrected_ts = original_ts.copy()
    chronology_tag = np.zeros(len(df_corrected), dtype=int)
    
    first_valid_idx = pd.Series(original_ts).first_valid_index()
    if first_valid_idx is None:
        logging.warning(f"Brak prawidłowych znaczników czasu w '{context_name}'.")
        return df_corrected

    last_good_ts = corrected_ts[first_valid_idx]
    any_fix_made = False
    block_start_index = -1

    def finalize_block(end_index: int):
        nonlocal block_start_index
        if block_start_index == -1: return
        # Funkcja logująca (bez zmian)
        log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        start_row, end_row = df_corrected.loc[block_start_index], df_corrected.loc[end_index]
        src_path = start_row.get('source_filepath', 'N/A')
        start_idx_orig, end_idx_orig = start_row.get('original_row_index', 'N/A'), end_row.get('original_row_index', 'N/A')
        original_start = pd.to_datetime(original_ts[block_start_index]).strftime('%Y-%m-%dT%H:%M:%S')
        original_end = pd.to_datetime(original_ts[end_index]).strftime('%Y-%m-%dT%H:%M:%S')
        corrected_start = pd.to_datetime(corrected_ts[block_start_index]).strftime('%Y-%m-%dT%H:%M:%S')
        corrected_end = pd.to_datetime(corrected_ts[end_index]).strftime('%Y-%m-%dT%H:%M:%S')
        log_line = f"{log_time};{src_path};{start_idx_orig};{end_idx_orig};{original_start};{original_end};{corrected_start};{corrected_end}"
        if chronology_logger: chronology_logger.info(log_line)
        block_start_index = -1

    # --- Pętla z logiką resynchronizacji i tolerancji ---
    for i in range(first_valid_idx + 1, len(corrected_ts)):
        current_original_ts = pd.to_datetime(original_ts[i])
        expected_ts = pd.to_datetime(last_good_ts) + interval_td

        # Jeśli oryginalny znacznik czasu jest wiarygodny i mieści się w tolerancji...
        if pd.notna(current_original_ts) and current_original_ts >= (expected_ts - tolerance_td):
            if block_start_index != -1:
                finalize_block(i - 1)
            
            # RESYNCHRONIZUJ oś czasu do tej nowej, wiarygodnej wartości.
            last_good_ts = current_original_ts
        else:
            # Oryginalny znacznik jest niewiarygodny.
            if block_start_index == -1:
                block_start_index = i
            
            # NADPISZ wartość, używając idealnego, oczekiwanego kroku.
            corrected_ts[i] = expected_ts
            chronology_tag[i] = 1
            any_fix_made = True
            
            # Przesuń naszą oś czasu do przodu zgodnie z korektą.
            last_good_ts = expected_ts

    if block_start_index != -1:
        finalize_block(len(corrected_ts) - 1)

    if any_fix_made:
        df_corrected[timestamp_col] = pd.to_datetime(corrected_ts)
        df_corrected['chronology_tag'] = chronology_tag
    
    return df_corrected
    
def diagnose_chronology_blocks(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Skanuje i loguje bloki z błędną chronologią
    zgodnie z zaawansowaną specyfikacją użytkownika.
    """
    logging.info(f"--- URUCHAMIANIE ZAAWANSOWANEJ DIAGNOZY CHRONOLOGII DLA: {context_name} ---")
    if df.empty or len(df) < 2 or timestamp_col not in df.columns:
        return df

    interval_td = pd.to_timedelta(known_interval)
    df_diag = df.copy()
    df_diag.reset_index(drop=True, inplace=True)
    
    timestamps = df_diag[timestamp_col].to_numpy()
    
    is_in_block = False
    
    for i in range(1, len(timestamps)):
        last_ts = pd.to_datetime(timestamps[i-1])
        current_ts = pd.to_datetime(timestamps[i])
        diff = current_ts - last_ts

        row_info = df_diag.loc[i]
        log_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        if not is_in_block:
            # Punkt 1: Znajdź "prawdziwy początek" nowego bloku
            if diff < pd.Timedelta(0):
                is_in_block = True
                log_entry = (f"{log_time};POCZATEK_BLOKU;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wykryto pierwsze cofnięcie czasu z {last_ts.strftime('%Y-%m-%dT%H:%M:%S')}")
                if chronology_logger: chronology_logger.info(log_entry)
        else:
            # Jesteśmy wewnątrz bloku - szukamy błędów wewnętrznych lub końca
            
            # Punkt 3: Znajdź "właściwy koniec" bloku (duży skok w przód)
            if diff > pd.Timedelta(hours=12):
                is_in_block = False
                end_row_info = df_diag.loc[i-1] # Blok zakończył się na poprzednim wierszu
                log_entry = (f"{log_time};KONIEC_BLOKU;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{end_row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wykryto duży skok w przód o {diff}, kończący blok.")
                if chronology_logger: chronology_logger.info(log_entry)

            # Punkt 2: Znajdź "błędy wewnętrzne"
            elif diff < pd.Timedelta(0):
                log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Wewnętrzne cofnięcie czasu wewnątrz bloku.")
                if chronology_logger: chronology_logger.info(log_entry)
            elif diff > (interval_td * 1.5) and diff <= pd.Timedelta(hours=12):
                 log_entry = (f"{log_time};BLAD_WEWNETRZNY;"
                             f"{row_info.get('source_filename', 'N/A')};"
                             f"{row_info.get('original_row_index', 'N/A')};"
                             f"{current_ts.strftime('%Y-%m-%dT%H:%M:%S')};"
                             f"Krótka przerwa lub nieregularność ({diff}) wewnątrz bloku.")
                 if chronology_logger: chronology_logger.info(log_entry)

    logging.info(f"Zakończono diagnozę chronologii. Sprawdź plik {CHRONOLOGY_LOG_FILENAME.name}.")
    
    # Zwróć oryginalną, niezmodyfikowaną ramkę danych
    return df
    
def diagnose_chronology_scan(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Generuje szczegółowy log
    do analizy działania algorytmu "skanowania w przód" na problematycznym zbiorze.
    """
    logging.info(f"--- URUCHAMIANIE TRYBU DIAGNOSTYCZNEGO DLA CHRONOLOGII (SKANOWANIE) ---")
    if df.empty or timestamp_col not in df.columns:
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)
    timestamps = df_corrected[timestamp_col].to_numpy()
    
    i = 1
    while i < len(timestamps):
        if timestamps[i] <= timestamps[i-1]:
            start_index = i
            last_good_ts = timestamps[i-1]
            
            logging.debug(f"\n{'='*20} ROZPOCZĘCIE DIAGNOZY BLOKU BŁĘDÓW {'='*20}")
            logging.debug(f"Wykryto błąd w wierszu: {start_index}")
            logging.debug(f"Ostatni poprawny znacznik czasu (last_good_ts): {pd.to_datetime(last_good_ts)}")
            logging.debug("Rozpoczynam skanowanie w przód w poszukiwaniu końca bloku...")
            logging.debug("-" * 80)
            logging.debug(f"{'Indeks (j)':<12} | {'Oryginalny Czas [j]':<28} | Warunek: Oryginalny > Ostatni Poprawny? | Wynik")
            logging.debug("-" * 80)

            end_index = -1
            for j in range(start_index, len(timestamps)):
                current_original_ts = timestamps[j]
                condition_met = current_original_ts > last_good_ts
                
                # Zaloguj tylko kluczowe momenty, aby nie zalać logu:
                # - Pierwsze 10 iteracji w bloku
                # - Co 500-ny wiersz
                # - Ostatnie 10 wierszy w paczce
                # - Wiersz, w którym warunek został spełniony
                if (j < start_index + 10) or (j % 500 == 0) or (j > len(timestamps) - 10) or condition_met:
                    logging.debug(f"{j:<12d} | {str(pd.to_datetime(current_original_ts)):<28} | {str(pd.to_datetime(current_original_ts))} > {str(pd.to_datetime(last_good_ts))} | {condition_met}")

                if condition_met:
                    end_index = j
                    logging.debug("-" * 80)
                    logging.debug(f"Zakończono skanowanie. Koniec bloku znaleziony przy indeksie: {end_index}")
                    logging.debug(f"{'='*20} ZAKOŃCZENIE DIAGNOZY BLOKU BŁĘDÓW {'='*20}\n")
                    break
            
            if end_index == -1:
                logging.debug("Skanowanie zakończone. Nie znaleziono końca bloku w tej paczce.")
            
            # Zakończ diagnostykę po pierwszym znalezionym bloku, aby log nie był zbyt duży
            logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY ---")
            return df 
        else:
            i += 1
            
    logging.debug("Nie znaleziono żadnych błędów chronologii w tej paczce.")
    logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY ---")
    return df

def diagnose_chronology(df: pd.DataFrame, context_name: str, known_interval: str, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    WERSJA DIAGNOSTYCZNA: Nie modyfikuje danych. Generuje szczegółowy log
    do analizy działania algorytmu korekty na problematycznym zbiorze danych.
    """
    logging.info(f"--- URUCHAMIANIE TRYBU DIAGNOSTYCZNEGO DLA CHRONOLOGII: {context_name} ---")
    if df.empty or timestamp_col not in df.columns:
        return df

    try:
        interval_td = pd.to_timedelta(known_interval)
    except ValueError:
        logging.error(f"Nieprawidłowy format interwału '{known_interval}'.")
        return df

    df_corrected = df.copy()
    df_corrected.reset_index(drop=True, inplace=True)

    original_timestamps = df_corrected[timestamp_col].to_numpy()
    # Tablica 'corrected' będzie symulować, jak wyglądałaby korekta
    corrected_timestamps = original_timestamps.copy()
    
    is_in_correction_mode = False
    
    for i in range(1, len(corrected_timestamps)):
        last_corrected_ts = pd.to_datetime(corrected_timestamps[i-1])
        current_original_ts = pd.to_datetime(original_timestamps[i])
        previous_original_ts = pd.to_datetime(original_timestamps[i-1])

        log_prefix = f"Wiersz {i:05d}:"
        
        if is_in_correction_mode:
            expected_next_ts = last_corrected_ts + interval_td
            
            # Sprawdzanie warunków wyjścia
            is_resynced = abs((current_original_ts - last_corrected_ts) - interval_td) < pd.Timedelta('1s')
            is_sequence_broken = abs((current_original_ts - previous_original_ts) - interval_td) > pd.Timedelta('1s')
            
            decision = ""
            if is_resynced or is_sequence_broken:
                is_in_correction_mode = False
                corrected_timestamps[i] = current_original_ts
                reason = "resynchronizacja" if is_resynced else "przerwana sekwencja"
                decision = f"DECYZJA: WYJŚCIE Z KOREKTY (powód: {reason}). Używam oryginału: {current_original_ts}"
            else:
                corrected_timestamps[i] = expected_next_ts
                decision = f"DECYZJA: KONTYNUACJA KOREKTY. Nowy czas: {pd.to_datetime(corrected_timestamps[i])}"

            logging.debug(f"{log_prefix} W TRYBIE KOREKTY. "
                          f"Poprzedni (skorygowany): {last_corrected_ts}, "
                          f"Oryginalny: {current_original_ts}. "
                          f"Warunki wyjścia -> Resync: {is_resynced}, PrzerwanaSekwencja: {is_sequence_broken}. {decision}")

        elif current_original_ts <= last_corrected_ts:
            is_in_correction_mode = True
            corrected_timestamps[i] = last_corrected_ts + interval_td
            logging.debug(f"{log_prefix} NORMALNY TRYB. "
                          f"Poprzedni: {last_corrected_ts}, "
                          f"Oryginalny: {current_original_ts}. "
                          f"DECYZJA: WEJŚCIE W TRYB KOREKTY. Nowy czas: {pd.to_datetime(corrected_timestamps[i])}")
        else:
            # W tym wierszu wszystko jest w porządku
            pass
            
    logging.info(f"--- ZAKOŃCZONO TRYB DIAGNOSTYCZNY DLA CHRONOLOGII ---")
    # Zwróć oryginalną, niezmodyfikowaną ramkę danych
    return df
    
def apply_column_mapping(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    # rename columns based on mapping rules
    file_id = config.get('file_id')
    if not file_id: return df
    ruleset_name = STATION_MAPPING_FOR_COLUMNS.get(file_id)
    if not ruleset_name: return df
    mapping_dict = COLUMN_MAPPING_RULES.get(ruleset_name)
    if not mapping_dict: return df
    df.rename(columns=mapping_dict, inplace=True)
    return df

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Removes sync-conflict suffixes from column names."""
    conflict_pattern = re.compile(r'\.sync-conflict-.*$')
    rename_map = {col: conflict_pattern.sub('', col) for col in df.columns if isinstance(col, str) and conflict_pattern.search(col)}
    if rename_map: df.rename(columns=rename_map, inplace=True)
    return df

def _sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanityzuje nazwy kolumn. Usuwa znaki specjalne. Jeśli w wyniku sanityzacji
    powstanie zduplikowana nazwa, do nowej nazwy dodawany jest sufiks '_rm'.
    """
    final_rename_map = {}
    # Zbiór do śledzenia już przetworzonych/stworzonych nazw, aby wykrywać kolizje
    seen_new_names = set()

    for col in df.columns:
        if col == 'TIMESTAMP':
            seen_new_names.add(col)
            continue
        
        # Usuń każdy znak, który nie jest literą, cyfrą ani podkreślnikiem
        new_col = re.sub(r'[^a-zA-Z0-9_]', '', col)
        
        # Sprawdź, czy oczyszczona nazwa już istnieje
        if new_col in seen_new_names:
            unique_col = f"{new_col}_rm"
            # W mało prawdopodobnym przypadku, gdy 'kolumna_rm' też już istnieje, dodaj kolejny sufiks
            while unique_col in seen_new_names:
                unique_col += "_rm"
            
            final_rename_map[col] = unique_col
            seen_new_names.add(unique_col)
        else:
            # Jeśli nazwa nie jest duplikatem, użyj jej
            if new_col != col:
                final_rename_map[col] = new_col
            seen_new_names.add(new_col)
            
    if final_rename_map:
        logging.debug(f"Sanityzacja nazw kolumn z obsługą duplikatów: {final_rename_map}")
        df.rename(columns=final_rename_map, inplace=True)
        
    return df
    
def _enforce_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wymusza konwersję kolumn na typ numeryczny, ze specjalnym traktowaniem kolumn flag.
    Wersja 7.13: Dodano agresywną konwersję kolumn '_flag' na typ integer.
    """
    cols_to_skip = ['TIMESTAMP', 'group_id', 'source_file', 'interval', 'TZ', '5M METAR Tab.4678', 
    '1M METAR Tab.4678', '5MMETARTab4678', '1MMETARTab4678', 'source_filename', 
    'source_filepath', 'http_header' , 'http_post_response', 'http_post_tx', 'file_handle', 
    'OSSignature', 'OSDate', 'OSVersion', 'ProgName', 'RevBoard']

    for col in df.columns:
        if col in cols_to_skip:
            continue

        # --- POCZĄTEK NOWEJ LOGIKI ---
        # Specjalne, agresywne traktowanie kolumn z flagami
        if col.endswith('_flag'):
            # Krok 1: Konwertuj na liczbę (błędy zamień na NaN).
            # Krok 2: Wypełnij ewentualne braki (NaN) wartością 0 (dane dobre).
            # Krok 3: Rzutuj na typ integer.
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            continue  # Przejdź do następnej kolumny
        # --- KONIEC NOWEJ LOGIKI ---

        # Standardowa obsługa pozostałych kolumn z danymi
        if not pd.api.types.is_numeric_dtype(df[col]):
            original_dtype = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if df[col].dtype != original_dtype:
                logging.debug(f"Konwersja kolumny '{col}' z typu {original_dtype} na {df[col].dtype}.")
    
    return df
    
def apply_manual_overrides(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Overwrites values in specified columns and time ranges based on config.
    Operates on naive timestamps.
    """
    group_id = config.get('file_id')
    if not group_id:
        return df

    # Find the correct ruleset for the given group_id
    ruleset_name = STATION_MAPPING_FOR_OVERRIDES.get(group_id)
    if not ruleset_name:
        return df

    station_rules = MANUAL_VALUE_OVERRIDES.get(ruleset_name)
    if not station_rules:
        return df

    df_out = df.copy()

    for col_name, rules_list in station_rules.items():
        if col_name not in df_out.columns:
            logging.warning(f"Nadpisywanie wartości: Kolumna '{col_name}' nie istnieje w danych dla grupy '{group_id}'.")
            continue

        for rule in rules_list:
            try:
                start_ts = pd.to_datetime(rule['start'])
                end_ts = pd.to_datetime(rule['end'])
                new_value = rule['new_value']
                reason = rule.get('reason', 'Brak powodu.')

                mask = (df_out['TIMESTAMP'] >= start_ts) & (df_out['TIMESTAMP'] <= end_ts)

                if mask.any():
                    df_out.loc[mask, col_name] = new_value
                    logging.info(f"Nadpisano {mask.sum()} wartości w kolumnie '{col_name}' na '{new_value}'. Powód: {reason}")

            except Exception as e:
                logging.error(f"Błąd podczas stosowania reguły nadpisywania dla '{col_name}': {e}")

    return df_out

def _ensure_flag_columns_exist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Zapewnia, że dla każdej numerycznej kolumny danych istnieje odpowiednia kolumna '_flag'.
    Wersja 7.76 FINAL: Implementuje nową logikę flag domyślnych (99 dla NaN, 0 dla wartości).
    """
    df = df.copy() # Defragmentacja na wejściu

    cols_to_skip = [
        'TIMESTAMP', 'group_id', 'source_file', 'interval', 
        'latitude', 'longitude', 'source_filepath', 
        'source_filename', 'original_row_index', 'RECORD'
    ]
    
    # Słownik do przechowania danych dla nowych kolumn flag
    new_cols_data = {}
    
    # Iteruj po wszystkich kolumnach, aby znaleźć te, które potrzebują flag
    for col_name in df.columns:
        # Pomiń metadane i kolumny, które już są flagami
        if col_name in cols_to_skip or col_name.endswith('_flag'):
            continue
        
        # Sprawdź, czy to kolumna numeryczna (tylko takie flagujemy)
        if not pd.api.types.is_numeric_dtype(df[col_name]):
            continue

        flag_name = f"{col_name}_flag"
        # Stwórz nową flagę tylko, jeśli jeszcze nie istnieje
        if flag_name not in df.columns:
            # Użyj np.where do wektorowego stworzenia nowej kolumny:
            # Gdzie wartość w col_name istnieje -> flaga = 0
            # Gdzie wartość w col_name jest NaN -> flaga = 99
            new_cols_data[flag_name] = np.where(df[col_name].notna(), 0, 99)

    # Jeśli znaleziono brakujące kolumny, dodaj je wszystkie naraz
    if new_cols_data:
        logging.debug(f"Tworzenie {len(new_cols_data)} brakujących kolumn flag z logiką 99/0.")
        
        # Stwórz nową ramkę danych z nowymi kolumnami i poprawnym typem
        new_flags_df = pd.DataFrame(new_cols_data, index=df.index).astype('int8')
        
        # Połącz z oryginalną ramką
        df = pd.concat([df, new_flags_df], axis=1)
            
    return df
    
def _filter_future_timestamps(df: pd.DataFrame, timestamp_col: str = 'TIMESTAMP') -> pd.DataFrame:
    """
    Usuwa wiersze, których znacznik czasu jest w przyszłości.
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    # Pobierz aktualny czas
    now = pd.Timestamp.now()
    
    # Stwórz maskę logiczną dla wierszy z przyszłą datą
    future_mask = df[timestamp_col] > now

    # Jeśli znaleziono jakiekolwiek przyszłe daty, odfiltruj je i zaraportuj
    if future_mask.any():
        num_removed = future_mask.sum()
        logging.warning(
            f"Usunięto {num_removed} rekordów ze znacznikiem czasu z przyszłości (późniejszym niż {now.strftime('%Y-%m-%d %H:%M:%S')})."
        )
        return df[~future_mask]
    
    return df
    
# def _validate_csv_file(df: pd.DataFrame, file_path: Path, config: dict) -> bool:
    # """
    # Sprawdza, czy pojedynczy plik CSV jest spójny.
    # Wersja 7.78: Zastępuje walidację mediany interwału ścisłą kontrolą monotoniczności.
    # """
    # if df.empty or len(df) < 2:
        # return False

    # # 1. Walidacja nazwy pliku względem ostatniego znacznika czasu (bez zmian)
    # try:
        # # Wyodrębnij datę z nazwy pliku, np. 'pom1m_20250524T234500' -> '20250524'
        # filename_date_str = re.search(r'(\d{8}T\d{6})', file_path.stem).group(1)[:8]
        # filename_date = pd.to_datetime(filename_date_str, format='%Y%m%d').date()
        
        # last_timestamp_date = df['TIMESTAMP'].iloc[-1].date()

        # if filename_date != last_timestamp_date:
            # logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: data z nazwy ({filename_date}) "
                          # f"nie zgadza się z datą ostatniego rekordu ({last_timestamp_date}).")
            # return False
    # except (AttributeError, IndexError, TypeError):
        # # TypeError dodany na wypadek problemów z .date() przy błędnych danych
        # logging.debug(f"Nie można było zweryfikować daty w nazwie pliku '{file_path.name}'.")
        # return False

    # # --- POCZĄTEK NOWEJ LOGIKI ---
    # # 2. Walidacja monotoniczności (sprawdzenie, czy czas się nie cofa)
    # # Atrybut .is_monotonic_increasing jest wysoce zoptymalizowany do tego zadania.
    # if not df['TIMESTAMP'].is_monotonic_increasing:
        # logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: wykryto cofnięcie czasu wewnątrz pliku.")
        # return False
    # # --- KONIEC NOWEJ LOGIKI ---

    # logging.info(f"Plik '{file_path.name}' pomyślnie przeszedł walidację.")
    # return True
    
def _validate_csv_file(df: pd.DataFrame, file_path: Path, config: dict) -> bool:
    """
    Sprawdza, czy pojedynczy plik CSV jest spójny.
    Wersja 7.79: Walidacja oparta na dacie modyfikacji pliku, a nie jego nazwie.
    """
    if df.empty or len(df) < 2:
        return False

    # --- POCZĄTEK ZMIANY ---
    # 1. Walidacja daty ostatniej modyfikacji pliku względem ostatniego znacznika czasu
    try:
        # Pobierz datę ostatniej modyfikacji pliku z systemu plików
        mod_time_unix = file_path.stat().st_mtime
        modification_date = pd.to_datetime(mod_time_unix, unit='s').date()
        
        # Pobierz datę ostatniego rekordu w pliku
        last_timestamp_date = df['TIMESTAMP'].iloc[-1].date()

        # Porównaj obie daty
        if modification_date != last_timestamp_date:
            logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: data modyfikacji ({modification_date}) "
                          f"nie zgadza się z datą ostatniego rekordu ({last_timestamp_date}).")
            return False
    except (IndexError, TypeError):
        logging.debug(f"Nie można było zweryfikować daty ostatniej modyfikacji dla pliku '{file_path.name}'.")
        return False
    # --- KONIEC ZMIANY ---

    # 2. Walidacja monotoniczności (bez zmian)
    if not df['TIMESTAMP'].is_monotonic_increasing:
        logging.debug(f"Plik '{file_path.name}' nie przeszedł walidacji: wykryto cofnięcie czasu wewnątrz pliku.")
        return False

    logging.info(f"Plik '{file_path.name}' pomyślnie przeszedł walidację.")
    return True    
    
# --- GŁÓWNE FUNKCJE PRZETWARZANIA ---

def scan_for_files(input_dirs: List[str], source_ids: List[str]) -> List[Path]:
    """Scans directories for matching files, ignoring sync-conflict files."""
    all_file_paths = []
    for input_dir in input_dirs:
        p_input = Path(input_dir)
        if not p_input.is_dir(): continue
        for p_file in p_input.rglob('*'):
            if "sync-conflict" in p_file.name: continue
            if "CONFIG" in p_file.name: continue
            if "tmp" in p_file.name: continue
            if "checkpoint" in p_file.name: continue
            if "pom1m_20210629T234501" in p_file.name: continue
            if "pom1m_20230614T234500" in p_file.name: continue
            if "pom1m_20210813T234500" in p_file.name: continue
            if "pom1m_20210822T234501" in p_file.name: continue
            if p_file.is_file():
                if any((sid.endswith('$') and p_file.stem.endswith(sid.rstrip('$'))) or (sid in p_file.name) for sid in source_ids):
                    all_file_paths.append(p_file.resolve())
    return sorted(list(set(all_file_paths)))

def identify_file_type(file_path: Path) -> str:
    """Quickly identifies file type from its first line."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
        if first_line.startswith('"TOB1"'): return 'TOB1'
        if first_line.startswith('"TOA5"'): return 'TOA5'
        if '"Timestamp"' in first_line: return 'CSV' # More robust check for CSV
        return 'UNKNOWN'
    except Exception:
        return 'UNKNOWN'

def process_binary_file(args: tuple) -> Optional[pd.DataFrame]:
    """Processing pipeline for a single binary file (TOB1/TOA5)."""
    file_path, config = args
    try:
        df = pd.DataFrame()
        file_type = identify_file_type(file_path)
        if file_type == 'TOB1':
            metadata = get_tob1_metadata(file_path)
            if metadata: df = read_tob1_data(file_path, metadata)
        elif file_type == 'TOA5':
            metadata = get_toa5_metadata(file_path)
            if metadata: df = read_toa5_data(file_path, metadata)
        else:
            return None

        if df.empty: return None

        # Apply only the specified filter for this pipeline
        df = filter_by_realistic_date_range(df, file_path)
        return df
    except Exception as e:
        logging.error(f"Krytyczny błąd (plik binarny) {file_path.name}: {e}", exc_info=True)
        return None

def process_and_save_data(raw_dfs: List[pd.DataFrame], config: dict, lock: multiprocessing.Lock):
    """
    Final, unified processing pipeline.
    Wersja 7.25: Optymalizuje moment defragmentacji, aby wyciszyć ostrzeżenia o wydajności.
    """
    if not raw_dfs:
        logging.info("Brak danych do przetworzenia.")
        return
    
    group_id = config['file_id']
    data_by_year = defaultdict(list)
    for df in raw_dfs:
        if not df.empty and 'TIMESTAMP' in df.columns:
            df.dropna(subset=['TIMESTAMP'], inplace=True)
            for year, year_group in df.groupby(df['TIMESTAMP'].dt.year):
                data_by_year[year].append(year_group)

    logging.info(f"Rozpoczynanie finalnego przetwarzania i zapisu dla {len(data_by_year)} lat...")

    for year, dfs_for_year in sorted(data_by_year.items()):
        try:
            logging.info(f"--- Przetwarzanie roku: {int(year)} | Grupa: {group_id} ---")

            logger_data_raw = pd.concat(dfs_for_year, ignore_index=True)
            if logger_data_raw.empty:
                continue
            
            logger_data_df = logger_data_raw.groupby('TIMESTAMP').first()

            if group_id in GROUP_IDS_FOR_MATLAB_FILL:
                matlab_df = load_matlab_data(int(year), config)
                if not matlab_df.empty:
                    # --- POCZĄTEK KLUCZOWEJ ZMIANY ---
                    # Krok 1: Scal duplikaty w danych z MAT, zanim cokolwiek innego z nimi zrobisz.
                    if matlab_df.columns.duplicated().any():
                        logging.info(f"Scalanie zduplikowanych kolumn w danych .MAT dla roku {int(year)}...")
                        matlab_df = matlab_df.T.groupby(level=0).first().T
                        # Upewnij się, że TIMESTAMP jest poprawnym typem po transpozycji
                        if 'TIMESTAMP' in matlab_df.columns:
                             matlab_df['TIMESTAMP'] = pd.to_datetime(matlab_df['TIMESTAMP'], errors='coerce')

                    # Krok 2: Ustaw indeks i dopiero teraz bezpiecznie połącz z danymi z loggera
                    matlab_df.set_index('TIMESTAMP', inplace=True)
                    combined_df = logger_data_df.combine_first(matlab_df).reset_index()
                    # --- KONIEC KLUCZOWEJ ZMIANY ---
                    logging.info(f"--- Uzupełniono/zastąpiono dane z .MAT dla roku: {int(year)} ---")
                else:
                    combined_df = logger_data_df.reset_index()
            else:
                combined_df = logger_data_df.reset_index()

            if combined_df.empty:
                continue

            # Krok A: Zastosuj mapowanie nazw. Może to utworzyć duplikaty.
            mapped_df = apply_column_mapping(combined_df, config)

            # Krok B: Centralne i jedyne miejsce scalania duplikatów.
            if mapped_df.columns.duplicated().any():
                logging.info(f"Scalanie zduplikowanych kolumn po mapowaniu dla roku {int(year)}...")
                mapped_df = mapped_df.T.groupby(level=0).first().T
                
                # Przywróć TIMESTAMP i typy danych
                if 'TIMESTAMP' in mapped_df.columns:
                    mapped_df['TIMESTAMP'] = pd.to_datetime(mapped_df['TIMESTAMP'], errors='coerce')
                else:
                    mapped_df.reset_index(inplace=True)
                    mapped_df.rename(columns={'index': 'TIMESTAMP'}, inplace=True)
                    mapped_df['TIMESTAMP'] = pd.to_datetime(mapped_df['TIMESTAMP'], errors='coerce')

                mapped_df.dropna(subset=['TIMESTAMP'], inplace=True)
                mapped_df = _enforce_numeric_types(mapped_df)
                logging.info("Scalanie zakończone.")
            
            # Krok C: Oczyszczenie nazw kolumn.
            mapped_df = _sanitize_column_names(mapped_df)
            if mapped_df.empty: continue

            # Teraz ramka jest gotowa do dalszych operacji
            corrected_df = mapped_df.copy()

            corrected_df['TIMESTAMP'] = apply_timezone_correction(corrected_df['TIMESTAMP'], config['file_id'])
            corrected_df.dropna(subset=['TIMESTAMP'], inplace=True)
            if corrected_df.empty: continue

            corrected_df = apply_manual_time_shifts(corrected_df, config['file_id'])
            corrected_df = apply_calibration(corrected_df, config['file_id'])
            corrected_df = apply_value_range_flags(corrected_df)
            corrected_df = apply_quality_flags(corrected_df, config)
            corrected_df = apply_manual_overrides(corrected_df, config)
            corrected_df = align_timestamp(corrected_df, config.get('interval'))
            # corrected_df = _enforce_numeric_types(corrected_df)
            corrected_df = _ensure_flag_columns_exist(corrected_df)
            corrected_df = corrected_df.copy()
            corrected_df = _filter_future_timestamps(corrected_df)
            # corrected_df.drop_duplicates(subset=['TIMESTAMP'], keep='last', inplace=True)
            # corrected_df.reset_index(drop=True, inplace=True)

            output_format = config.get('output_format', 'sqlite')
            if output_format in ['sqlite', 'both']:
                save_dataframe_to_sqlite(corrected_df, config, lock)
            if output_format in ['csv', 'both']:
                save_dataframe_to_csv(corrected_df, int(year), config, lock)

        except Exception as e:
            logging.error(f"Krytyczny błąd podczas finalnego przetwarzania roku {int(year)}: {e}", exc_info=True)
            
# --- FUNKCJA TESTUJĄCA ---
def _run_tests(config: dict):
    """
    Uruchamia zautomatyzowany test, używając logiki przygotowania danych
    identycznej jak w funkcji main() i poprawnie zamykając połączenia z bazą.
    Wersja 7.82 FINAL: Pełna spójność logiki i poprawne zarządzanie zasobami.
    """
    logging.info("="*20 + " URUCHAMIANIE TRYBU TESTOWEGO " + "="*20)
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        test_db_path = temp_dir / "test_db.sqlite"
        test_csv_dir = temp_dir / "test_csv_output"
        test_csv_dir.mkdir()

        test_config = config.copy()
        test_config['db_path'] = str(test_db_path)
        test_config['output_dir'] = str(test_csv_dir)
        test_config['output_format'] = 'both'

        # --- Uruchomienie głównej logiki skryptu ---
        all_files = scan_for_files(test_config['input_dir'], test_config.get('source_ids', []))
        if not all_files:
            logging.warning("Test pominięty: nie znaleziono plików.")
            return
        
        binary_files = [p for p in all_files if identify_file_type(p) in ['TOB1', 'TOA5']]
        csv_files = [p for p in all_files if identify_file_type(p) == 'CSV']
        
        all_raw_results = []
        
        if binary_files:
            # Ta część pozostaje uproszczona, zakładając, że główny problem leży w CSV
            binary_args = [(p, test_config) for p in binary_files]
            with multiprocessing.Pool(processes=test_config['jobs']) as pool:
                binary_results = list(pool.imap_unordered(process_binary_file, binary_args))
            all_raw_results.extend([df for df in binary_results if df is not None and not df.empty])
        
        if csv_files:
            # Użyj DOKŁADNIE tej samej logiki deduplikacji, co w main()
            csv_files.sort(key=lambda p: p.stat().st_mtime)
            all_csv_dfs = [read_simple_csv_data(p) for p in csv_files]
            non_empty_dfs = [df for df in all_csv_dfs if df is not None and not df.empty]
            if non_empty_dfs:
                batch_df = pd.concat(non_empty_dfs, ignore_index=True)
                
                if 'TIMESTAMP' in batch_df.columns:
                    initial_rows = len(batch_df)
                    metadata_cols = ['TIMESTAMP', 'source_filename', 'original_row_index', 'source_filepath']
                    cols_to_check = [col for col in batch_df.columns if col not in metadata_cols]
                    
                    if cols_to_check:
                        df_for_dedup = batch_df.copy()
                        numeric_cols_to_round = [col for col in df_for_dedup.select_dtypes(include=np.number).columns if col not in metadata_cols]
                        df_for_dedup[numeric_cols_to_round] = df_for_dedup[numeric_cols_to_round].round(4)
                        indices_to_keep = df_for_dedup.drop_duplicates(subset=cols_to_check, keep='first').index
                        batch_df = batch_df.loc[indices_to_keep]
                    
                    rows_removed = initial_rows - len(batch_df)
                    if rows_removed > 0:
                        logging.info(f"[Test] Usunięto {rows_removed} zduplikowanych wierszy.")

                known_interval = test_config.get('interval')
                corrected_batch = correct_and_report_chronology(batch_df, f"Testowe pliki CSV", known_interval)
                all_raw_results.append(corrected_batch)

        if not all_raw_results:
            logging.error("[Test] Nie udało się przetworzyć żadnych danych.")
            return

        lock = multiprocessing.Manager().Lock()
        initialize_database(Path(test_config['db_path']))
        process_and_save_data(all_raw_results, test_config, lock)

        logging.info("[Test] Porównywanie wyników z bazy SQLite i plików CSV...")

        engine = sqlalchemy.create_engine(f"sqlite:///{test_db_path}")
        table_name = f"data_{test_config['file_id']}"
        db_df = pd.DataFrame() # Zapewnij istnienie zmiennej
        try:
            db_df = pd.read_sql_table(table_name, engine, parse_dates=['TIMESTAMP'])
        except Exception as e:
            logging.error(f"[Test FAILED] Nie można odczytać danych z bazy: {e}")
        finally:
            # --- KLUCZOWA ZMIANA: Zawsze zamykaj połączenie z bazą ---
            engine.dispose()

        if db_df.empty:
            logging.error("[Test FAILED] Ramka danych z bazy jest pusta.")
            return

        csv_dfs = [pd.read_csv(p, parse_dates=['TIMESTAMP'], low_memory=False) for p in test_csv_dir.glob("**/*.csv")]
        if not csv_dfs:
            logging.error("[Test FAILED] Nie wygenerowano żadnych plików CSV.")
            return
        csv_df = pd.concat(csv_dfs, ignore_index=True)

        cols_to_drop_from_csv = ['group_id', 'interval', 'latitude', 'longitude', 'source_filepath', 'source_filename', 'original_row_index']
        csv_df.drop(columns=[c for c in cols_to_drop_from_csv if c in csv_df.columns], inplace=True, errors='ignore')
        
        shared_cols = sorted(list(set(db_df.columns) & set(csv_df.columns)))
        if not shared_cols or 'TIMESTAMP' not in shared_cols:
             logging.error("[Test FAILED] Brak wspólnych kolumn do porównania.")
             return

        db_df = db_df[shared_cols].sort_values(by='TIMESTAMP').reset_index(drop=True)
        csv_df = csv_df[shared_cols].sort_values(by='TIMESTAMP').reset_index(drop=True)

        for col in db_df.columns:
            if col != 'TIMESTAMP':
                try:
                    db_df[col] = pd.to_numeric(db_df[col])
                    csv_df[col] = pd.to_numeric(csv_df[col])
                except (ValueError, TypeError):
                    pass 

        try:
            pd.testing.assert_frame_equal(db_df, csv_df, check_dtype=False, atol=1e-5)
            logging.info("✅ [Test PASSED] Wyniki z bazy SQLite i plików CSV są identyczne.")
        except AssertionError as e:
            logging.error("❌ [Test FAILED] Wykryto rozbieżności między wynikami z bazy SQLite i plików CSV.")
            logging.error(f"Szczegóły błędu:\n{e}")

    logging.info("="*20 + " ZAKOŃCZONO TRYB TESTOWY " + "="*20)

import pandas as pd
import logging
from pathlib import Path

def is_approx_24h_monotonic(filename, col='Timestamp', tolerance_hours=1):
    """
    Sprawdza, czy zakres serii czasowej to ok. 24h (+/- tolerancja)
    oraz czy czas jest monotonnie rosnący (nie cofa się).
    Jeśli czas w pliku się cofnął – loguje błąd i zwraca False.
    """
    try:
        times = pd.read_csv(filename, usecols=[col])[col]
        times = pd.to_datetime(times, errors='coerce')
        times = times.dropna()
        if len(times) < 2:
            return False
        # # Sprawdź monotoniczność
        # if not times.is_monotonic_increasing:
            # logging.warning(f"W pliku {filename} czas się cofa – plik pominięty.")
            # return False
        # Policz zakres czasowy w godzinach
        dt_hours = (times.max() - times.min()).total_seconds() / 3600
        return abs(dt_hours - 24) <= tolerance_hours
    except Exception as e:
        logging.error(f"Nie udało się sprawdzić {filename}: {e}")
        return False

import pandas as pd
import logging
from pathlib import Path

def is_over_24h_monotonic(filename, col='Timestamp', min_hours=25):
    """
    Zwraca True, jeśli czas w pliku jest monotonnie rosnący
    i zakres czasowy przekracza min_hours (domyślnie 24h).
    Jeśli czas się cofa – loguje ostrzeżenie i zwraca False.
    """
    try:
        times = pd.read_csv(filename, usecols=[col])[col]
        times = pd.to_datetime(times, errors='coerce')
        times = times.dropna()
        if len(times) < 2:
            return False
        # # Sprawdź monotoniczność czasów
        # if not times.is_monotonic_increasing:
            # logging.warning(f"W pliku {filename} czas się cofa – plik pominięty.")
            # return False
        # Zakres czasowy w godzinach
        dt_hours = (times.max() - times.min()).total_seconds() / 3600
        return dt_hours > min_hours
    except Exception as e:
        logging.error(f"Nie udało się sprawdzić {filename}: {e}")
        return False


def main():
    """Główna funkcja orkiestrująca."""
    parser = argparse.ArgumentParser(description="Uniwersalny skrypt do przetwarzania danych pomiarowych.")
    parser.add_argument("-i", "--input_dir", required=True, nargs='+', help="Katalogi wejściowe.")
    parser.add_argument("-o", "--output_dir", required=True, help="Katalog wyjściowy (dla CSV, logów, cache).")
    parser.add_argument("-fid", "--file_id", required=True, help="Identyfikator grupy do przetworzenia.")
    parser.add_argument("--db-path", help="Ścieżka do pliku bazy danych SQLite.")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1, help="Liczba procesów.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Poziom logowania.")
    parser.add_argument("--output-format", default="sqlite", choices=["sqlite", "csv", "both"], help="Format wyjściowy.")
    parser.add_argument("--no-cache", action='store_true', help="Wyłącza użycie cache.")
    parser.add_argument("--run-tests", action='store_true', help="Uruchamia tryb testowy.")
    parser.add_argument("--overwrite", action='store_true', help="Wymusza ponowne przetworzenie i nadpisanie istniejących danych w plikach CSV.")
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    setup_chronology_logger()
    
    # --- Configuration validation ---
    if args.output_format in ['sqlite', 'both'] and not args.db_path:
        parser.error("--db-path jest wymagany dla formatu 'sqlite' lub 'both'.")
    if args.run_tests and not args.db_path:
         parser.error("--db-path jest wymagany do uruchomienia testów.")

    group_config = FILE_ID_MERGE_GROUPS.get(args.file_id, {})
    if not group_config:
        logging.error(f"Nie znaleziono konfiguracji dla grupy '{args.file_id}'. Przerwanie pracy.")
        return

    group_config.update({
        'file_id': args.file_id,
        'output_dir': args.output_dir,
        'db_path': args.db_path,
        'input_dir': args.input_dir,
        'main_input_path': args.input_dir[0] if args.input_dir else None,
        'output_format': args.output_format,
        'jobs': args.jobs,
        'overwrite': args.overwrite
    })

    # Overwrite implies no-cache
    use_cache = not (args.no_cache or args.overwrite)
    processed_files_cache = load_cache() if use_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]
    
    if args.run_tests:
        _run_tests(group_config)
        return

    logging.info(f"{'='*20} Rozpoczęto przetwarzanie dla grupy: '{args.file_id}' {'='*20}")
    
    # Initialize DB if needed
    if args.output_format in ['sqlite', 'both']:
        initialize_database(Path(args.db_path))

    processed_files_cache = load_cache() if not args.no_cache else {}
    all_files = scan_for_files(args.input_dir, group_config.get('source_ids', []))
    files_to_process = [p for p in all_files if not is_file_in_cache(p, processed_files_cache)]

    if not files_to_process:
        logging.info("Brak nowych lub zmodyfikowanych plików do przetworzenia.")
        return
    logging.info(f"Znaleziono {len(files_to_process)} nowych/zmienionych plików.")

    binary_files = [p for p in files_to_process if identify_file_type(p) in ['TOB1', 'TOA5']]
    csv_files = [p for p in files_to_process if identify_file_type(p) == 'CSV']
    
    all_raw_results = []
    
    # Pipeline 1: Process binary files in parallel
    if binary_files:
        logging.info(f"Przetwarzanie {len(binary_files)} plików binarnych (TOB1/TOA5)...")
        binary_args = [(p, group_config) for p in binary_files]
        with multiprocessing.Pool(processes=args.jobs) as pool:
            binary_results = list(tqdm(pool.imap_unordered(process_binary_file, binary_args), total=len(binary_files), desc="Pliki binarne"))
        all_raw_results.extend([df for df in binary_results if df is not None and not df.empty])

    # Pipeline 2: Process ALL CSV files at once, sorted by modification time
    if csv_files:
        # # --- DEBUG: Zapisz listę plików PRZED sortowaniem ---
        # try:
            # debug_before_path = LOGS_DIR / "debug_files_before_sort.txt"
            # logging.info(f"DEBUG: Zapisywanie listy plików PRZED sortowaniem do: {debug_before_path.name}")
            # with open(debug_before_path, 'w', encoding='utf-8') as f:
                # for p in csv_files:
                    # f.write(f"{p.name}\n")
        # except Exception as e:
            # logging.error(f"DEBUG: Nie udało się zapisać pliku listy przed sortowaniem: {e}")
        # --- KONIEC DEBUG ---
        unique_files = []
        seen_names = set() # A set for fast lookups of filenames we've already added.

        for p in csv_files:
            # Check if a file with this name has already been seen.
            if p.name not in seen_names:
                # If not, add the full path object to our unique list.
                unique_files.append(p)
                # And record the name so we can ignore its future duplicates.
                seen_names.add(p.name)
                
        # # --- DEBUG: filtruj dane po latach wg nazwy plików
        # unique_files = [
            # p for p in unique_files if p.name.startswith('pom1m_2018')
        # ]
        # # --- KONIEC DEBUG ---
        
        # KROK KRYTYCZNY: 
        group_id = args.file_id
        parts = group_id.split('_', 1)
        station_code, _folder_name = parts
        if 'ME' not in station_code:
            # Wersja 1: (Sortuj pliki CSV według czasu ich modyfikacji)
            logging.info(f"Sortowanie {len(csv_files)} plików CSV według czasu modyfikacji...")
            unique_files.sort(key=lambda p: p.stat().st_mtime)
        else:
            # Wersja 2: (sortowanie alfabetyczne po nazwie pliku)
            logging.info(f"Sortowanie {len(unique_files)} plików CSV alfabetycznie (po samej nazwie, bez ścieżki, case-insensitive)...")
            unique_files.sort(key=lambda p: int(re.sub(r'[^0-9]', '', p.name)))
                
        # --- FILTRACJA: usuń pliki puste # oraz większe niż 0.4 MB --- (opcjonalne)
        filtered_files = []
        for p in unique_files:
            try:
                st = p.stat()
                if 0 < st.st_size: # <= int(0.4 * 1024 * 1024):  # od 1 bajta do 0.4 MB
                    filtered_files.append(p)
            except Exception:
                # jeżeli nie udało się odczytać metadanych, pomijamy plik
                continue
        
        # --- DEBUG: Zapisz listę plików PO sortowaniu ---
        try:
            debug_after_path = LOGS_DIR / f"debug_files_after_sort_{args.file_id}.txt"
            logging.info(f"DEBUG: Zapisywanie listy plików PO sortowaniu do: {debug_after_path.name}")
            with open(debug_after_path, 'w', encoding='utf-8') as f:
                f.write("filename;fullpath;modified_utc;size_mb\n")
                for p in unique_files: # tu zapisuje wszystkie pliki do przerobienia
                    try:
                        st = p.stat()
                        modified_utc = datetime.fromtimestamp(st.st_mtime, UTC).strftime('%Y-%m-%d %H:%M:%S')
                        size_mb = round(st.st_size / (1024*1024), 3)
                    except Exception:
                        modified_utc = "N/A"
                        size_mb = "N/A"
                    f.write(f"{p.name};{str(p.resolve())};{modified_utc};{size_mb}\n")
        except Exception as e:
            logging.error(f"DEBUG: Nie udało się zapisać pliku listy po sortowaniu: {e}")
        # --- KONIEC DEBUG ---
        
        logging.info(f"Wczytywanie wszystkich {len(unique_files)} posortowanych plików CSV do pamięci...")

        def process_files_group(file_list, group_name, group_config, all_raw_results):
            logging.info(f"Przetwarzanie grupy: {group_name}, liczba plików: {len(file_list)}")
            
            # Wczytanie plików (serialne)
            all_csv_dfs = [read_simple_csv_data(p) for p in tqdm(file_list, desc=f"Wczytywanie plików CSV - {group_name}")]
            non_empty_dfs = [df for df in all_csv_dfs if df is not None and not df.empty]
            # # --- DEBUG: Zapisz ramkę danych PRZED deduplikacją ---
            # try:
                # combined_df = pd.concat(non_empty_dfs, ignore_index=True)
                # debug_path_before_dedup = Path(group_config.get('output_dir')) / f"debug_before_deduplication_{group_config['file_id']}.csv"
                # debug_path_before_dedup.parent.mkdir(parents=True, exist_ok=True)
                # logging.info(f"DEBUG: Zapisywanie stanu danych PRZED deduplikacją do: {debug_path_before_dedup.name}")
                # combined_df.to_csv(debug_path_before_dedup, index=False)
                # # zapis tylko pierwszych 70k wierszy
                # # combined_df.head(70000).to_csv(debug_path_before_dedup, index=False)
            # except Exception as e:
                # logging.error(f"DEBUG: Nie udało się zapisać pliku PRZED deduplikacji: {e}")
            # # --- KONIEC DEBUG ---
            
            # Usuwa duplikaty
            if non_empty_dfs:
                batch_df = pd.concat(non_empty_dfs, ignore_index=True)

                if 'TIMESTAMP' in batch_df.columns:
                    initial_rows = len(batch_df)
                    metadata_cols = ['source_filename', 'original_row_index', 'source_filepath']  # ewentualnie modyfikuj
                    cols_to_check = [col for col in batch_df.columns if col not in metadata_cols]

                    if cols_to_check:
                        df_for_dedup = batch_df.copy()
                        indices_to_keep = df_for_dedup.drop_duplicates(subset=cols_to_check, keep='last').index # tu musi być 'last' bo pliki duże psują chronologię
                        batch_df = batch_df.loc[indices_to_keep]

                    rows_removed = initial_rows - len(batch_df)
                    if rows_removed > 0:
                        logging.info(f"Usunięto {rows_removed} zduplikowanych wierszy z grupy {group_name}.")

                known_interval = group_config.get('interval')
                corrected_batch = correct_and_report_chronology(batch_df, f"Grupa: {group_name}", known_interval)
                all_raw_results.append(corrected_batch)
            else:
                logging.warning(f"Brak niepustych DataFrame do przetworzenia w grupie {group_name}.")

        all_raw_results = []
        
        # # --- DEBUG: Przetwarzam pliki wg wielkości
        
        # Podziel pliki na dwie grupy
        # files_approx_24h = [p for p in unique_files if is_approx_24h_monotonic(p)]
        # files_over_24h = [p for p in unique_files if is_over_24h_monotonic(p)]

        # process_files_group(files_approx_24h, "około 24h danych", group_config, all_raw_results)
        # process_files_group(files_over_24h, "powyżej 24h danych", group_config, all_raw_results)
        # # --- KONIEC DEBUG ---
        process_files_group(unique_files, args.file_id, group_config, all_raw_results)
                
    # Final processing and saving
    if all_raw_results:
        lock = multiprocessing.Manager().Lock()
        process_and_save_data(all_raw_results, group_config, lock)

    if use_cache:
        update_cache(files_to_process, processed_files_cache)
        save_cache(processed_files_cache)
        logging.info("Cache został zaktualizowany.")

    logging.info(f"{'='*20} Zakończono przetwarzanie dla grupy: '{args.file_id}' {'='*20}\n")


if __name__ == '__main__':
    if os.name in ['nt', 'posix']:
        multiprocessing.set_start_method('spawn', force=True)
    main()