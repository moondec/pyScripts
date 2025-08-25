import pandas as pd
import os
import struct # Potrzebne do odczytu TOB1

# --- KONFIGURACJA TESTU ---
# Proszę podać pełne ścieżki do plików, które mają być przetestowane

FILE_PATH_1 = r"D:\sites\ME\dT_ME\ThermoFisher\20180903\DT85_down\pom1m_20180621T234500.CSV"
FILE_PATH_2 = r"D:\sites\ME\dT_ME\ThermoFisher\20180903\DT85_down\pom1m_20180620T234501.CSV"

# Podaj ścieżkę, gdzie ma być zapisany wynik testu
OUTPUT_PATH = r"E:\pdfy\Porownanie_CSV_vs_MATLAB\ME\ME_3\wynik_deduplikacji.csv"

# Ilość miejsc po przecinku do zaokrąglenia
PRECISION = 6

# --- ZESTAW MINIMALNYCH FUNKCJI POMOCNICZYCH ---
def parse_header_line(line): return [field.strip() for field in line.strip('"').split('","')]
def get_toa5_metadata(file_path):
    try:
        with open(file_path, 'r', encoding='latin-1') as f: header_lines = [f.readline().strip() for _ in range(4)]
        if not header_lines[0].startswith('"TOA5"'): return None
        return parse_header_line(header_lines[1]), 4
    except Exception: return None
def read_toa5_data(file_path, metadata):
    col_names, num_header_lines = metadata
    try:
        df = pd.read_csv(file_path, skiprows=num_header_lines, header=None, names=col_names, na_values=['"NAN"','NAN','"INF"','""',''], quotechar='"', escapechar='\\', on_bad_lines='warn', encoding='latin-1')
        if 'TIMESTAMP' not in df.columns: return pd.DataFrame()
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce'); df.dropna(subset=['TIMESTAMP'], inplace=True)
        return df
    except Exception: return pd.DataFrame()
def read_any_file(file_path):
    try:
        with open(file_path, 'r', encoding='latin-1') as f: first_line = f.readline().strip()
    except Exception: return pd.DataFrame()
    if first_line.startswith('"TOA5"'):
        metadata = get_toa5_metadata(file_path)
        if metadata: return read_toa5_data(file_path, metadata)
    elif first_line.startswith('"Timestamp"'):
        df = pd.read_csv(file_path, header=0, low_memory=False, encoding='latin-1')
        if 'Timestamp' in df.columns: df.rename(columns={'Timestamp':'TIMESTAMP'}, inplace=True)
        if 'TIMESTAMP' in df.columns: df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce'); df.dropna(subset=['TIMESTAMP'], inplace=True)
        return df
    else: print(f"BŁĄD: Ten skrypt diagnostyczny nie obsługuje plików TOB1."); return pd.DataFrame()

# --- GŁÓWNA LOGIKA DIAGNOSTYCZNA ---
print("="*30 + " START DIAGNOSTYKI " + "="*30)

# Krok 1: Wczytaj oba pliki
print(f"\n1. Wczytywanie pliku 1: {os.path.basename(FILE_PATH_1)}")
df1 = read_any_file(FILE_PATH_1)
if df1.empty: print("   -> BŁĄD: Nie udało się wczytać pliku 1 lub jest on pusty.")
else: print(f"   -> Wczytano {len(df1)} wierszy.")
print(f"\n2. Wczytywanie pliku 2: {os.path.basename(FILE_PATH_2)}")
df2 = read_any_file(FILE_PATH_2)
if df2.empty: print("   -> BŁĄD: Nie udało się wczytać pliku 2 lub jest on pusty.")
else: print(f"   -> Wczytano {len(df2)} wierszy.")
    
if df1.empty and df2.empty:
    print("\nOba pliki są puste. Zakończono.")
else:
    combined_df = pd.concat([df1, df2], ignore_index=True)
    initial_rows = len(combined_df)
    print(f"\n3. Połączono dane. Łączna liczba wierszy: {initial_rows}")

    # === NOWY KROK: Normalizacja danych zmiennoprzecinkowych (zaokrąglanie) ===
    print(f"\n4. Normalizacja danych: zaokrąglanie wartości float do {PRECISION} miejsc po przecinku...")
    float_columns = combined_df.select_dtypes(include='float64').columns
    if not float_columns.empty:
        print(f"   -> Znaleziono {len(float_columns)} kolumn float do zaokrąglenia.")
        combined_df[float_columns] = combined_df[float_columns].round(PRECISION)
    else:
        print("   -> Nie znaleziono kolumn float do zaokrąglenia.")
    # === KONIEC NOWEGO KROKU ===

    # Krok 5: Zastosuj logikę deduplikacji
    print("\n5. Zastosowanie dwustopniowej deduplikacji...")
    # a) Usuń wiersze, które są w 100% identyczne
    pre_dedup_rows = len(combined_df)
    combined_df.drop_duplicates(inplace=True)
    rows_after_step1 = len(combined_df)
    print(f"   -> Krok 1 (usuwanie idealnych duplikatów): Usunięto {pre_dedup_rows - rows_after_step1} wierszy.")
    
    # b) Inteligentnie scal wiersze z tym samym TIMESTAMP, ale różnymi danymi
    if not combined_df.empty:
        final_df = combined_df.groupby('TIMESTAMP').first().reset_index()
    else:
        final_df = pd.DataFrame()
        
    final_rows = len(final_df)
    print(f"   -> Krok 2 (inteligentne scalanie): Scalono {rows_after_step1} wierszy do {final_rows} unikalnych rekordów.")
    
    # Krok 6: Weryfikacja i zapis
    print("\n6. Weryfikacja wyniku...")
    final_duplicates = final_df[final_df.duplicated(subset=['TIMESTAMP'], keep=False)]
    if not final_duplicates.empty:
        print("   -> BŁĄD KRYTYCZNY: Po deduplikacji nadal istnieją zduplikowane znaczniki czasu!")
        print(final_duplicates)
    else:
        print("   -> SUKCES: W finalnym zbiorze nie ma zduplikowanych znaczników czasu.")
    
    try:
        final_df.sort_values(by='TIMESTAMP', inplace=True)
        final_df.to_csv(OUTPUT_PATH, index=False)
        print(f"\n7. Zapisano wynik do pliku: {OUTPUT_PATH}")
    except Exception as e:
        print(f"\nBŁĄD: Nie udało się zapisać pliku wynikowego: {e}")

print("\n" + "="*30 + " KONIEC DIAGNOSTYKI " + "="*30)