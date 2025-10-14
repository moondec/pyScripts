import pandas as pd
import os

# --- KONFIGURACJA TESTU ---
FILE_PATH_1 = r"D:\sites\ME\dT_ME\ThermoFisher\20200709\DT85_down\pom1m_20200207T234500.CSV"
FILE_PATH_2 = r"D:\sites\ME\dT_ME\ThermoFisher\20200709\DT85_down\pom1m_20200119T234501.CSV"
OUTPUT_PATH = r"C:\Users\marek.urbaniak\Documents\pyScripts\wynik_finalnej_deduplikacji.csv"
INTERVAL_TO_TEST = '1min' # Ustaw właściwy interwał
PRECISION = 6

# --- Funkcja wczytująca ---
def read_and_align(file_path, interval):
    try:
        df = pd.read_csv(file_path, header=0, low_memory=False, encoding='latin-1')
        if 'Timestamp' in df.columns: df.rename(columns={'Timestamp':'TIMESTAMP'}, inplace=True)
        if 'TIMESTAMP' not in df.columns: return pd.DataFrame()
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        df.dropna(subset=['TIMESTAMP'], inplace=True)
        if interval: df['TIMESTAMP'] = df['TIMESTAMP'].dt.round(interval)
        return df
    except Exception as e: print(f"BŁĄD wczytywania {file_path}: {e}"); return pd.DataFrame()

# --- GŁÓWNA LOGIKA DIAGNOSTYCZNA ---
print("="*30 + " START DIAGNOSTYKI DEDUPLIKACJI " + "="*30)

df1 = read_and_align(FILE_PATH_1, INTERVAL_TO_TEST)
if not df1.empty: df1['source_file'] = os.path.basename(FILE_PATH_1)
df2 = read_and_align(FILE_PATH_2, INTERVAL_TO_TEST)
if not df2.empty: df2['source_file'] = os.path.basename(FILE_PATH_2)

if not df1.empty or not df2.empty:
    combined_df = pd.concat([df1, df2], ignore_index=True)
    initial_rows = len(combined_df)
    print(f"\n1. Połączono dane. Łączna liczba wierszy: {initial_rows}")

    print(f"\n2. Normalizacja danych: zaokrąglanie wartości float do {PRECISION} miejsc...")
    float_columns = combined_df.select_dtypes(include='float').columns
    if not float_columns.empty:
        combined_df[float_columns] = combined_df[float_columns].round(PRECISION)

    print("\n3. Zastosowanie dwustopniowej deduplikacji...")

    # === NOWA, KLUCZOWA ZMIANA ===
    # Tworzymy listę kolumn do sprawdzenia, ignorując 'source_file'
    cols_for_dedup = [col for col in combined_df.columns if col != 'source_file']
    
    rows_before_perfect = len(combined_df)
    # Usuń wiersze, które są w 100% identyczne we WSZYSTKICH KOLUMNACH POZA 'source_file'
    combined_df.drop_duplicates(subset=cols_for_dedup, inplace=True)
    rows_after_perfect = len(combined_df)
    print(f"   -> Krok 1 (usuwanie idealnych duplikatów wg wartości): Usunięto {rows_before_perfect - rows_after_perfect} wierszy.")
    # === KONIEC ZMIANY ===
    
    # Inteligentnie scal wiersze z tym samym TIMESTAMP, ale różnymi danymi
    if not combined_df.empty:
        final_df = combined_df.groupby('TIMESTAMP').first().reset_index()
    else:
        final_df = pd.DataFrame()
        
    final_rows = len(final_df)
    print(f"   -> Krok 2 (inteligentne scalanie): Scalono {rows_after_perfect} wierszy do {final_rows} unikalnych rekordów.")
    
    print("\n4. Weryfikacja wyniku...")
    final_duplicates = final_df[final_df.duplicated(subset=['TIMESTAMP'], keep=False)]
    if not final_duplicates.empty:
        print("   -> BŁĄD KRYTYCZNY: Po deduplikacji nadal istnieją zduplikowane znaczniki czasu!")
    else:
        print("   -> SUKCES: W finalnym zbiorze nie ma zduplikowanych znaczników czasu.")
    
    try:
        final_df.sort_values(by='TIMESTAMP', inplace=True)
        final_df.to_csv(OUTPUT_PATH, index=False)
        print(f"\n5. Zapisano wynik do pliku: {OUTPUT_PATH}")
    except Exception as e:
        print(f"\nBŁĄD: Nie udało się zapisać pliku wynikowego: {e}")
else:
    print("\nNie można przeprowadzić testu, oba pliki wejściowe są puste.")

print("\n" + "="*30 + " KONIEC DIAGNOSTYKI " + "="*30)