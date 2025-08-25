import pandas as pd
import os

# --- KONFIGURACJA TESTU ---
# Proszę podać pełne ścieżki do plików, które mają być przetestowane

FILE_PATH_1 = r"D:\sites\ME\dT_ME\ThermoFisher\20200709\DT85_down\pom1m_20200207T234500.CSV"
FILE_PATH_2 = r"D:\sites\ME\dT_ME\ThermoFisher\20200709\DT85_down\pom1m_20200119T234501.CSV"

# Interwał, do którego mają być zaokrąglone znaczniki czasu (bardzo ważne!)
# Użyj tego samego, co w Twojej konfiguracji `FILE_ID_MERGE_GROUPS`, np. '30min', '1min'
INTERVAL_TO_TEST = '1min'

# --- Minimalny zestaw funkcji z głównego skryptu ---
# (Uproszczone, aby skupić się tylko na problemie deduplikacji)

def read_and_align(file_path, interval):
    """Wczytuje i od razu wyrównuje czas w pojedynczym pliku."""
    try:
        # Uproszczone wczytywanie dla plików CSV - dostosuj w razie potrzeby
        df = pd.read_csv(file_path, header=0, low_memory=False, encoding='latin-1')
        if 'Timestamp' in df.columns:
            df.rename(columns={'Timestamp': 'TIMESTAMP'}, inplace=True)
        
        if 'TIMESTAMP' not in df.columns: return pd.DataFrame()
            
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        df.dropna(subset=['TIMESTAMP'], inplace=True)
        
        # Kluczowy krok: od razu wyrównujemy czas
        if interval:
            df['TIMESTAMP'] = df['TIMESTAMP'].dt.round(interval)
        
        return df
    except Exception as e:
        print(f"BŁĄD wczytywania {file_path}: {e}")
        return pd.DataFrame()

# --- GŁÓWNA LOGIKA DIAGNOSTYCZNA ---

print("="*30 + " START DIAGNOSTYKI DEDUPLIKACJI " + "="*30)

# Krok 1: Wczytaj i wyrównaj czas w obu plikach osobno
print(f"\n1. Wczytywanie i wyrównywanie pliku 1...")
df1 = read_and_align(FILE_PATH_1, INTERVAL_TO_TEST)
if df1.empty:
    print("   -> BŁĄD: Nie udało się wczytać pliku 1 lub jest on pusty.")
else:
    df1['source_file'] = os.path.basename(FILE_PATH_1)
    df1.to_csv('_debug_1_file1_aligned.csv', index=False)
    print(f"   -> Wczytano {len(df1)} wierszy. Zapisano do _debug_1_file1_aligned.csv")

print(f"\n2. Wczytywanie i wyrównywanie pliku 2...")
df2 = read_and_align(FILE_PATH_2, INTERVAL_TO_TEST)
if df2.empty:
    print("   -> BŁĄD: Nie udało się wczytać pliku 2 lub jest on pusty.")
else:
    df2['source_file'] = os.path.basename(FILE_PATH_2)
    df2.to_csv('_debug_2_file2_aligned.csv', index=False)
    print(f"   -> Wczytano {len(df2)} wierszy. Zapisano do _debug_2_file2_aligned.csv")

# Krok 2: Połącz dane
if not df1.empty and not df2.empty:
    combined_df = pd.concat([df1, df2], ignore_index=True)
    print(f"\n3. Połączono dane. Łączna liczba wierszy: {len(combined_df)}")
    combined_df.to_csv('_debug_3_combined_before_dedup.csv', index=False)
    print("   -> Zapisano połączone dane do _debug_3_combined_before_dedup.csv")
    
    # Krok 3: Zastosuj logikę deduplikacji
    print("\n4. Zastosowanie dwustopniowej deduplikacji...")

    # a) Normalizacja i usuwanie idealnych duplikatów
    for col in combined_df.columns:
        if col not in ['TIMESTAMP', 'source_file']:
            if not pd.api.types.is_numeric_dtype(combined_df[col]):
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    
    float_columns = combined_df.select_dtypes(include='float').columns
    combined_df[float_columns] = combined_df[float_columns].round(6)
    
    rows_before = len(combined_df)
    combined_df.drop_duplicates(inplace=True)
    rows_after_perfect_dedup = len(combined_df)
    print(f"   -> Krok 1 (usuwanie idealnych duplikatów): Usunięto {rows_before - rows_after_perfect_dedup} wierszy.")

    # b) Inteligentne scalanie
    final_df = combined_df.groupby('TIMESTAMP').first().reset_index()
    final_rows = len(final_df)
    print(f"   -> Krok 2 (inteligentne scalanie): Scalono {rows_after_perfect_dedup} wierszy do {final_rows} unikalnych rekordów.")

    # Krok 4: Weryfikacja i zapis
    print("\n5. Weryfikacja wyniku...")
    final_duplicates = final_df[final_df.duplicated(subset=['TIMESTAMP'], keep=False)]
    if not final_duplicates.empty:
        print("   -> BŁĄD KRYTYCZNY: Po deduplikacji nadal istnieją zduplikowane znaczniki czasu!")
    else:
        print("   -> SUKCES: W finalnym zbiorze nie ma zduplikowanych znaczników czasu.")
    
    final_df.sort_values(by='TIMESTAMP', inplace=True)
    final_df.to_csv('_debug_4_final_output.csv', index=False)
    print(f"\n6. Zapisano ostateczny wynik do pliku: _debug_4_final_output.csv")

else:
    print("\nNie można przeprowadzić testu, ponieważ jeden z plików wejściowych jest pusty.")

print("\n" + "="*30 + " KONIEC DIAGNOSTYKI " + "="*30)