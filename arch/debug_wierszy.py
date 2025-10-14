import pandas as pd
import os

# --- KONFIGURACJA TESTU ---
# Proszę podać pełne ścieżki do plików, które mają być przetestowane

FILE_PATH_1 = r"D:\sites\ME\dT_ME\ThermoFisher\20180903\DT85_down\pom1m_20180621T234500.CSV"
FILE_PATH_2 = r"D:\sites\ME\dT_ME\ThermoFisher\20180903\DT85_down\pom1m_20180620T234501.CSV"

# Podaj dokładny string znacznika czasu, który chcesz porównać
TIMESTAMP_DO_SPRAWDZENIA = "2018/06/19 23:46:00.912"

# --- Funkcja wczytująca ---
def read_test_file(file_path):
    """Wczytuje plik testowy, zakładając prosty format CSV z nagłówkiem."""
    try:
        df = pd.read_csv(file_path, header=0, low_memory=False, encoding='latin-1')
        if 'Timestamp' in df.columns:
            df.rename(columns={'Timestamp': 'TIMESTAMP'}, inplace=True)
        # Na potrzeby tego testu nie konwertujemy od razu na datetime,
        # aby zobaczyć oryginalne stringi.
        return df
    except Exception as e:
        print(f"BŁĄD podczas wczytywania pliku {file_path}: {e}")
        return None

# --- GŁÓWNA LOGIKA DIAGNOSTYCZNA ---

print("="*30 + " START DIAGNOSTYKI WIERSZY " + "="*30)

df1 = read_test_file(FILE_PATH_1)
df2 = read_test_file(FILE_PATH_2)

if df1 is None or df2 is None or df1.empty or df2.empty:
    print("Nie udało się wczytać obu plików. Zakończono.")
else:
    # Znajdź wiersze dla podanego timestampu
    row1 = df1[df1['TIMESTAMP'] == TIMESTAMP_DO_SPRAWDZENIA]
    row2 = df2[df2['TIMESTAMP'] == TIMESTAMP_DO_SPRAWDZENIA]

    if row1.empty or row2.empty:
        print(f"BŁĄD: Nie znaleziono wiersza dla znacznika czasu '{TIMESTAMP_DO_SPRAWDZENIA}' w jednym z plików.")
    else:
        # Weź pierwszy znaleziony wiersz (na wypadek, gdyby nawet w jednym pliku były duplikaty)
        series1 = row1.iloc[0]
        series2 = row2.iloc[0]

        print(f"\nPorównanie wierszy dla TIMESTAMP: {TIMESTAMP_DO_SPRAWDZENIA}\n")
        
        # Stwórz ramkę danych do porównania
        comparison_df = pd.DataFrame({
            'Kolumna': series1.index,
            'Wartość_Plik_1': series1.values,
            'Typ_Plik_1': [type(v).__name__ for v in series1.values],
            'Wartość_Plik_2': series2.values,
            'Typ_Plik_2': [type(v).__name__ for v in series2.values],
        })

        # Sprawdź równość
        comparison_df['Czy_Identyczne'] = comparison_df.apply(
            lambda row: 'TAK' if str(row['Wartość_Plik_1']) == str(row['Wartość_Plik_2']) else 'NIE',
            axis=1
        )
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 200)
        print(comparison_df)

print("\n" + "="*30 + " KONIEC DIAGNOSTYKI " + "="*30)