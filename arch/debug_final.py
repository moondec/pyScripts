import pandas as pd
import os
import struct
import re
from datetime import datetime

# --- Pełen zestaw funkcji pomocniczych z głównego skryptu ---
CAMPBELL_EPOCH = pd.Timestamp('1990-01-01 00:00:00', tz='UTC')
STRUCT_FORMAT_MAP = {'ULONG':'L', 'IEEE4':'f', 'IEEE8':'d', 'LONG':'l', 'BOOL':'?', 'SHORT':'h', 'USHORT':'H', 'BYTE':'b'}
def parse_header_line(line): return [field.strip() for field in line.strip('"').split('","')]
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
def read_tob1_data(file_path,metadata):
    col_names,struct_pattern,num_header_lines,fp2_cols=metadata;records=[]
    try:
        if not col_names:return pd.DataFrame()
        record_size=struct.calcsize(struct_pattern)
        if record_size==0:return pd.DataFrame(columns=col_names)
        with open(file_path,'rb')as f:
            for i in range(num_header_lines):
                if not f.readline():return pd.DataFrame(columns=col_names)
            processed_records_count=0
            while True:
                chunk=f.read(record_size)
                if not chunk:break
                if len(chunk)<record_size:break
                try:records.append(struct.unpack(struct_pattern,chunk));processed_records_count+=1
                except struct.error as e:print(f"BŁĄD ROZPAKOWYWANIA TOB1 w {file_path} (rekord ~{processed_records_count+1}): {e}");break
        if not records:return pd.DataFrame(columns=col_names)
        try:df=pd.DataFrame(records,columns=col_names)
        except ValueError as ve:print(f"Błąd DataFrame TOB1 {os.path.basename(file_path)}: {ve}");return pd.DataFrame()
        if not df.empty and fp2_cols:
            for fp2_col_name in fp2_cols:
                if fp2_col_name in df.columns:
                    try:
                        if not pd.api.types.is_numeric_dtype(df[fp2_col_name]):df[fp2_col_name]=pd.to_numeric(df[fp2_col_name],errors='coerce')
                        df[fp2_col_name]=df[fp2_col_name].apply(decode_csi_fs2_float)
                    except Exception as e_fp2:print(f"  OSTRZEŻENIE: Błąd dekodowania FP2 dla '{fp2_col_name}': {e_fp2}")
        timestamp_created_successfully=False
        if'SECONDS'in df.columns and'NANOSECONDS'in df.columns:
            df['SECONDS']=pd.to_numeric(df['SECONDS'],errors='coerce');df['NANOSECONDS']=pd.to_numeric(df['NANOSECONDS'],errors='coerce')
            valid_ts_data_indices=df.dropna(subset=['SECONDS','NANOSECONDS']).index
            if not valid_ts_data_indices.empty:
                try:
                    secs_s=df.loc[valid_ts_data_indices,'SECONDS'];nanos_s=df.loc[valid_ts_data_indices,'NANOSECONDS']
                    df.loc[valid_ts_data_indices,'TIMESTAMP']=CAMPBELL_EPOCH+pd.to_timedelta(secs_s,unit='s')+pd.to_timedelta(nanos_s,unit='ns')
                    timestamp_created_successfully=True
                except Exception as e_ts:print(f"Błąd tworzenia TIMESTAMP (TOB1) dla {file_path}: {e_ts}")
        if not timestamp_created_successfully and'TIMESTAMP'in df.columns and df['TIMESTAMP'].isnull().all():del df['TIMESTAMP']
        return df
    except Exception as e:print(f"Krytyczny błąd odczytu TOB1 {file_path}: {e}");return pd.DataFrame()
def get_toa5_metadata(file_path):
    try:
        with open(file_path,'r',encoding='latin-1')as f:header_lines=[f.readline().strip()for _ in range(4)]
        if not header_lines[0].startswith('"TOA5"'):return None
        return parse_header_line(header_lines[1]),4
    except Exception:return None
def read_toa5_data(file_path,metadata):
    col_names,num_header_lines=metadata
    try:
        df=pd.read_csv(file_path,skiprows=num_header_lines,header=None,names=col_names,na_values=['"NAN"','NAN','"INF"','""',''],quotechar='"',escapechar='\\',on_bad_lines='warn',encoding='latin-1')
        if'TIMESTAMP'not in df.columns:return pd.DataFrame()
        df['TIMESTAMP']=pd.to_datetime(df['TIMESTAMP'],errors='coerce');df.dropna(subset=['TIMESTAMP'],inplace=True)
        return df
    except Exception:return pd.DataFrame()
def read_simple_csv_data(file_path):
    try:
        df=pd.read_csv(file_path,header=0,low_memory=False,encoding='latin-1')
        if'Timestamp'not in df.columns:return pd.DataFrame()
        df.rename(columns={'Timestamp':'TIMESTAMP'},inplace=True)
        df['TIMESTAMP']=pd.to_datetime(df['TIMESTAMP'],errors='coerce')
        if df['TIMESTAMP'].isnull().any():df.dropna(subset=['TIMESTAMP'],inplace=True)
        return df
    except Exception as e:print(f"Krytyczny błąd odczytu SimpleCSV z {file_path}: {e}");return pd.DataFrame()

def read_any_file(file_path):
    """Wczytuje plik, automatycznie rozpoznając jego typ."""
    try:
        with open(file_path, 'r', encoding='latin-1') as f:
            first_line = f.readline().strip()
    except Exception as e:
        print(f"Nie można otworzyć lub odczytać pierwszej linii pliku: {e}")
        return None

    print(f"INFO: Rozpoznano pierwszą linię: {first_line[:40]}...")

    if first_line.startswith('"TOB1"'):
        print("INFO: Identyfikuję jako plik TOB1.")
        metadata = get_tob1_metadata(file_path)
        if metadata: return read_tob1_data(file_path, metadata)
    elif first_line.startswith('"TOA5"'):
        print("INFO: Identyfikuję jako plik TOA5.")
        metadata = get_toa5_metadata(file_path)
        if metadata: return read_toa5_data(file_path, metadata)
    elif first_line.startswith('"Timestamp"'):
        print("INFO: Identyfikuję jako plik SimpleCSV.")
        return read_simple_csv_data(file_path)
    else:
        print("BŁĄD: Nie rozpoznano typu pliku.")
        return None

# --- UŻYTKOWNIK MUSI WYPEŁNIĆ TĘ LINIĘ ---
PROBLEM_FILE_PATH = r"D:\sites\ME\dT_ME\ThermoFisher\20200709\DT85_down\pom1m_20200119T234501.CSV"

# --- GŁÓWNA LOGIKA DIAGNOSTYCZNA ---
if not os.path.exists(PROBLEM_FILE_PATH):
    print(f"BŁĄD: Plik nie istnieje pod podaną ścieżką: {PROBLEM_FILE_PATH}")
else:
    df = read_any_file(PROBLEM_FILE_PATH)
    
    if df is not None and not df.empty and 'TIMESTAMP' in df.columns:
        ts_series = df['TIMESTAMP'].dropna()
        
        if not ts_series.empty:
            print("\n" + "="*25 + " DIAGNOSTYKA PRZED OPERACJĄ " + "="*25)
            print(f"Typ obiektu: \t\t\t{type(ts_series)}")
            print(f"Typ danych (dtype) serii: \t{ts_series.dtype}")
            print(f"Czy seria jest 'świadoma' strefy? \t{pd.api.types.is_datetime64_any_dtype(ts_series) and ts_series.dt.tz is not None}")
            print("\nPierwsze 3 wartości:")
            print(ts_series.head(3))
            print("\nOstatnie 3 wartości:")
            print(ts_series.tail(3))
            print("="*72)

            try:
                print("\n>>> PRÓBA WYKONANIA PROBLEMATYCZNEJ OPERACJI...")
                df_test = df.copy()
                
                # Symulujemy domyślne zachowanie z `apply_timezone_correction`
                if df_test['TIMESTAMP'].dt.tz is None:
                    print(">>> Wykonuję: .dt.tz_localize('UTC', ...)")
                    df_test.loc[:, 'TIMESTAMP'] = df_test['TIMESTAMP'].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
                else:
                    print(">>> Wykonuję: .dt.tz_convert('UTC')")
                    df_test.loc[:, 'TIMESTAMP'] = df_test['TIMESTAMP'].dt.tz_convert('UTC')
                
                print("\n--- OPERACJA ZAKOŃCZONA SUKCESEM ---")
                print(f"Nowy typ danych (dtype): \t{df_test['TIMESTAMP'].dtype}")

            except Exception as e:
                print("\n" + "!"*28 + " WYSTĄPIŁ BŁĄD! " + "!"*28)
                print(f"Typ błędu: {type(e)}")
                print(f"Treść błędu:\n{e}")
                import traceback
                traceback.print_exc()
                print("!"*72)
        else:
            print("Wczytano plik, ale kolumna TIMESTAMP jest pusta po usunięciu błędów.")
    else:
        print("Nie udało się wczytać danych lub ramka danych jest pusta.")