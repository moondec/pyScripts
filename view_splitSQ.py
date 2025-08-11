import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
import re
from multiprocessing import Pool, cpu_count

# --- CONFIGURATION SECTION ---
CONFIG = {
    # Path to the SQLite database
    "DB_PATH": r"E:\test_split\metLab.db",
    # Directory for saving output PDF files
    "OUTPUT_DIR": r"E:\pdfy\view_splitSQ",
    # Table names for metadata
    "STATIONS_TABLE": "stations",
    "GROUPS_TABLE": "groups",
    # Maximum number of plots per PDF page
    "PLOTS_PER_PAGE": 6,
    # Layout of the plot grid on a page (rows, columns)
    "GRID_LAYOUT": (3, 2),
    # Optional filtering of variables to be processed
    "VARIABLE_SELECTION": {
        "prefixes": ["PPFD_IN", "SW_IN", "PPFD_BC_IN"],
        "exact_names": []
    }
}

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- HELPER FUNCTIONS ---

def create_dummy_database(config: dict):
    """
    Creates a dummy SQLite database with a relational structure and multiple 'data_' tables.
    """
    db_path = config["DB_PATH"]
    if os.path.exists(db_path):
        logging.info(f"Database '{db_path}' already exists. Skipping creation.")
        return

    logging.info(f"Creating a dummy relational database at '{db_path}'...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Create metadata tables
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config["STATIONS_TABLE"]} (
        station_id TEXT PRIMARY KEY, name TEXT, latitude REAL, longitude REAL
    )""")
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config["GROUPS_TABLE"]} (
        group_id TEXT PRIMARY KEY, station_id TEXT NOT NULL, interval TEXT,
        FOREIGN KEY (station_id) REFERENCES {config["STATIONS_TABLE"]} (station_id)
    )""")
    conn.commit()

    # 2. Create multiple data tables for a single station to test the logic
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_SA_MET_1min (
        TIMESTAMP DATETIME PRIMARY KEY,
        TA_1_1_1 REAL, TA_1_1_1_flag INTEGER
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_SA_MET_30min (
        TIMESTAMP DATETIME PRIMARY KEY,
        TS_7_1_1 REAL,
        TA_1_1_1 REAL 
    )""")

    # 3. Populate tables
    stations_data = [('STATION_SA', 'Sample A', 52.40, 16.92)]
    groups_data = [('SA_MET_1min', 'STATION_SA', '1min'), ('SA_MET_30min', 'STATION_SA', '30min')]
    cursor.executemany(f'INSERT INTO {config["STATIONS_TABLE"]} VALUES (?, ?, ?, ?)', stations_data)
    cursor.executemany(f'INSERT INTO {config["GROUPS_TABLE"]} VALUES (?, ?, ?)', groups_data)

    data_1min, data_30min = [], []
    for day in range(1):
        for minute in range(0, 1440, 1):
            ts = datetime(2024, 7, 20) + timedelta(days=day, minutes=minute)
            ta = 15 + 10 * np.sin((ts.hour + ts.minute/60 - 9) * np.pi / 12)
            data_1min.append((ts.strftime("%Y-%m-%d %H:%M:%S"), ta, 0))
            if minute % 30 == 0:
                data_30min.append((ts.strftime("%Y-%m-%d %H:%M:%S"), ta - 5, ta * 1.1)) # Add TA_1_1_1 here as well
    
    cursor.executemany('INSERT INTO data_SA_MET_1min VALUES (?, ?, ?)', data_1min)
    cursor.executemany('INSERT INTO data_SA_MET_30min VALUES (?, ?, ?)', data_30min)
    
    conn.commit()
    conn.close()
    logging.info("Dummy relational database created successfully.")


def get_group_info(config: dict) -> dict:
    """
    Fetches group IDs and their coordinates by joining the groups and stations tables.
    """
    logging.info("Fetching group and station information...")
    with sqlite3.connect(config["DB_PATH"]) as conn:
        query = f"""
        SELECT g.group_id, s.latitude, s.longitude 
        FROM {config["GROUPS_TABLE"]} AS g
        JOIN {config["STATIONS_TABLE"]} AS s ON g.station_id = s.station_id
        """
        df = pd.read_sql_query(query, conn)
    return {row['group_id']: {'lat': row['latitude'], 'lon': row['longitude']} for _, row in df.iterrows()}


def get_table_to_variables_map(config: dict) -> dict:
    """
    Scans all tables starting with 'data_' and creates a map of
    {table_name: [list_of_variables]}.
    """
    table_map = {}
    with sqlite3.connect(config["DB_PATH"]) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'data_%'")
        data_tables = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found data tables: {data_tables}")

        metadata_cols = {'group_id', 'TIMESTAMP'}
        for table_name in data_tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            variables = [col for col in columns if col not in metadata_cols and not col.lower().endswith('_flag')]
            if variables:
                table_map[table_name] = variables
    return table_map

def filter_variables(variables: list, selection_rules: dict) -> list:
    """
    Filters variables based on a flexible pattern and exact names. 
    The prefix check is case-insensitive.
    """
    if not selection_rules or (not selection_rules.get("prefixes") and not selection_rules.get("exact_names")):
        return variables
    prefixes = selection_rules.get("prefixes", [])
    exact_names = selection_rules.get("exact_names", [])
    matched_variables = set()

    if prefixes:
        for var in variables:
            for prefix in prefixes:
                # Flexible regex that allows other characters (like '_IN_') between prefix and H_V_R suffix.
                pattern = re.compile(f"^{prefix}.*?_\\d{{1,2}}_\\d{{1,2}}_\\d{{1,2}}$", re.IGNORECASE)
                if pattern.match(var):
                    matched_variables.add(var)
                    break 
    
    if exact_names:
        all_vars_lower = {v.lower(): v for v in variables}
        for name in exact_names:
            if name.lower() in all_vars_lower:
                matched_variables.add(all_vars_lower[name.lower()])
    
    return sorted(list(matched_variables))

def potential_radiation(date_time_index: pd.DatetimeIndex, latitude: float, longitude: float) -> pd.DataFrame:
    """
    Calculates potential solar radiation, simplified to peak at 12:00 local time.
    """
    if not isinstance(date_time_index, pd.DatetimeIndex):
        raise TypeError("Input must be a pandas DatetimeIndex.")
    
    tv_hh = date_time_index.hour + date_time_index.minute / 60 + date_time_index.second / 3600
    doy = date_time_index.dayofyear
    lat_rad = np.deg2rad(latitude)
    declination = np.deg2rad(-23.44) * np.cos(np.deg2rad(360 / 365 * (doy + 10)))
    hour_angle = np.pi * (tv_hh - 12) / 12
    sin_psi = (np.sin(lat_rad) * np.sin(declination) + 
               np.cos(lat_rad) * np.cos(declination) * np.cos(hour_angle))
    radiation = np.zeros_like(sin_psi, dtype=float)
    S = 1370
    mask = sin_psi > 0
    transmissivity = 0.6 + 0.2 * sin_psi[mask]
    radiation[mask] = S * transmissivity * sin_psi[mask]
    return pd.DataFrame({'Radiation': radiation}, index=date_time_index)

def create_processing_tasks(groups: dict, table_to_vars_map: dict, selection_rules: dict) -> list:
    """
    Creates a definitive list of tasks. Each task is a unique combination of
    (group, variable, table) ensuring every occurrence of a variable is processed.
    """
    tasks = []
    
    for table_name, variables_in_table in table_to_vars_map.items():
        # Find the best matching group for the current table
        best_match_group_id = None
        best_match_len = 0
        for group_id in groups.keys():
            # A more robust check: see if the group_id is a substring of the table_name
            if group_id.lower() in table_name.lower() and len(group_id) > best_match_len:
                best_match_group_id = group_id
                best_match_len = len(group_id)

        if not best_match_group_id:
            logging.warning(f"Could not find a matching group for table '{table_name}'. Skipping its variables.")
            continue

        group_meta = groups[best_match_group_id]
        
        # Filter the variables from this specific table
        selected_vars_for_table = filter_variables(variables_in_table, selection_rules)
        
        # Create a task for each selected variable found in this table
        for var_name in selected_vars_for_table:
            tasks.append((best_match_group_id, group_meta, var_name, table_name))
    
    return tasks

def process_single_task(group_id, group_meta, var_name, table_name):
    """
    Worker function to process a single variable from a specific table and generate a PDF.
    """
    try:
        flag_name = f"{var_name}_flag"
        
        with sqlite3.connect(CONFIG["DB_PATH"]) as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            available_cols = [row[1] for row in cursor.fetchall()]
            
            cols_to_select = ['TIMESTAMP', f'"{var_name}"']
            if flag_name in available_cols:
                cols_to_select.append(f'"{flag_name}"')

            query = f'SELECT {", ".join(cols_to_select)} FROM "{table_name}"'
            df = pd.read_sql_query(query, conn)

        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df.set_index('TIMESTAMP', inplace=True)

        if df.empty or df[var_name].isnull().all():
            return f"Skipped: No data for {group_id}/{var_name} in {table_name}"
        
        has_flag_column = flag_name in df.columns
        
        # MODIFIED: Filename now includes the table name to ensure uniqueness
        pdf_filename = f"{group_id}_{var_name}_from_{table_name}.pdf"
        pdf_path = os.path.join(CONFIG["OUTPUT_DIR"], pdf_filename)
        
        with PdfPages(pdf_path) as pdf:
            daily_groups = [group for _, group in df.groupby(df.index.date)]
            for i in range(0, len(daily_groups), CONFIG["PLOTS_PER_PAGE"]):
                page_groups = daily_groups[i:i + CONFIG["PLOTS_PER_PAGE"]]
                fig, axes = plt.subplots(nrows=CONFIG["GRID_LAYOUT"][0], ncols=CONFIG["GRID_LAYOUT"][1], figsize=(8.27, 11.69), constrained_layout=True)
                fig.suptitle(f"Group: {group_id} - Variable: {var_name}\n(Source Table: {table_name})", fontsize=14)
                axes_flat = axes.flatten()

                for j, day_df in enumerate(page_groups):
                    ax = axes_flat[j]
                    day_date = day_df.index[0].date()
                    
                    if has_flag_column:
                        good_data = day_df[day_df[flag_name] == 0]
                        bad_data = day_df[day_df[flag_name] != 0]
                        if not good_data.empty: ax.plot(good_data.index, good_data[var_name], 'b-', marker='.', markersize=3, label='Flag 0 (Good)')
                        if not bad_data.empty: ax.plot(bad_data.index, bad_data[var_name], 'r.', markersize=4, label='Other Flags (Bad)')
                    else:
                        if not day_df.empty: ax.plot(day_df.index, day_df[var_name], 'b-', marker='.', markersize=3, label='Data (no flag)')
                    
                    if var_name.startswith(('PPFD', 'SW_IN')):
                        rad_df = potential_radiation(day_df.index, group_meta['lat'], group_meta['lon'])
                        ax.plot(rad_df.index, rad_df['Radiation'], 'g--', linewidth=1.2, label='Potential Rad.')
                    
                    ax.set_title(day_date.strftime('%Y-%m-%d'), fontsize=10)
                    ax.set_ylabel(var_name, fontsize=8)
                    ax.grid(True, linestyle='--', alpha=0.6)
                    
                    data_for_ylim = good_data if has_flag_column and not good_data.empty else day_df
                    if data_for_ylim[var_name].notna().any():
                        ymin, ymax = data_for_ylim[var_name].min(), data_for_ylim[var_name].max()
                        margin = (ymax - ymin) * 0.1 if (ymax - ymin) > 0 else 1
                        ax.set_ylim(ymin - margin, ymax + margin)
                    
                    start_of_day = datetime.combine(day_date, datetime.min.time())
                    end_of_day = start_of_day + timedelta(days=1)
                    ax.set_xlim(start_of_day, end_of_day)
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=8)
                    if j == 0: ax.legend(fontsize=7)

                for j in range(len(page_groups), len(axes_flat)):
                    axes_flat[j].set_visible(False)
                    
                pdf.savefig(fig)
                plt.close(fig)
        return f"OK: Generated report for {group_id}/{var_name} from table {table_name}"
    except Exception as e:
        return f"ERROR: Failed {group_id}/{var_name} from {table_name} with error: {e}"

# --- MAIN ORCHESTRATOR ---
def generate_reports():
    """Main function to orchestrate the report generation process."""
    # create_dummy_database(CONFIG) # Commented out for use with a real database
    os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)
    
    try:
        groups = get_group_info(CONFIG)
        if not groups:
            logging.error("No group information found in the database.")
            return
        
        table_to_vars_map = get_table_to_variables_map(CONFIG)
        
        tasks = create_processing_tasks(groups, table_to_vars_map, CONFIG.get("VARIABLE_SELECTION"))
        logging.info(f"Created {len(tasks)} processing tasks.")
        if not tasks:
            logging.warning("No tasks created. Check variable selection and table/group naming.")
            return

    except Exception as e:
        logging.error(f"Could not fetch metadata or create tasks. Error: {e}")
        return

    # --- Parallel Processing ---
    num_processes = max(1, cpu_count() - 1)
    logging.info(f"Starting parallel processing with {num_processes} workers...")

    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.starmap(process_single_task, tasks), total=len(tasks), desc="Generating PDFs"))

    # Log results from workers
    for res in results:
        if "ERROR" in res:
            logging.error(res)
        elif "Skipped" in res:
            logging.debug(res)
        else:
            logging.info(res.replace("OK: ", ""))


    logging.info("Processing finished successfully.")

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    generate_reports()
