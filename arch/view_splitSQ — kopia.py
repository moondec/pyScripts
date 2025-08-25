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

# --- CONFIGURATION SECTION ---
CONFIG = {
    # Path to the SQLite database
    "DB_PATH": r"E:\test_split\metLab.db",
    # Directory for saving output PDF files
    "OUTPUT_DIR": r"E:\pdfy\view_splitSQ",
    # Table names
    "STATIONS_TABLE": "stations",
    "GROUPS_TABLE": "groups",
    "DATA_TABLE": "data", # Assuming time-series data is in this table
    # Maximum number of plots per PDF page
    "PLOTS_PER_PAGE": 6,
    # Layout of the plot grid on a page (rows, columns)
    "GRID_LAYOUT": (3, 2),
    # Optional filtering of variables to be processed
    "VARIABLE_SELECTION": {
        "prefixes": ["PPFD", "PPFD_IN" "SW_IN", "TA", "TS"],
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
    Creates a dummy SQLite database with the new relational structure.
    """
    db_path = config["DB_PATH"]
    if os.path.exists(db_path):
        logging.info(f"Database '{db_path}' already exists. Skipping creation.")
        return

    logging.info(f"Creating a dummy relational database at '{db_path}'...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Create metadata tables (stations, groups) as per the new schema
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config["STATIONS_TABLE"]} (
        station_id TEXT PRIMARY KEY, name TEXT, latitude REAL, longitude REAL
    )""")
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config["GROUPS_TABLE"]} (
        group_id TEXT PRIMARY KEY, station_id TEXT NOT NULL, interval TEXT,
        FOREIGN KEY (station_id) REFERENCES {config["STATIONS_TABLE"]} (station_id)
    )""")

    # 2. Create the main data table (assuming this structure)
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {config["DATA_TABLE"]} (
        group_id TEXT NOT NULL,
        TIMESTAMP DATETIME NOT NULL,
        PPFD_IN_1_1_1 REAL, PPFD_IN_1_1_1_flag INTEGER,
        TA_1_1_1 REAL, TA_1_1_1_flag INTEGER,
        TS_7_1_1 REAL, -- Example of a variable without a flag
        PRIMARY KEY (group_id, TIMESTAMP),
        FOREIGN KEY (group_id) REFERENCES {config["GROUPS_TABLE"]} (group_id)
    )""")

    # 3. Populate tables with sample data
    stations_data = [('STATION_A', 'Poznan', 52.40, 16.92), ('STATION_B', 'Krakow', 50.06, 19.94)]
    groups_data = [('POZ_30MIN', 'STATION_A', '30min'), ('KRA_30MIN', 'STATION_B', '30min')]
    
    cursor.executemany(f'INSERT INTO {config["STATIONS_TABLE"]} VALUES (?, ?, ?, ?)', stations_data)
    cursor.executemany(f'INSERT INTO {config["GROUPS_TABLE"]} VALUES (?, ?, ?)', groups_data)

    # Generate and insert time-series data
    data_to_insert = []
    for group_id, _, _ in groups_data:
        base_date = datetime(2024, 7, 20)
        for day in range(3):
            for minute in range(0, 1440, 30):
                ts = base_date + timedelta(days=day, minutes=minute)
                hour = ts.hour + ts.minute / 60
                ppfd = max(0, 2000 * np.sin((hour - 6) * np.pi / 12)) * np.random.uniform(0.8, 1.2)
                ta = 15 + 10 * np.sin((hour - 9) * np.pi / 12) + np.random.uniform(-1, 1)
                data_to_insert.append((
                    group_id, ts.strftime("%Y-%m-%d %H:%M:%S"),
                    ppfd, 0 if np.random.random() > 0.1 else 1,
                    ta, 0 if np.random.random() > 0.2 else 2,
                    ta - 2  # Sample TS data
                ))
    
    cursor.executemany(f'INSERT INTO {config["DATA_TABLE"]} VALUES (?, ?, ?, ?, ?, ?, ?)', data_to_insert)
    
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


def get_all_variables(config: dict) -> list:
    """
    Gets the list of measurement variables from the data table.
    """
    with sqlite3.connect(config["DB_PATH"]) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({config['DATA_TABLE']})")
        columns = [row[1] for row in cursor.fetchall()]
    
    metadata_cols = {'group_id', 'TIMESTAMP'}
    variables = [col for col in columns if col not in metadata_cols and not col.endswith('_flag')]
    return variables

# def filter_variables(variables: list, selection_rules: dict) -> list:
    # """
    # Filters variables based on a specific pattern (PREFIX_H_V_R) and exact names.
    # """
    # if not selection_rules or (not selection_rules.get("prefixes") and not selection_rules.get("exact_names")):
        # return variables
    # prefixes = selection_rules.get("prefixes", [])
    # exact_names = selection_rules.get("exact_names", [])
    # matched_variables = set()
    # if prefixes:
        # for var in variables:
            # for prefix in prefixes:
                # if re.compile(f"^{prefix}_\\d{{1,2}}_\\d{{1,2}}_\\d{{1,2}}$").match(var):
                    # matched_variables.add(var)
                    # break 
    # if exact_names:
        # all_vars_set = set(variables)
        # for name in exact_names:
            # if name in all_vars_set:
                # matched_variables.add(name)
    # return sorted(list(matched_variables))
    
def filter_variables(variables: list, selection_rules: dict) -> list:
    """
    Filters variables based on prefixes and exact names.
    """
    if not selection_rules or (not selection_rules.get("prefixes") and not selection_rules.get("exact_names")):
        return variables
    prefixes = selection_rules.get("prefixes", [])
    exact_names = selection_rules.get("exact_names", [])
    matched_variables = set()

    # Use a more flexible 'startswith' check instead of a strict regex
    if prefixes:
        for var in variables:
            for prefix in prefixes:
                if var.startswith(prefix):
                    matched_variables.add(var)
                    break # Move to the next variable once a match is found
    
    if exact_names:
        all_vars_set = set(variables)
        for name in exact_names:
            if name in all_vars_set:
                matched_variables.add(name)
    
    return sorted(list(matched_variables))
    

def potential_radiation(date_time_index: pd.DatetimeIndex, latitude: float, longitude: float) -> pd.DataFrame:
    """
    Calculates potential solar radiation based on timestamp and location.
    """
    if not isinstance(date_time_index, pd.DatetimeIndex):
        raise TypeError("Input must be a pandas DatetimeIndex.")
    doy = date_time_index.dayofyear
    tv_hh = date_time_index.hour + date_time_index.minute / 60 + date_time_index.second / 3600
    lat_rad = np.deg2rad(latitude)
    declination = np.deg2rad(-23.44) * np.cos(np.deg2rad(360 / 365 * (doy + 10)))
    hour_angle = np.pi * (tv_hh - 12) / 12 - np.deg2rad(longitude)
    sin_psi = (np.sin(lat_rad) * np.sin(declination) + 
               np.cos(lat_rad) * np.cos(declination) * np.cos(hour_angle))
    radiation = np.zeros_like(sin_psi, dtype=float)
    S = 1370
    mask = sin_psi > 0
    transmissivity = 0.6 + 0.2 * sin_psi[mask]
    radiation[mask] = S * transmissivity * sin_psi[mask]
    return pd.DataFrame({'Radiation': radiation}, index=date_time_index)


# --- MAIN PROCESSING FUNCTION ---
def generate_reports():
    """Main function to orchestrate the report generation process."""
    create_dummy_database(CONFIG)
    os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)
    
    try:
        groups = get_group_info(CONFIG)
        if not groups:
            logging.error("No group information found in the database.")
            return
        all_vars = get_all_variables(CONFIG)
        vars_to_process = filter_variables(all_vars, CONFIG.get("VARIABLE_SELECTION"))
    except Exception as e:
        logging.error(f"Could not fetch metadata from database. Error: {e}")
        return

    group_pbar = tqdm(groups.items(), desc="Processing Groups")
    for group_id, group_meta in group_pbar:
        group_pbar.set_postfix_str(group_id)
        
        variable_pbar = tqdm(vars_to_process, desc=f"Variables for {group_id}", leave=False)
        for var_name in variable_pbar:
            variable_pbar.set_postfix_str(var_name)
            flag_name = f"{var_name}_flag"
            
            try:
                with sqlite3.connect(CONFIG["DB_PATH"]) as conn:
                    # The query now needs to select the flag column conditionally
                    # We first check which columns exist in the data table
                    cursor = conn.cursor()
                    cursor.execute(f"PRAGMA table_info({CONFIG['DATA_TABLE']})")
                    available_cols = [row[1] for row in cursor.fetchall()]
                    
                    cols_to_select = ['TIMESTAMP', f'"{var_name}"']
                    if flag_name in available_cols:
                        cols_to_select.append(f'"{flag_name}"')

                    query = f'SELECT {", ".join(cols_to_select)} FROM {CONFIG["DATA_TABLE"]} WHERE group_id = ?'
                    df = pd.read_sql_query(query, conn, params=(group_id,))

                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.tz_localize('Etc/GMT-1').dt.tz_convert('Europe/Warsaw')
                df.set_index('TIMESTAMP', inplace=True)
            except Exception as e:
                logging.error(f"Failed to fetch data for {group_id}/{var_name}. Error: {e}")
                continue

            if df.empty or df[var_name].isnull().all():
                logging.warning(f"No data or only nulls for {group_id}/{var_name}. Skipping.")
                continue
            
            has_flag_column = flag_name in df.columns
            if not has_flag_column:
                logging.warning(f"No flag column for '{var_name}'. Plotting all data in one color.")
            
            pdf_filename = f"{group_id}_{var_name}.pdf"
            pdf_path = os.path.join(CONFIG["OUTPUT_DIR"], pdf_filename)
            
            with PdfPages(pdf_path) as pdf:
                daily_groups = [group for _, group in df.groupby(df.index.date)]
                for i in range(0, len(daily_groups), CONFIG["PLOTS_PER_PAGE"]):
                    page_groups = daily_groups[i:i + CONFIG["PLOTS_PER_PAGE"]]
                    fig, axes = plt.subplots(nrows=CONFIG["GRID_LAYOUT"][0], ncols=CONFIG["GRID_LAYOUT"][1], figsize=(8.27, 11.69), constrained_layout=True)
                    fig.suptitle(f"Group: {group_id} - Variable: {var_name}", fontsize=16)
                    axes_flat = axes.flatten()

                    for j, day_df in enumerate(page_groups):
                        ax = axes_flat[j]
                        day_date = day_df.index[0].date()
                        
                        if has_flag_column:
                            good_data = day_df[day_df[flag_name] == 0]
                            bad_data = day_df[day_df[flag_name] != 0]
                            if not good_data.empty:
                                ax.plot(good_data.index, good_data[var_name], 'b-', marker='.', markersize=3, label='Flag 0 (Good)')
                            if not bad_data.empty:
                                ax.plot(bad_data.index, bad_data[var_name], 'r.', markersize=4, label='Other Flags (Bad)')
                        else:
                            if not day_df.empty:
                                ax.plot(day_df.index, day_df[var_name], 'b-', marker='.', markersize=3, label='Data (no flag)')
                        
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
                        if j == 0:
                            ax.legend(fontsize=7)

                    for j in range(len(page_groups), len(axes_flat)):
                        axes_flat[j].set_visible(False)
                        
                    pdf.savefig(fig)
                    plt.close(fig)
            logging.info(f"Generated report: {pdf_path}")

    logging.info("Processing finished successfully.")

# --- SCRIPT EXECUTION ---
if __name__ == '__main__':
    generate_reports()
