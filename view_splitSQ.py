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
import argparse
from pathlib import Path

# --- DYNAMIC IMPORT OF THE CONFIGURATION FILE ---
try:
    import config
except ImportError:
    print("Error: config.py not found. Make sure it's in the same directory or accessible in PYTHONPATH.")
    exit()

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ReportGenerator:
    """
    A class to handle the generation of data visualization reports
    by integrating with a central configuration file (config.py).
    """
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.plots_per_page = 6
        self.grid_layout = (3, 2)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at the specified path: {self.db_path}")
        
        self.output_dir.mkdir(exist_ok=True)
        
        self.groups_meta = self._get_group_info_from_config()
        self.table_to_vars_map = self._get_table_to_variables_map()

    def _get_group_info_from_config(self) -> dict:
        """
        Loads group and station metadata from config.py.
        FIXED: This version is more robust. If an exact match for a group isn't found,
        it tries to find any key in STATION_COORDINATES that starts with the same
        station prefix (e.g., 'TU_') to get the coordinates.
        """
        logging.info("Loading group and station information from config.py...")
        info = {}
        all_coord_keys = list(config.STATION_COORDINATES.keys())

        for group_id in config.FILE_ID_MERGE_GROUPS.keys():
            station_coords = config.STATION_COORDINATES.get(group_id)
            
            # If no direct match, try to find a related key
            if not station_coords:
                prefix = group_id.split('_')[0] + '_'
                # Find the first key in the coordinates dict that matches the prefix
                matching_key = next((key for key in all_coord_keys if key.startswith(prefix)), None)
                if matching_key:
                    station_coords = config.STATION_COORDINATES.get(matching_key)

            if station_coords:
                info[group_id] = {
                    'lat': station_coords['lat'],
                    'lon': station_coords['lon']
                }
            else:
                # This warning will now only appear if no coordinate key matches the prefix
                logging.warning(f"Could not find coordinates for any group with prefix like '{group_id.split('_')[0]}'.")
        return info

    def _get_table_to_variables_map(self) -> dict:
        """
        Scans all 'data_*' tables in the SQLite database and maps table names
        to the list of variables they contain (excluding flag columns).
        """
        logging.info("Scanning database for data tables and variables...")
        table_map = {}
        with sqlite3.connect(self.db_path) as conn:
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

    def _create_processing_tasks(self, selected_group_patterns: list, var_patterns: list, all_vars: bool) -> list:
        """
        Creates a definitive list of tasks based on user command-line selections.
        FIXED: This version correctly handles wildcards (*) in group selection patterns.
        """
        tasks = []
        
        # Determine which groups to target based on the provided patterns
        target_group_ids = set()
        if not selected_group_patterns:
             # If no groups are specified, target all available groups
            target_group_ids = set(self.groups_meta.keys())
        else:
            # Match patterns against all available group IDs
            for pattern in selected_group_patterns:
                regex_pattern = re.compile(pattern.replace('*', '.*'), re.IGNORECASE)
                for group_id in self.groups_meta.keys():
                    if regex_pattern.match(group_id):
                        target_group_ids.add(group_id)

        for table_name, variables_in_table in self.table_to_vars_map.items():
            current_group_id = table_name.replace("data_", "")
            
            if current_group_id not in target_group_ids:
                continue

            group_meta = self.groups_meta.get(current_group_id)
            if not group_meta:
                logging.warning(f"Could not find metadata for group '{current_group_id}'. Skipping table '{table_name}'.")
                continue
                
            selected_vars_for_table = []
            if all_vars:
                selected_vars_for_table = variables_in_table
            elif var_patterns:
                for pattern in var_patterns:
                    regex_pattern = re.compile(pattern.replace('*', '.*'), re.IGNORECASE)
                    selected_vars_for_table.extend([var for var in variables_in_table if regex_pattern.match(var)])
            
            for var_name in sorted(list(set(selected_vars_for_table))):
                tasks.append((current_group_id, group_meta, var_name, table_name))
        
        return tasks

    def _process_single_task(self, task_info: tuple):
        """
        Worker function to process a single variable and generate its PDF report.
        """
        group_id, group_meta, var_name, table_name = task_info
        try:
            flag_name = f"{var_name}_flag"
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                available_cols = [row[1] for row in cursor.fetchall()]
                
                cols_to_select = ['TIMESTAMP', f'"{var_name}"']
                if flag_name in available_cols:
                    cols_to_select.append(f'"{flag_name}"')

                query = f'SELECT {", ".join(cols_to_select)} FROM "{table_name}"'
                df = pd.read_sql_query(query, conn, index_col='TIMESTAMP', parse_dates=['TIMESTAMP'])

            if df.empty or df[var_name].isnull().all():
                return f"Skipped: No data for {group_id}/{var_name} in {table_name}"
            
            has_flag_column = flag_name in df.columns
            
            valid_range = None
            for prefix, limits in config.VALUE_RANGE_FLAGS.items():
                if var_name.startswith(prefix):
                    valid_range = limits
                    break
            
            pdf_filename = f"{group_id}_{var_name}_from_{table_name}.pdf"
            pdf_path = self.output_dir / pdf_filename
            
            with PdfPages(pdf_path) as pdf:
                daily_groups = [group for _, group in df.groupby(df.index.date)]
                for i in range(0, len(daily_groups), self.plots_per_page):
                    page_groups = daily_groups[i:i + self.plots_per_page]
                    fig, axes = plt.subplots(
                        nrows=self.grid_layout[0], ncols=self.grid_layout[1], 
                        figsize=(8.27, 11.69), constrained_layout=True
                    )
                    fig.suptitle(f"Group: {group_id} | Variable: {var_name}\n(Source Table: {table_name})", fontsize=14)
                    axes_flat = axes.flatten()

                    for j, day_df in enumerate(page_groups):
                        ax = axes_flat[j]
                        day_date = day_df.index[0].date()
                        
                        if has_flag_column:
                            good_data = day_df[day_df[flag_name] == 0]
                            bad_data = day_df[day_df[flag_name] != 0]
                            if not good_data.empty: ax.plot(good_data.index, good_data[var_name], 'b-', marker='.', markersize=3, label='Flag 0 (Good)')
                            if not bad_data.empty: ax.plot(bad_data.index, bad_data[var_name], 'r.', markersize=4, label='Other Flags')
                        else:
                            if not day_df.empty: ax.plot(day_df.index, day_df[var_name], 'b-', marker='.', markersize=3, label='Data (no flag)')
                        
                        if var_name.startswith(('PPFD', 'SW_IN')):
                            rad_df = self._potential_radiation(day_df.index, group_meta['lat'], group_meta['lon'])
                            ax.plot(rad_df.index, rad_df['Radiation'], 'g--', linewidth=1.2, label='Potential Rad.')
                        
                        if valid_range:
                            ax.axhspan(valid_range['min'], valid_range['max'], color='green', alpha=0.1, zorder=0, label='Valid Range')

                        ax.set_title(day_date.strftime('%Y-%m-%d'), fontsize=10)
                        ax.set_ylabel(var_name, fontsize=8)
                        ax.grid(True, linestyle='--', alpha=0.6)
                        
                        data_for_ylim = good_data if has_flag_column and not good_data.empty else day_df
                        if data_for_ylim[var_name].notna().any():
                            ymin, ymax = data_for_ylim[var_name].min(), data_for_ylim[var_name].max()
                            margin = (ymax - ymin) * 0.1 if (ymax - ymin) > 0 else 1
                            ax.set_ylim(ymin - margin, ymax + margin)
                        
                        start_of_day = datetime.combine(day_date, datetime.min.time())
                        ax.set_xlim(start_of_day, start_of_day + timedelta(days=1))
                        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                        plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=8)
                        if j == 0: ax.legend(fontsize=7)

                    for k in range(len(page_groups), len(axes_flat)):
                        axes_flat[k].set_visible(False)
                        
                    pdf.savefig(fig)
                    plt.close(fig)
            return f"OK: Generated report for {group_id}/{var_name} from table {table_name}"
        except Exception as e:
            logging.error(f"Error processing task {task_info}: {e}", exc_info=True)
            return f"ERROR: Failed {group_id}/{var_name} from {table_name} with error: {e}"

    def _potential_radiation(self, date_time_index: pd.DatetimeIndex, latitude: float, longitude: float) -> pd.DataFrame:
        """
        Calculates potential solar radiation, simplified to peak at 12:00 local time.
        """
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

    def run(self, selected_groups: list, var_patterns: list, all_vars: bool):
        """
        Main method to orchestrate the report generation process.
        """
        try:
            tasks = self._create_processing_tasks(selected_groups, var_patterns, all_vars)
            logging.info(f"Created {len(tasks)} processing tasks.")
            if not tasks:
                logging.warning("No tasks created. Check your group/variable selections or patterns.")
                return

            num_processes = max(1, cpu_count() - 1)
            logging.info(f"Starting parallel processing with {num_processes} workers...")

            with Pool(processes=num_processes) as pool:
                results = list(tqdm(pool.imap_unordered(self._process_single_task, tasks), total=len(tasks), desc="Generating PDFs"))

            for res in results:
                if "ERROR" in res:
                    logging.error(res)
                elif "Skipped" in res:
                    logging.debug(res)
                else:
                    logging.info(res.replace("OK: ", ""))

            logging.info("Processing finished successfully.")

        except Exception as e:
            logging.error(f"A critical error occurred: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Generate PDF reports for meteorological data stored in an SQLite database.")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the SQLite database file (e.g., E:\\test_split\\metLab.db).")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save the generated PDF files (e.g., E:\\pdfy\\view_splitSQ).")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--vars", nargs='+', help="A list of variable patterns to plot (e.g., --vars 'PPFD_IN_*' 'SW_IN_*' TS_1_1_1).")
    group.add_argument("--all-vars", action='store_true', help="Generate plots for all available variables in the selected groups.")
    
    parser.add_argument("--groups", nargs='+', help="A list of group_id patterns to process (e.g., --groups TU_MET_* ME_*). If not provided, all groups are processed.")

    args = parser.parse_args()

    try:
        generator = ReportGenerator(db_path=args.db_path, output_dir=args.output_dir)
        generator.run(selected_groups=args.groups, var_patterns=args.vars, all_vars=args.all_vars)
    except FileNotFoundError as e:
        logging.error(e)
    except Exception as e:
        logging.error(f"An unexpected error occurred during initialization: {e}", exc_info=True)

if __name__ == '__main__':
    main()