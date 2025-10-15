# Repository of scripts for processing meteorological data
(c) Laboratory of Meteorology

## Files structure

```         
.
├── arch
│   └── view.py
├── comCSV_MAT2pdf.py
├── config.py
├── environment.yml
├── generate_all_reports.bat
├── logs
│   ├── debug_files_after_sort_ME_CalPlates.txt
│   ├── debug_files_after_sort_ME_DOWN_MET_1min.txt
│   ├── debug_files_after_sort_ME_DOWN_MET_30min.txt
│   ├── debug_files_after_sort_ME_Rain_down.txt
│   ├── debug_files_after_sort_ME_Rain_top.txt
│   ├── debug_files_after_sort_ME_TOP_MET_1min.txt
│   ├── debug_files_after_sort_ME_TOP_MET_30min.txt
│   ├── debug_files_after_sort_TL1a_CalPlates_dT.txt
│   ├── debug_files_after_sort_TL1a_MET_1_dT.txt
│   ├── debug_files_after_sort_TL1a_MET_30_dT.txt
│   ├── debug_files_after_sort_TL1a_Rain_down_dT.txt
│   ├── debug_files_after_sort_TL2_CalPlates_dT.txt
│   ├── debug_files_after_sort_TL2_MET_1_dT.txt
│   ├── debug_files_after_sort_TL2_MET_30_dT.txt
│   ├── log_chronology_correction.txt
│   └── log_split.txt
├── pom1m_example.csv
├── README.md
├── runSplit3.bat
├── unified_script.py
└── view_splitSQ.py
```

## Files description

-   `config.py` - main config file

-   `environment.yml` - anaconda virtual environment file

    -   Use `conda env create -f environment.yml` to create isolated python env
    -   and `conda activate pyScripts` to activate

-   `generate_all_reports.bat` edit path inside and plot diagnostic reports
-   `logs` folder for logs
-   `runSplit3.bat` main batch file for Windows. It use:
-   `unified_script.py` main python script. Reads raw meteo data and processes it.

