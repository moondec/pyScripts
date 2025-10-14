# convert_config.py

import json
import ast
import re
from datetime import datetime, timedelta

def convert_py_to_json(py_file_path, json_file_path):
    """
    Converts a Python configuration file with variables into a JSON file.

    This function reads a Python file, extracts global variables (in uppercase),
    and saves them to a JSON file. It specifically handles datetime and
    timedelta objects by converting them to strings, which JSON can serialize.

    Args:
        py_file_path (str): The path to the input Python configuration file.
        json_file_path (str): The path to the output JSON file.
    """
    # 1. Read the Python file content
    try:
        with open(py_file_path, 'r', encoding='utf-8') as f:
            py_content = f.read()
    except FileNotFoundError:
        print(f"Error: The file '{py_file_path}' was not found.")
        return

    # 2. Extract global variables from the Python content
    # We use a custom parser to handle the 'from datetime import ...' line and other special cases
    # to extract a dictionary of variables without executing the file in a dangerous way.
    parsed_tree = ast.parse(py_content)
    globals_dict = {}
    for node in parsed_tree.body:
        if isinstance(node, (ast.Assign, ast.Expr)):
            if isinstance(node.targets[0], ast.Name) and node.targets[0].id.isupper():
                # Extract variable name (e.g., 'COLUMNS_TO_EXCLUDE_FROM_CSV')
                var_name = node.targets[0].id

                # Create a temporary environment to safely evaluate the variable's value
                temp_env = {'datetime': datetime, 'timedelta': timedelta, 'json': json, 're': re}
                
                # We need to evaluate the value, which can be complex. The safest way is to
                # compile and execute just the assignment line within our environment.
                try:
                    exec(ast.unparse(node), temp_env, temp_env)
                    var_value = temp_env[var_name]
                    globals_dict[var_name] = var_value
                except Exception as e:
                    print(f"Error evaluating variable '{var_name}': {e}")

    # 3. Handle special object types (like datetime) for JSON serialization
    # The `json` module cannot serialize datetime objects directly.
    def default_serializer(obj):
        if isinstance(obj, (datetime, timedelta)):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    # 4. Write the variables to a JSON file
    try:
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(globals_dict, f, indent=2, default=default_serializer, ensure_ascii=False)
        print(f"Successfully converted '{py_file_path}' to '{json_file_path}'.")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")

# --- KORZYSTANIE Z FUNKCJI ---
if __name__ == '__main__':
    # Upewnij się, że te ścieżki są poprawne w Twoim środowisku
    python_config_file = 'config.py'
    json_output_file = 'config.json'
    convert_py_to_json(python_config_file, json_output_file)