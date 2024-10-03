import json
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Union
import os
import pandas as pd
from datetime import datetime


def iterate_json_files(directory: str):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            yield (filename, file_path)

def flatten_json(data: Union[Dict[str, Any], List[Any]], prefix: str = '') -> Dict[str, Any]:
    flattened = {}
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flattened.update(flatten_json(value, new_key))
            elif isinstance(value, list):
                flattened[new_key] = f"Array[{len(value)}]"
            else:
                flattened[new_key] = value
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{prefix}[{i}]" if prefix else f"[{i}]"
            if isinstance(item, dict):
                flattened.update(flatten_json(item, new_key))
            elif isinstance(item, list):
                flattened[new_key] = f"Array[{len(item)}]"
            else:
                flattened[new_key] = item
    else:
        flattened[prefix] = data
    return flattened

def analyze_json_files(directory: str):
    field_frequency = defaultdict(int)
    missing_fields = defaultdict(list)
    extra_fields = defaultdict(list)
    all_fields = set()
    file_structures = defaultdict(list)
    file_data = defaultdict(list)

    for file_name, file_path in iterate_json_files(directory):
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        flattened_data = flatten_json(data)
        fields = set(flattened_data.keys())
        all_fields.update(fields)
        
        # Field frequency analysis
        for field in fields:
            field_frequency[field] += 1
        
        # Store file structure for grouping
        structure_key = tuple(sorted(fields))
        file_structures[structure_key].append(file_name)
        file_data[structure_key].append((file_name, file_path, flattened_data))

    total_files = sum(len(files) for files in file_structures.values())
    common_fields = set(field for field, count in field_frequency.items() if count == total_files)

    # Missing and extra field detection
    for structure, files in file_structures.items():
        missing = common_fields - set(structure)
        extra = set(structure) - common_fields
        for file in files:
            if missing:
                missing_fields[file].extend(missing)
            if extra:
                extra_fields[file].extend(extra)

    return field_frequency, missing_fields, extra_fields, file_structures, file_data

def generate_summary_report(field_frequency: Dict[str, int], 
                            missing_fields: Dict[str, List[str]], 
                            extra_fields: Dict[str, List[str]], 
                            file_structures: Dict[Tuple[str], List[str]]) -> str:
    total_files = sum(len(files) for files in file_structures.values())
    report = f"JSON Files Analysis Report\n"
    report += f"Total files analyzed: {total_files}\n\n"

    report += "Field Frequency:\n"
    for field, count in sorted(field_frequency.items(), key=lambda x: x[1], reverse=True):
        report += f"  {field}: {count} ({count/total_files*100:.2f}%)\n"

    report += "\nMissing Fields:\n"
    for file, fields in missing_fields.items():
        report += f"  {file}: {', '.join(fields)}\n"

    report += "\nExtra Fields:\n"
    for file, fields in extra_fields.items():
        report += f"  {file}: {', '.join(fields)}\n"

    report += "\nFile Grouping:\n"
    for i, (structure, files) in enumerate(file_structures.items(), 1):
        report += f"  Group {i} ({len(files)} files):\n"
        report += f"    Fields: {', '.join(structure)}\n"
        report += f"    Files: {', '.join(files[:5])}{'...' if len(files) > 5 else ''}\n"

    return report

def create_dataframes(file_data: Dict[Tuple[str], List[Tuple[str, str, Dict]]]) -> Dict[int, pd.DataFrame]:
    dataframes = {}
    for i, (structure, files) in enumerate(file_data.items(), 1):
        df_data = []
        for file_name, file_path, data in files:
            row = {
                'file_name': file_name,
                'file_path': file_path,
                'field_names': ', '.join(sorted(data.keys())),
                **data
            }
            df_data.append(row)
        df = pd.DataFrame(df_data)
        # Reorder columns to put field_names after file_path
        columns = ['file_name', 'file_path', 'field_names'] + [col for col in df.columns if col not in ['file_name', 'file_path', 'field_names']]
        df = df[columns]
        dataframes[i] = df
    return dataframes

def main(directory: str, report_directory: str):
    field_frequency, missing_fields, extra_fields, file_structures, file_data = analyze_json_files(directory)
    report = generate_summary_report(field_frequency, missing_fields, extra_fields, file_structures)
    
    print(report)

    # Create the reports directory if it doesn't exist
    os.makedirs(report_directory, exist_ok=True)

    # Generate timestamp for the report file name
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Save the report to a file in the specified directory with timestamp
    report_file_name = f'{timestamp}_json_analysis_report.txt'
    report_file_path = os.path.join(report_directory, report_file_name)
    with open(report_file_path, 'w') as f:
        f.write(report)
    
    print(f"Report saved to: {report_file_path}")

    # Create and save dataframes
    dataframes = create_dataframes(file_data)
    for group_number, df in dataframes.items():
        df_file_path = os.path.join(report_directory, f'group_{group_number}_dataframe.csv')
        df.to_csv(df_file_path, index=False)
        print(f"Group {group_number} DataFrame saved to: {df_file_path}")

    return dataframes  # Return the dataframes for potential future use

if __name__ == "__main__":
    # Specify the directory containing the JSON files
    json_directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\json_test_files2"
    
    # Specify the directory where the report and dataframes should be saved
    report_directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\reports"
    
    # Call the main function with the specified directories
    dataframes = main(json_directory, report_directory)