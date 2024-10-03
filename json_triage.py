import json
from collections import defaultdict
from typing import Dict, List, Tuple
import os

def iterate_json_files(directory: str):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            yield (filename, file_path)

def analyze_json_files(directory: str):
    field_frequency = defaultdict(int)
    missing_fields = defaultdict(list)
    extra_fields = defaultdict(list)
    all_fields = set()
    file_structures = defaultdict(list)

    for file_name, file_path in iterate_json_files(directory):
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        fields = set(data.keys())
        all_fields.update(fields)
        
        # Field frequency analysis
        for field in fields:
            field_frequency[field] += 1
        
        # Store file structure for grouping
        structure_key = tuple(sorted(fields))
        file_structures[structure_key].append(file_name)

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

    return field_frequency, missing_fields, extra_fields, file_structures

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

def main(directory: str, report_directory: str):
    field_frequency, missing_fields, extra_fields, file_structures = analyze_json_files(directory)
    report = generate_summary_report(field_frequency, missing_fields, extra_fields, file_structures)
    
    print(report)

    # Create the reports directory if it doesn't exist
    os.makedirs(report_directory, exist_ok=True)

    # Save the report to a file in the specified directory
    report_file_path = os.path.join(report_directory, 'json_analysis_report.txt')
    with open(report_file_path, 'w') as f:
        f.write(report)
    
    print(f"Report saved to: {report_file_path}")

if __name__ == "__main__":
    # Specify the directory containing the JSON files
    json_directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\json_test_files"
    
    # Specify the directory where the report should be saved
    report_directory = r"C:\Users\kroy2\Documents\python\projects\json_processor\reports"
    
    # Call the main function with the specified directories
    main(json_directory, report_directory)