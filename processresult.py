import json
from packaging import version

def parse_version(path):
    parts = path.split('/')
    if len(parts) >= 2:
        ver_part = parts[-2].split('-')[0]  # Get the version part before any dash
        try:
            return version.parse(ver_part)
        except version.InvalidVersion:
            return None
    return None

def filter_versions(data, min_version):
    min_ver = version.parse(min_version)

    def filter_list(items):
        return [item for item in items if isinstance(item, str) and
                (parsed_ver := parse_version(item)) is not None and parsed_ver >= min_ver]

    def filter_dict(items):
        return [item for item in items if
                (parsed_ver := parse_version(item['key'])) is not None and parsed_ver >= min_ver]

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                if key in ['matching', 'mismatched', 'only_in_s3', 'only_in_ssm']:
                    data[key] = filter_list(value)
                elif key == 'wrong_owner':
                    data[key] = filter_dict(value)
            elif isinstance(value, dict):
                filter_versions(value, min_version)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                filter_versions(item, min_version)

def main():
    input_file = "comparison_results.json"
    output_file = "filtered_comparison_results.json"
    min_version = "1.14.3"

    # Load the comparison results
    with open(input_file, 'r') as f:
        comparison_results = json.load(f)

    # Filter the results
    filter_versions(comparison_results, min_version)

    # Save the filtered results
    with open(output_file, 'w') as f:
        json.dump(comparison_results, f, indent=2)

    print(f"Filtered results have been saved to {output_file}")

if __name__ == "__main__":
    main()
