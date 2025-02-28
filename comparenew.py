import boto3
import json
from botocore.exceptions import ClientError
import subprocess
import argparse
import os

def load_s3_results(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Unable to parse {filename}. Ensure it's valid JSON.")
        return {}

def get_ssm_parameters_for_region(region, use_cache=True):
    """Get SSM parameters for a specific region, using cache if available"""
    cache_file = f"ssm_cache_{region}.json"

    if use_cache and os.path.exists(cache_file):
        print(f"Loading cached SSM parameters for {region}")
        with open(cache_file, 'r') as f:
            return json.load(f)

    print(f"Fetching SSM parameters for {region}")
    try:
        cmd = [
            "aws", "ssm", "get-parameters-by-path",
            "--path", "/aws/service/bottlerocket",
            "--recursive",
            "--region", region
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        response = json.loads(result.stdout)

        parameters = {}
        for param in response.get('Parameters', []):
            if param['Name'].endswith('/image_id'):
                parameters[param['Name'].lstrip('/')] = param['Value']

        while 'NextToken' in response:
            cmd.extend(["--next-token", response['NextToken']])
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            response = json.loads(result.stdout)
            for param in response.get('Parameters', []):
                if param['Name'].endswith('/image_id'):
                    parameters[param['Name'].lstrip('/')] = param['Value']

        # Cache the results
        with open(cache_file, 'w') as f:
            json.dump(parameters, f, indent=2)

        return parameters
    except subprocess.CalledProcessError as e:
        print(f"Error fetching SSM parameters for region {region}: {e}")
        print(f"stderr: {e.stderr}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error parsing AWS CLI output for region {region}: {e}")
        return {}

def compare_region_results(s3_data, ssm_data):
    """Compare results for a specific region"""
    comparison = {
        'matching': [],
        'mismatched': [],
        'only_in_s3': [],
        'only_in_ssm': []
    }

    all_keys = set(s3_data.keys()) | set(ssm_data.keys())

    for key in all_keys:
        if key in s3_data and key in ssm_data:
            if s3_data[key] == ssm_data[key]:
                comparison['matching'].append(key)
            else:
                comparison['mismatched'].append({
                    'key': key,
                    's3_value': s3_data[key],
                    'ssm_value': ssm_data[key]
                })
        elif key in s3_data:
            comparison['only_in_s3'].append(key)
        else:
            comparison['only_in_ssm'].append(key)

    return comparison

def main():
    parser = argparse.ArgumentParser(description="Compare Bottlerocket AMI data from S3 with SSM parameters.")
    parser.add_argument('--regions', nargs='+', help="Specify regions to process (default: all regions in S3 data)")
    parser.add_argument('--no-cache', action='store_true', help="Do not use cached SSM data")
    args = parser.parse_args()

    print("Loading S3 results...")
    s3_results = load_s3_results("bottlerocket_ami_ssm_mapping.json")
    if not s3_results:
        print("Error loading S3 results. Exiting.")
        return

    print(f"Loaded data for {len(s3_results)} regions from S3 results.")

    regions_to_process = args.regions if args.regions else s3_results.keys()

    comparison = {
        'by_region': {},
        'summary': {
            'total_matching': 0,
            'total_mismatched': 0,
            'total_only_in_s3': 0,
            'total_only_in_ssm': 0
        }
    }

    for region in regions_to_process:
        if region not in s3_results:
            print(f"Warning: No S3 data for region {region}. Skipping.")
            continue

        print(f"\nProcessing region: {region}")

        ssm_data = get_ssm_parameters_for_region(region, use_cache=not args.no_cache)
        print(f"Found {len(ssm_data)} SSM parameters in {region}")

        region_comparison = compare_region_results(s3_results[region], ssm_data)
        comparison['by_region'][region] = region_comparison

        comparison['summary']['total_matching'] += len(region_comparison['matching'])
        comparison['summary']['total_mismatched'] += len(region_comparison['mismatched'])
        comparison['summary']['total_only_in_s3'] += len(region_comparison['only_in_s3'])
        comparison['summary']['total_only_in_ssm'] += len(region_comparison['only_in_ssm'])

    with open("comparison_results.json", 'w') as f:
        json.dump(comparison, f, indent=2)

    print("\nOverall comparison summary:")
    print(f"Total matching parameters: {comparison['summary']['total_matching']}")
    print(f"Total mismatched parameters: {comparison['summary']['total_mismatched']}")
    print(f"Total only in S3: {comparison['summary']['total_only_in_s3']}")
    print(f"Total only in SSM: {comparison['summary']['total_only_in_ssm']}")

    print("\nRegion-specific summaries:")
    for region, data in comparison['by_region'].items():
        print(f"\nRegion: {region}")
        print(f"  Matching: {len(data['matching'])}")
        print(f"  Mismatched: {len(data['mismatched'])}")
        print(f"  Only in S3: {len(data['only_in_s3'])}")
        print(f"  Only in SSM: {len(data['only_in_ssm'])}")

        if data['mismatched']:
            print(f"\n  Example mismatch in {region}:")
            print(json.dumps(data['mismatched'][0], indent=2))

    print("\nDetailed comparison results written to comparison_results.json")

if __name__ == "__main__":
    main()
