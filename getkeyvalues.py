import boto3
import json
from botocore.exceptions import ClientError
import subprocess

def get_all_tags():
    try:
        cmd = ["gh", "api", "repos/bottlerocket-os/bottlerocket/tags", "--paginate"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        all_tags = json.loads(result.stdout)
        return all_tags
    except subprocess.CalledProcessError as e:
        print(f"Error fetching tags using GitHub CLI: {e}")
        print(f"stderr: {e.stderr}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing GitHub CLI output: {e}")
        return []

def check_commit_folder_exists(s3_client, bucket_name, commit_sha):
    prefix = f"builds/{commit_sha}/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        if 'CommonPrefixes' in response:
            return True
        return 'Contents' in response
    except ClientError as e:
        print(f"Error checking folder existence: {e}")
        return False

def get_commit_sha_from_tag(tag_data):
    return tag_data.get('commit', {}).get('sha')

def check_commit_folder_exists(s3_client, bucket_name, commit_sha):
    prefix = f"builds/{commit_sha}/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=1
        )
        return 'Contents' in response
    except ClientError as e:
        print(f"Error checking folder existence: {e}")
        return False

def find_root_path(s3_client, bucket_name, commit_sha):
    prefix = f"builds/{commit_sha}/"
    try:
        while True:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            if 'CommonPrefixes' in response:
                if any(p['Prefix'].endswith('root/') for p in response['CommonPrefixes']):
                    return next(p['Prefix'] for p in response['CommonPrefixes'] if p['Prefix'].endswith('root/'))
                prefix = response['CommonPrefixes'][0]['Prefix']
            else:
                return None
    except ClientError as e:
        print(f"Error finding root path: {e}")
        return None

def list_variant_folders(s3_client, bucket_name, commit_sha):
    root_path = find_root_path(s3_client, bucket_name, commit_sha)
    if not root_path:
        print(f"Could not find root path for commit {commit_sha}")
        return []

    prefix = f"{root_path}bottlerocket_code_repo/build/images/"
    variant_folders = set()

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

        for page in pages:
            if 'CommonPrefixes' in page:
                for prefix_obj in page['CommonPrefixes']:
                    folder_path = prefix_obj['Prefix']
                    variant = folder_path.split('/')[-2]
                    variant_folders.add(variant)

        return list(variant_folders)
    except ClientError as e:
        print(f"Error listing variant folders: {e}")
        return []

def find_ami_json(s3_client, bucket_name, commit_sha, variant, version):
    root_path = find_root_path(s3_client, bucket_name, commit_sha)
    if not root_path:
        print(f"Could not find root path for commit {commit_sha}")
        return None

    prefix = f"{root_path}bottlerocket_code_repo/build/images/{variant}/"

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('-amis.json'):
                        return key

        return None
    except ClientError as e:
        print(f"Error finding AMI JSON: {e}")
        return None

def get_ssm_parameter_name(ami_name, include_commit=True):
    parts = ami_name.split('-')
    if len(parts) < 7:
        return None

    # Ensure the variant starts with "aws-"
    if parts[2] != "aws":
        parts.insert(2, "aws")

    # Handle NVIDIA variants
    if 'nvidia' in parts:
        nvidia_index = parts.index('nvidia')
        variant = '-'.join(parts[2:nvidia_index+1])  # e.g., "aws-k8s-1.24-nvidia"
        arch_index = nvidia_index + 1
    elif 'fips' in parts:
        fips_index = parts.index('fips')
        variant = '-'.join(parts[2:fips_index+1])  # e.g., "aws-k8s-1.24-fips"
        arch_index = fips_index + 1
    else:
        variant = '-'.join(parts[2:5])  # e.g., "aws-k8s-1.24" or "aws-ecs-1"
        arch_index = 5

    # Convert architecture
    arch = parts[arch_index]
    if arch == 'aarch64':
        arch = 'arm64'

    # Extract version and commit SHA
    version_parts = [p for p in parts[arch_index+1:] if p != 'v1']
    if len(version_parts) >= 2:
        version = version_parts[-2]
        commit_sha = version_parts[-1][:8]  # Get the first 8 characters of the commit SHA
    else:
        # Handle cases where version might be missing
        version = "unknown"
        commit_sha = version_parts[-1][:8] if version_parts else "unknown"

    # Remove 'v' prefix from version if present
    if version.startswith('v'):
        version = version[1:]

    # Choose between full version (with commit SHA) or just version
    if include_commit:
        if version != commit_sha:
            version_string = f"{version}-{commit_sha}"
        else:
            version_string = version
    else:
        version_string = version

    return f"aws/service/bottlerocket/{variant}/{arch}/{version_string}/image_id"

def process_ami_json(s3_client, bucket_name, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        ami_data = json.loads(file_content)

        simplified_ami_data = {}
        for region, ami_info in ami_data.items():
            if region not in simplified_ami_data:
                simplified_ami_data[region] = {}

            # Get both parameter paths (with and without commit SHA)
            ssm_param_name_full = get_ssm_parameter_name(ami_info.get('name', ''), include_commit=True)
            ssm_param_name_version = get_ssm_parameter_name(ami_info.get('name', ''), include_commit=False)

            # Add both parameter paths pointing to the same AMI ID
            if ssm_param_name_full:
                simplified_ami_data[region][ssm_param_name_full] = ami_info.get('id')
            if ssm_param_name_version:
                simplified_ami_data[region][ssm_param_name_version] = ami_info.get('id')

        return simplified_ami_data
    except ClientError as e:
        print(f"Error processing AMI JSON file {file_key}: {e}")
        return None

def process_tag(bucket_name, tag_data):
    s3_client = boto3.client('s3')
    tag_name = tag_data['name']
    commit_sha = get_commit_sha_from_tag(tag_data)

    if not commit_sha:
        print(f"Could not get commit SHA for tag {tag_name}")
        return None

    print(f"\nProcessing tag {tag_name} (commit: {commit_sha})")

    results = {}

    variants = list_variant_folders(s3_client, bucket_name, commit_sha)

    for variant in variants:
        print(f"  Processing variant: {variant}")

        ami_json_key = find_ami_json(s3_client, bucket_name, commit_sha, variant, tag_name)

        if ami_json_key:
            ami_data = process_ami_json(s3_client, bucket_name, ami_json_key)
            if ami_data:
                # Merge the AMI data into results by region
                for region, region_data in ami_data.items():
                    if region not in results:
                        results[region] = {}
                    results[region].update(region_data)

    return results

def merge_results(all_results):
    """Merge results from all tags by region"""
    final_results = {}

    for tag_result in all_results:
        if tag_result:  # Skip None results
            for region, region_data in tag_result.items():
                if region not in final_results:
                    final_results[region] = {}
                final_results[region].update(region_data)

    return final_results

def main():
    bucket_name = "bottlerocket-launchsys-n-launchsystembucketcafdfa-1k615o8vel9n7"

    print("Fetching tags from GitHub...")
    tags = get_all_tags()
    print(f"Found {len(tags)} tags")

    s3_client = boto3.client('s3')
    all_results = []

    for tag in tags:
        tag_name = tag['name']
        commit_sha = tag['commit']['sha']

        if check_commit_folder_exists(s3_client, bucket_name, commit_sha):
            result = process_tag(bucket_name, tag)
            if result:
                all_results.append(result)

    # Merge all results by region
    final_results = merge_results(all_results)

    # Count total entries
    total_entries = sum(len(region_data) for region_data in final_results.values())
    print(f"\nTotal regions: {len(final_results)}")
    print(f"Total key-value pairs: {total_entries}")

    # Save the final results
    with open("bottlerocket_ami_ssm_mapping.json", 'w') as f:
        json.dump(final_results, f, indent=2)

    print("\nProcessing complete. Results written to bottlerocket_ami_ssm_mapping.json")

if __name__ == "__main__":
    main()
