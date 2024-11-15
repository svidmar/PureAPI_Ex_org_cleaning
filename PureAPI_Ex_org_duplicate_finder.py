import requests
from collections import defaultdict
from getpass import getpass
import csv
import time
import sys

def get_api_data(base_url, api_key):
    headers = {
        "accept": "application/json",
        "api-key": api_key
    }
    all_items = []
    offset = 0
    size = 100  

    # Initial request to get total count
    response = requests.get(f"https://{base_url}/ws/api/external-organizations?offset={offset}&size={size}", headers=headers)
    response.raise_for_status()
    data = response.json()
    total_count = data['count']
    all_items.extend(data['items'])
    offset += size

    # Small delay to prevent hammering the API too much
    time.sleep(1)  # Adjust the delay if needed

    # Fetch the remaining pages
    while offset < total_count:
        response = requests.get(f"https://{base_url}/ws/api/external-organizations?offset={offset}&size={size}", headers=headers)
        
        # Check for server response and handle errors
        if response.status_code == 500:
            print("Server error (500). Retrying after delay...")
            time.sleep(5)  # Longer delay for retry on server error
            continue
        
        response.raise_for_status()
        data = response.json()
        
        # Add items to the list
        all_items.extend(data['items'])
        offset += size

        # Update progress
        progress = (len(all_items) / total_count) * 100
        sys.stdout.write(f"\rFetching data... {progress:.2f}% complete")
        sys.stdout.flush()

        # Small delay to prevent rate limiting
        time.sleep(1)

    print("\nData fetching complete.")
    return all_items

def get_organization_name(item):
    # Check for organization name in 'en_GB' and 'da_DK' and prioritize 'en_GB'
    if 'en_GB' in item['name']:
        return item['name']['en_GB']
    elif 'da_DK' in item['name']:
        return item['name']['da_DK']
    return None

def get_country(item):
    # Safely retrieve country name if address and country are available
    return item.get('address', {}).get('country', {}).get('term', {}).get('en_GB')

def get_type(item):
    # Safely retrieve the type term if available
    return item.get('type', {}).get('term', {}).get('en_GB')

def find_duplicates(data):
    # Organize items by name, country, and type
    org_info = defaultdict(list)
    
    for item in data:
        name = get_organization_name(item)
        country = get_country(item)
        org_type = get_type(item)  # Include type in the grouping key
        if name and country and org_type:
            org_info[(name, country, org_type)].append(item)
    
    # Filter to only include entries with duplicates
    duplicates = {k: v for k, v in org_info.items() if len(v) > 1}
    return duplicates

def save_to_csv(duplicates):
    with open('duplicate_organizations.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Organization Name", "Country", "Type", "UUIDs", "Count", "Merge Candidate"])
        
        for (name, country, org_type), items in duplicates.items():
            uuids = [item['uuid'] for item in items]
            count = len(items)
            # Find the UUID of the item with 'approved' status
            merge_candidate_uuid = next((item['uuid'] for item in items if item.get('workflow', {}).get('step') == 'approved'), "")
            writer.writerow([name, country, org_type, ", ".join(uuids), count, merge_candidate_uuid])

def main():
    base_url = input("Enter the base URL (e.g., xyz.elsevierpure.com): ")
    api_key = getpass("Enter your API key: ")

    try:
        data = get_api_data(base_url, api_key)
        duplicates = find_duplicates(data)

        if duplicates:
            save_to_csv(duplicates)
            print("Duplicate organizations have been saved to 'duplicate_organizations.csv'.")
        else:
            print("No duplicate organizations found.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    main()
