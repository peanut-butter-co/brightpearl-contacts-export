import os
import requests
import csv
from dotenv import load_dotenv
import sys
import time
from typing import List, Dict, Any, Optional

# Set default encoding to UTF-8 for Python 2 compatibility
reload(sys)
sys.setdefaultencoding('utf8')

load_dotenv()

BRIGHTPEARL_ACCOUNT = os.getenv('BRIGHTPEARL_ACCOUNT')
BRIGHTPEARL_API_TOKEN = os.getenv('BRIGHTPEARL_API_TOKEN')
BRIGHTPEARL_API_DOMAIN = os.getenv('BRIGHTPEARL_API_DOMAIN')
BRIGHTPEARL_APP_REF = os.getenv('BRIGHTPEARL_APP_REF')

if not all([BRIGHTPEARL_ACCOUNT, BRIGHTPEARL_API_TOKEN, BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_APP_REF]):
    print('Missing one or more required environment variables.')
    exit(1)

BASE_URL = 'https://{}/public-api/{}'.format(BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_ACCOUNT)
HEADERS = {
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'brightpearl-app-ref': BRIGHTPEARL_APP_REF,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

# Constants for rate limiting
REQUEST_DELAY = 0.5  # Half second delay between requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds to wait between retries

def make_request(url, headers, params=None):
    """
    Make a rate-limited request with retries
    """
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)  # Rate limiting delay
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                if attempt < MAX_RETRIES - 1:  # Don't sleep on last attempt
                    print("Rate limit hit, waiting {} seconds...".format(RETRY_DELAY))
                    time.sleep(RETRY_DELAY)
                    continue
            raise
    return None

# --- API Functions ---
def get_tag_id(tag_name):
    url = "{}/contact-service/tag".format(BASE_URL)
    try:
        print("Looking up tag ID for '{}'...".format(tag_name))
        resp = make_request(url, HEADERS)
        if not resp:
            return None
        data = resp.json()
        
        # The response has a nested structure: {'response': {'tagId': {'tagName': '...', ...}}}
        tags = data.get('response', {})
        
        # Search through all tags
        for tag_id, tag_info in tags.items():
            if isinstance(tag_info, dict) and tag_info.get('tagName', '').upper() == tag_name.upper():
                print("Found tag '{}' with ID {}".format(tag_name, tag_info['tagId']))
                return tag_info['tagId']
                    
        print("Warning: Tag '{}' not found in response".format(tag_name))
        return None
    except Exception as e:
        print("Error fetching tag ID:", e)
        print("Response content:", resp.text if resp else "No response")
        return None

def get_contacts_with_tag(tag_name):
    # First get the tag ID
    tag_id = get_tag_id(tag_name)
    if not tag_id:
        print("Could not find tag ID for '{}'. Aborting search.".format(tag_name))
        return []
        
    url = "{}/contact-service/contact-search".format(BASE_URL)
    params = {
        "firstResult": 1,
        "maxResults": 500,
        "tagIds": tag_id
    }
    try:
        print("Searching for contacts with tag '{}'...".format(tag_name))
        resp = make_request(url, HEADERS, params)
        if not resp:
            return []
        data = resp.json()
        
        results = data.get('response', {}).get('results', [])
        print("Found {} contacts with tag '{}'".format(len(results), tag_name))
        
        contact_ids = [result[0] for result in results if result and len(result) > 0]
        return contact_ids
    except Exception as e:
        print("Error fetching contacts:", e)
        return []

def get_contact_details(contact_id):
    url = "{}/contact-service/contact-search".format(BASE_URL)
    params = {
        "contactId": contact_id,
        "firstResult": 1,
        "maxResults": 1
    }
    try:
        resp = make_request(url, HEADERS, params)
        if not resp:
            return None
        data = resp.json()
        results = data.get('response', {}).get('results', [])
        if results and len(results) > 0:
            contact_data = results[0]
            contact = {
                'contactId': contact_data[0],
                'email': contact_data[1] or '',  # primaryEmail
                'firstName': contact_data[4] or '',
                'lastName': contact_data[5] or '',
                'isSupplier': contact_data[6],
                'companyName': contact_data[7] or '',
                'isStaff': contact_data[8],
                'isCustomer': contact_data[9],
                'phone': contact_data[16] or '',  # pri (primary phone)
                'mobile': contact_data[18] or '',  # mob (mobile phone)
            }
            return contact
        else:
            print("No data found for contact ID: {}".format(contact_id))
            return None
    except Exception as e:
        print("Error fetching contact {} details:".format(contact_id), e)
        return None

def get_contact_addresses(contact_id):
    try:
        # Get full contact details using direct contact endpoint
        contact_url = "{}/contact-service/contact/{}".format(BASE_URL, contact_id)
        print("Fetching contact details from:", contact_url)
        resp = make_request(contact_url, HEADERS)
        if not resp:
            return []
            
        contact_data = resp.json()
        print("Contact data response:", contact_data)
        
        # The response contains a list in the 'response' field
        contact = contact_data.get('response', [])[0] if contact_data.get('response') else None
        if not contact:
            print("No contact found for ID {}".format(contact_id))
            return []
        
        # Get postal address IDs from the postAddressIds dictionary
        post_address_ids = contact.get('postAddressIds', {})
        print("Found address IDs:", post_address_ids)
        if not post_address_ids:
            print("No addresses found")
            return []
            
        # Create a mapping of address IDs to their types
        address_types = {}
        for addr_type, addr_id in post_address_ids.items():
            if addr_id not in address_types:
                address_types[addr_id] = []
            address_types[addr_id].append(addr_type)
            
        # Fetch each unique address once
        addresses = []
        num_addresses = len(address_types)
        print("Found {} unique address{}...".format(
            num_addresses,
            '' if num_addresses == 1 else 'es'
        ))
        
        for addr_id in address_types.keys():
            addr_url = "{}/contact-service/postal-address/{}".format(BASE_URL, addr_id)
            print("Fetching address from:", addr_url)
            resp = make_request(addr_url, HEADERS)
            if not resp:
                continue
                
            addr_data = resp.json()
            print("Address data response:", addr_data)
            addr_data = resp.json().get('response', [])
            if addr_data:
                addr = addr_data[0]
                # Add contact ID to the address
                addr['contactId'] = contact_id
                # Add all types this address is used for
                types = address_types[addr_id]
                # Sort types in consistent order: BIL, DEL, DEF
                type_order = {'BIL': 0, 'DEL': 1, 'DEF': 2}
                types.sort(key=lambda x: type_order.get(x, 99))
                addr['addressType'] = '/'.join(types)
                
                # Add a more readable type description
                type_desc = []
                for t in types:
                    if t == 'BIL': type_desc.append('Billing')
                    elif t == 'DEL': type_desc.append('Delivery')
                    elif t == 'DEF': type_desc.append('Default')
                addr['addressTypeDesc'] = ', '.join(type_desc)
                addresses.append(addr)
        
        if addresses:
            print("Retrieved: {}".format(
                ', '.join(a.get('addressTypeDesc', '') for a in addresses)
            ))
        else:
            print("No addresses were retrieved successfully")
        return addresses
    except Exception as e:
        print("Error fetching addresses:", str(e))
        print("Full error details:", e)
        return []

def get_company_details(company_id):
    return get_contact_details(company_id)

# --- CSV Writers ---
def write_contacts_csv(contacts):
    if not os.path.exists('exports'):
        os.makedirs('exports')
    with open('exports/contacts.csv', 'wb') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'contactId', 'name', 'email', 'phone', 'tagList', 'companyId'
        ])
        writer.writeheader()
        for c in contacts:
            # Ensure all values are encoded as UTF-8 strings
            row = {}
            for key, value in c.items():
                if isinstance(value, unicode):
                    row[key] = value.encode('utf-8')
                else:
                    row[key] = str(value)
            writer.writerow(row)

def write_addresses_csv(addresses):
    if not os.path.exists('exports'):
        os.makedirs('exports')
    
    print("\nDebug: About to write {} addresses to CSV".format(len(addresses)))
    
    with open('exports/addresses.csv', 'wb') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'contactId', 'addressId', 'isBilling', 'isDelivery', 'isDefault',
            'addressLine1', 'addressLine2', 'addressLine3', 'addressLine4',
            'city', 'postcode', 'country'
        ])
        writer.writeheader()
        for a in addresses:
            # Get the address types from the combined string (e.g., "BIL/DEL")
            types = a.get('addressType', '').split('/')
            
            # Map the Brightpearl fields to our CSV fields
            row = {
                'contactId': a.get('contactId', ''),
                'addressId': a.get('addressId', ''),
                'isBilling': 'TRUE' if 'BIL' in types else 'FALSE',
                'isDelivery': 'TRUE' if 'DEL' in types else 'FALSE',
                'isDefault': 'TRUE' if 'DEF' in types else 'FALSE',
                'addressLine1': a.get('addressLine1', ''),
                'addressLine2': a.get('addressLine2', ''),
                'addressLine3': a.get('addressLine3', ''),
                'addressLine4': a.get('addressLine4', ''),
                'city': a.get('addressLine3', ''),  # City is usually in addressLine3
                'postcode': a.get('postalCode', ''),  # Note: API uses 'postalCode', not 'postcode'
                'country': a.get('countryIsoCode', '')
            }
            print("Debug: Writing address:", row)
            
            # Ensure all values are encoded as UTF-8 strings
            encoded_row = {}
            for key, value in row.items():
                if isinstance(value, unicode):
                    encoded_row[key] = value.encode('utf-8')
                else:
                    encoded_row[key] = str(value)
            writer.writerow(encoded_row)

def write_companies_csv(companies):
    if not os.path.exists('exports'):
        os.makedirs('exports')
    with open('exports/companies.csv', 'wb') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'companyId', 'companyName', 'email', 'phone', 'website'
        ])
        writer.writeheader()
        for c in companies:
            # Ensure all values are encoded as UTF-8 strings
            row = {}
            for key, value in c.items():
                if isinstance(value, unicode):
                    row[key] = value.encode('utf-8')
                else:
                    row[key] = str(value)
            writer.writerow(row)

# --- Main Logic ---
def main():
    # For testing - limit to 20 contacts
    TEST_LIMIT = 10
    
    print("\nStarting contacts export...")
    contact_ids = get_contacts_with_tag('B2B')
    if TEST_LIMIT:
        original_count = len(contact_ids)
        contact_ids = contact_ids[:TEST_LIMIT]
        print("\nTEST MODE: Processing {} contacts (limited from {})...\n".format(
            len(contact_ids),
            original_count
        ))
    else:
        print("\nProcessing {} contacts...\n".format(len(contact_ids)))
    
    contacts_csv = []
    addresses_csv = []
    companies_csv = []
    company_ids_seen = set()

    for cid in contact_ids:
        contact = get_contact_details(cid)
        if not contact:
            continue
        
        # Contact basic info
        name = u'{} {}'.format(
            contact['firstName'] or '',
            contact['lastName'] or ''
        ).strip()
        
        contact_row = {
            'contactId': contact['contactId'],
            'name': name,
            'email': contact['email'],
            'phone': contact['phone'] or contact['mobile'],
            'tagList': '',  # We'll add tag handling later if needed
            'companyId': ''  # We'll add company handling later if needed
        }
        contacts_csv.append(contact_row)
        print("Processing: {}".format(name.encode('utf-8')))

        # Get addresses
        addresses = get_contact_addresses(cid)
        print("Debug: Got {} addresses for contact {}".format(len(addresses), cid))
        if addresses:
            addresses_csv.extend(addresses)
            print("Debug: Total addresses collected so far: {}".format(len(addresses_csv)))
        print("")  # Add blank line between contacts

    print("\nWriting files:")
    print("- contacts.csv ({} records)".format(len(contacts_csv)))
    write_contacts_csv(contacts_csv)
    print("- addresses.csv ({} records)".format(len(addresses_csv)))
    write_addresses_csv(addresses_csv)
    print("- companies.csv ({} records)".format(len(companies_csv)))
    write_companies_csv(companies_csv)
    print('\nExport complete!')

if __name__ == '__main__':
    main()
