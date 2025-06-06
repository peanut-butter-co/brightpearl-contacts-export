# -*- coding: utf-8 -*-

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

# Let's use simple text indicators instead of emojis for better compatibility
INDICATORS = {
    'error': '[ERROR]',
    'warning': '[WARNING]',
    'info': '[INFO]',
    'success': '[SUCCESS]',
    'progress': '[PROGRESS]'
}

def make_request(url, headers, params=None):
    """
    Make a rate-limited request with retries
    """
    start_time = time.time()
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)  # Rate limiting delay
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            elapsed = time.time() - start_time
            if elapsed > 5:  # Log if request takes more than 5 seconds
                print("{} API request took {:.1f}s: {}".format(
                    INDICATORS['warning'],
                    elapsed,
                    url
                ))
            return resp
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                if attempt < MAX_RETRIES - 1:  # Don't sleep on last attempt
                    print("{} Rate limit hit, waiting {} seconds...".format(
                        INDICATORS['warning'],
                        RETRY_DELAY
                    ))
                    time.sleep(RETRY_DELAY)
                    continue
            print("{} HTTP error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            raise
        except Exception as e:
            print("{} Request error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            raise
    return None

# --- API Functions ---
def get_tag_id(tag_name):
    url = "{}/contact-service/tag".format(BASE_URL)
    try:
        resp = make_request(url, HEADERS)
        if not resp:
            return None
        data = resp.json()
        
        # Find the tag with matching name
        tags = data.get('response', {})
        for tag_id, tag_info in tags.items():
            if isinstance(tag_info, dict) and tag_info.get('tagName', '').upper() == tag_name.upper():
                return tag_info['tagId']
                    
        print("{} Tag '{}' not found".format(INDICATORS['error'], tag_name))
        return None
    except Exception as e:
        print("{} Error fetching tag ID: {}".format(INDICATORS['error'], str(e)))
        return None

def get_contacts_with_tag(tag_name):
    # First get the tag ID
    tag_id = get_tag_id(tag_name)
    if not tag_id:
        return []
        
    url = "{}/contact-service/contact-search".format(BASE_URL)
    all_contact_ids = []
    first_result = 1
    page_size = 200  # Maximum allowed by Brightpearl
    
    while True:
        params = {
            "firstResult": first_result,
            "maxResults": page_size,
            "tagIds": tag_id
        }
        
        try:
            resp = make_request(url, HEADERS, params)
            if not resp:
                break
                
            data = resp.json()
            results = data.get('response', {}).get('results', [])
            
            if not results:
                break
                
            # Extract contact IDs from this page
            page_contact_ids = [result[0] for result in results if result and len(result) > 0]
            all_contact_ids.extend(page_contact_ids)
            
            current_page = ((first_result - 1) // page_size) + 1
            print("{} Page {}: {} contacts".format(INDICATORS['info'], current_page, len(page_contact_ids)))
            
            # Update first_result for next page
            first_result += len(page_contact_ids)
            
            # If we got fewer results than requested, we're done
            if len(page_contact_ids) < page_size:
                break
            
        except Exception as e:
            print("{} Error fetching contacts page {}: {}".format(INDICATORS['error'], current_page, e))
            break
    
    total = len(all_contact_ids)
    print("{} Found {} total contacts with tag '{}'".format(INDICATORS['success'], total, tag_name))
    return all_contact_ids

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
        resp = make_request(contact_url, HEADERS)
        if not resp:
            return []
            
        contact_data = resp.json()
        contact = contact_data.get('response', [])[0] if contact_data.get('response') else None
        if not contact:
            return []
        
        # Get postal address IDs from the postAddressIds dictionary
        post_address_ids = contact.get('postAddressIds', {})
        if not post_address_ids:
            return []
            
        # Create a mapping of address IDs to their types
        address_types = {}
        for addr_type, addr_id in post_address_ids.items():
            if addr_id not in address_types:
                address_types[addr_id] = []
            address_types[addr_id].append(addr_type)
            
        # Fetch each unique address once
        addresses = []
        for addr_id in address_types.keys():
            resp = make_request(
                "{}/contact-service/postal-address/{}".format(BASE_URL, addr_id),
                HEADERS
            )
            if not resp:
                continue
                
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
                addresses.append(addr)
        
        return addresses
    except Exception as e:
        print("{} Error fetching addresses for contact {}: {}".format(INDICATORS['error'], contact_id, str(e)))
        return []

def get_company_details(contact_id):
    try:
        # Get full contact details using direct contact endpoint
        contact_url = "{}/contact-service/contact/{}".format(BASE_URL, contact_id)
        resp = make_request(contact_url, HEADERS)
        if not resp:
            return None
            
        contact_data = resp.json()
        contact = contact_data.get('response', [])[0] if contact_data.get('response') else None
        if not contact:
            return None

        # Extract organization details
        org = contact.get('organisation', {})
        if not org or org.get('organisationId', 0) == 0:
            return None

        # Get communication details
        communication = contact.get('communication', {})
        emails = communication.get('emails', {})
        telephones = communication.get('telephones', {})
        websites = communication.get('websites', {})

        # Get primary email if it exists
        primary_email = emails.get('PRI', {})
        if isinstance(primary_email, dict):
            email = primary_email.get('email', '')
        else:
            email = ''

        # Get primary phone or mobile
        phone = telephones.get('PRI', '') or telephones.get('MOB', '')

        # Get primary website
        primary_website = websites.get('PRI', {})
        if isinstance(primary_website, dict):
            website = primary_website.get('url', '')
        else:
            website = ''

        company = {
            'companyId': org.get('organisationId', ''),
            'companyName': org.get('name', ''),
            'email': email,
            'phone': phone,
            'website': website
        }
        return company
    except Exception as e:
        print("{} Error fetching company details for contact {}: {}".format(INDICATORS['error'], contact_id, str(e)))
        return None

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
                'postcode': a.get('postalCode', ''),
                'country': a.get('countryIsoCode', '')
            }
            
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
    # For testing - set to 0 for unlimited contacts
    TEST_LIMIT = 100
    
    print("\n{} Starting B2B contacts export...".format(INDICATORS['info']))
    contact_ids = get_contacts_with_tag('B2B')
    if TEST_LIMIT:
        original_count = len(contact_ids)
        contact_ids = contact_ids[:TEST_LIMIT]
        print("\n{} TEST MODE: Processing {} of {} contacts\n".format(
            INDICATORS['warning'],
            len(contact_ids),
            original_count
        ))
    else:
        print("\n{} Processing all {} contacts\n".format(INDICATORS['info'], len(contact_ids)))
    
    contacts_csv = []
    addresses_csv = []
    companies_csv = []
    company_ids_seen = set()

    total_contacts = len(contact_ids)
    for idx, cid in enumerate(contact_ids, 1):
        try:
            # Progress indicator (update on same line)
            sys.stdout.write("\r{} Progress: {}/{} contacts processed".format(
                INDICATORS['progress'],
                idx,
                total_contacts
            ))
            sys.stdout.flush()
            
            # Get contact details
            contact = get_contact_details(cid)
            if not contact:
                print("\n{} Skipping contact ID {} - no details found".format(INDICATORS['warning'], cid))
                continue
            
            # Contact basic info
            name = u'{} {}'.format(
                contact['firstName'] or '',
                contact['lastName'] or ''
            ).strip()
            
            # Get company details if we haven't seen this company before
            company = get_company_details(cid)
            company_id = company['companyId'] if company else ''
            
            contact_row = {
                'contactId': contact['contactId'],
                'name': name,
                'email': contact['email'],
                'phone': contact['phone'] or contact['mobile'],
                'tagList': '',
                'companyId': company_id
            }
            contacts_csv.append(contact_row)
            
            if company and company_id and company_id not in company_ids_seen:
                companies_csv.append(company)
                company_ids_seen.add(company_id)

            # Get addresses
            addresses = get_contact_addresses(cid)
            if addresses:
                addresses_csv.extend(addresses)
                
        except Exception as e:
            print("\n{} Error processing contact {}: {}".format(INDICATORS['error'], cid, str(e)))
            continue

    # Print a newline after progress is complete
    print("\n")
    print("{} Writing export files:".format(INDICATORS['info']))
    print("- contacts.csv: {} records".format(len(contacts_csv)))
    write_contacts_csv(contacts_csv)
    print("- addresses.csv: {} records".format(len(addresses_csv)))
    write_addresses_csv(addresses_csv)
    print("- companies.csv: {} records".format(len(companies_csv)))
    write_companies_csv(companies_csv)
    print('\n{} Export complete!'.format(INDICATORS['success']))

if __name__ == '__main__':
    main()
