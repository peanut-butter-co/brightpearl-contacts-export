# -*- coding: utf-8 -*-

import os
import requests
import json
from dotenv import load_dotenv
import sys
import time

# Load environment variables
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

def make_request(url, headers, params=None):
    """Make a request to the Brightpearl API"""
    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("Error making request: {}".format(str(e)))
        return None

def get_contact_details(contact_id):
    """Get full contact details including addresses and company info"""
    # Get basic contact info
    url = "{}/contact-service/contact/{}".format(BASE_URL, contact_id)
    params = {"includeOptional": "customFields"}
    contact_data = make_request(url, HEADERS, params=params)
    if not contact_data or not contact_data.get('response'):
        print("Contact not found")
        return None
        
    contact = contact_data['response'][0]
    
    # Get addresses
    addresses = []
    post_address_ids = contact.get('postAddressIds', {})
    for addr_type, addr_id in post_address_ids.items():
        addr_url = "{}/contact-service/postal-address/{}".format(BASE_URL, addr_id)
        addr_data = make_request(addr_url, HEADERS)
        if addr_data and addr_data.get('response'):
            addr = addr_data['response'][0]
            addr['type'] = addr_type
            addresses.append(addr)
    
    # Extract custom fields
    custom_fields = contact.get('customFields', {})
    # Map Brightpearl codes to user-friendly names
    wholesale = custom_fields.get('PCF_CUSTWHOL', None)
    joor_account_code = custom_fields.get('PCF_JOORACCO', None)

    # Format the output
    output = {
        "Contact Information": {
            "Contact ID": contact.get('contactId'),
            "First Name": contact.get('firstName'),
            "Last Name": contact.get('lastName'),
            "Title": contact.get('title'),
            "Salutation": contact.get('salutation')
        },
        "Communication": {
            "Emails": contact.get('communication', {}).get('emails', {}),
            "Phones": contact.get('communication', {}).get('telephones', {}),
            "Websites": contact.get('communication', {}).get('websites', {})
        },
        "Organization": contact.get('organisation', {}),
        "Addresses": addresses,
        "Relationships": {
            "Is Customer": contact.get('isCustomer', False),
            "Is Supplier": contact.get('isSupplier', False),
            "Is Staff": contact.get('isStaff', False)
        },
        "Custom Fields": {
            "Wholesale": wholesale,
            "Joor Account Code": joor_account_code
        }
    }
    
    return output

def print_contact(contact_data):
    """Pretty print contact information"""
    if not contact_data:
        return
        
    print("\n=== Contact Information ===")
    for key, value in contact_data["Contact Information"].items():
        if value:
            print("{}: {}".format(key, value))
    
    print("\n=== Communication ===")
    comm = contact_data["Communication"]
    if comm["Emails"]:
        print("Emails:")
        for email_type, email in comm["Emails"].items():
            print("  {}: {}".format(email_type, email.get('email', '')))
    
    if comm["Phones"]:
        print("Phones:")
        for phone_type, number in comm["Phones"].items():
            print("  {}: {}".format(phone_type, number))
            
    if comm["Websites"]:
        print("Websites:")
        for web_type, web in comm["Websites"].items():
            print("  {}: {}".format(web_type, web.get('url', '')))
    
    org = contact_data["Organization"]
    if org:
        print("\n=== Organization ===")
        print("Organization ID: {}".format(org.get('organisationId')))
        print("Name: {}".format(org.get('name')))
    
    print("\n=== Addresses ===")
    for addr in contact_data["Addresses"]:
        print("\nAddress Type: {}".format(addr['type']))
        for key in ['addressLine1', 'addressLine2', 'addressLine3', 'addressLine4']:
            if addr.get(key):
                print(addr[key])
        if addr.get('postalCode'):
            print(addr['postalCode'])
        if addr.get('countryIsoCode'):
            print(addr['countryIsoCode'])
    
    print("\n=== Relationships ===")
    for key, value in contact_data["Relationships"].items():
        print("{}: {}".format(key, value))

    # Print custom fields if present
    custom_fields = contact_data.get("Custom Fields", {})
    if custom_fields and (custom_fields.get("Wholesale") is not None or custom_fields.get("Joor Account Code") is not None):
        print("\n=== Custom Fields ===")
        if custom_fields.get("Wholesale") is not None:
            print("Wholesale: {}".format(custom_fields["Wholesale"]))
        if custom_fields.get("Joor Account Code") is not None:
            print("Joor Account Code: {}".format(custom_fields["Joor Account Code"]))

def main():
    if len(sys.argv) != 2:
        print("Usage: python get_contact.py <contact_id>")
        sys.exit(1)
        
    try:
        contact_id = int(sys.argv[1])
    except ValueError:
        print("Error: Contact ID must be a number")
        sys.exit(1)
        
    # Get the raw API response with customFields
    url = "{}/contact-service/contact/{}".format(BASE_URL, contact_id)
    params = {"includeOptional": "customFields"}
    raw_response = make_request(url, HEADERS, params=params)
    contact_data = get_contact_details(contact_id)
    if contact_data:
        print_contact(contact_data)
        print("\n=== Raw JSON Response ===")
        print(json.dumps(raw_response, indent=2, ensure_ascii=False))
    else:
        print("Failed to retrieve contact information")

if __name__ == '__main__':
    main()
