import csv
import os
from collections import defaultdict

EXPORT_DIR = './exports'
CONVERTED_DIR = './converted'

COMPANIES_CSV = os.path.join(EXPORT_DIR, 'companies.csv')
CONTACTS_CSV = os.path.join(EXPORT_DIR, 'contacts.csv')
ADDRESSES_CSV = os.path.join(EXPORT_DIR, 'addresses.csv')
OUTPUT_CSV = os.path.join(CONVERTED_DIR, 'companies.csv')

# Output columns as per Shopify example
OUTPUT_COLUMNS = [
    'Name', 'Command', 'Main Contact: Customer ID',
    'Location: Name', 'Location: Command', 'Location: Phone', 'Location: Locale', 'Location: Tax ID', 'Location: Tax Setting', 'Location: Tax Exemptions',
    'Location: Allow Shipping To Any Address', 'Location: Checkout To Draft', 'Location: Checkout Payment Terms', 'Location: Checkout Pay Now Only',
    'Location: Shipping First Name', 'Location: Shipping Last Name', 'Location: Shipping Recipient', 'Location: Shipping Phone', 'Location: Shipping Address 1', 'Location: Shipping Address 2', 'Location: Shipping Zip', 'Location: Shipping City', 'Location: Shipping Province Code', 'Location: Shipping Country Code',
    'Location: Billing First Name', 'Location: Billing Last Name', 'Location: Billing Recipient', 'Location: Billing Phone', 'Location: Billing Address 1', 'Location: Billing Address 2', 'Location: Billing Zip', 'Location: Billing City', 'Location: Billing Province Code', 'Location: Billing Country Code',
    'Location: Catalogs', 'Location: Catalogs Command',
    'Customer: Email', 'Customer: Command', 'Customer: First Name', 'Customer: Last Name', 'Customer: Location Role',
    'Metafield: brightpearl.contact_id [single_line_text_field]', 'Metafield: brightpearl.wholesale [boolean]'
]

# Expected columns for each input file
COMPANIES_COLUMNS = ['companyId', 'companyName', 'email', 'phone', 'website', 'isPrimaryContact', 'priceListId', 'nominalCode', 'taxCodeId', 'creditTermDays', 'currencyId', 'discountPercentage', 'creditTermTypeId']
CONTACTS_COLUMNS = ['contactId', 'isPrimaryContact', 'name', 'email', 'phone', 'tagList', 'companyId', 'Wholesale', 'Joor Account Code']
ADDRESSES_COLUMNS = ['contactId', 'addressId', 'isBilling', 'isDelivery', 'isDefault', 'addressLine1', 'addressLine2', 'addressLine3', 'addressLine4', 'city', 'postcode', 'country']

def ensure_csv_exists(path, columns):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()

def read_csv(path):
    # Create the file with just the header if it does not exist
    if path == COMPANIES_CSV:
        ensure_csv_exists(path, COMPANIES_COLUMNS)
    elif path == CONTACTS_CSV:
        ensure_csv_exists(path, CONTACTS_COLUMNS)
    elif path == ADDRESSES_CSV:
        ensure_csv_exists(path, ADDRESSES_COLUMNS)
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def normalize_address(addr):
    # Lowercase, remove spaces and special chars for deduplication
    return ''.join(e for e in addr.lower() if e.isalnum())

def split_name(name):
    parts = name.strip().split()
    if not parts:
        return '', ''
    return parts[0], ' '.join(parts[1:])

def main():
    os.makedirs(CONVERTED_DIR, exist_ok=True)
    companies = read_csv(COMPANIES_CSV)
    contacts = read_csv(CONTACTS_CSV)
    addresses = read_csv(ADDRESSES_CSV)

    # Index contacts by companyId
    contacts_by_company = defaultdict(list)
    for c in contacts:
        contacts_by_company[c.get('companyId','')].append(c)

    # Index addresses by contactId
    addresses_by_contact = defaultdict(list)
    for a in addresses:
        addresses_by_contact[a.get('contactId','')].append(a)

    rows = []
    for company in companies:
        company_id = company.get('companyId','')
        company_name = company.get('companyName','')
        company_contacts = contacts_by_company.get(company_id, [])
        # If company_name is empty, use the name from the main contact (contactId == companyId)
        if not company_name:
            main_contact = next((c for c in company_contacts if c.get('contactId','') == company_id), None)
            if main_contact:
                company_name = main_contact.get('name','')
        for contact in company_contacts:
            contact_id = contact.get('contactId','')
            contact_name = contact.get('name','')
            contact_email = contact.get('email','')
            contact_phone = contact.get('phone','')
            wholesale = contact.get('Wholesale','')
            # Find all delivery addresses for this contact
            delivery_addrs = [a for a in addresses_by_contact.get(contact_id, []) if a.get('isDelivery','').upper() == 'TRUE']
            # Deduplicate by normalized addressLine1
            seen = set()
            unique_delivery_addrs = []
            for addr in delivery_addrs:
                norm = normalize_address(addr.get('addressLine1',''))
                if norm and norm not in seen:
                    seen.add(norm)
                    unique_delivery_addrs.append(addr)
            # Find first billing address for this contact
            billing_addr = next((a for a in addresses_by_contact.get(contact_id, []) if a.get('isBilling','').upper() == 'TRUE'), None)
            for ship_addr in unique_delivery_addrs:
                # Shipping fields
                ship_first, ship_last = split_name(contact_name)
                ship_recipient = company_name
                ship_phone = contact_phone
                ship_addr1 = ship_addr.get('addressLine1','')
                ship_addr2 = ship_addr.get('addressLine2','')
                ship_zip = ship_addr.get('postcode','')
                ship_city = ship_addr.get('city','')
                ship_prov = ship_addr.get('addressLine4') or ship_addr.get('addressLine3','')
                ship_country = ship_addr.get('country','')
                # Billing fields
                if billing_addr:
                    bill_first, bill_last = split_name(contact_name)
                    bill_recipient = company_name
                    bill_phone = contact_phone
                    bill_addr1 = billing_addr.get('addressLine1','')
                    bill_addr2 = billing_addr.get('addressLine2','')
                    bill_zip = billing_addr.get('postcode','')
                    bill_city = billing_addr.get('city','')
                    bill_prov = billing_addr.get('addressLine4') or billing_addr.get('addressLine3','')
                    bill_country = billing_addr.get('country','')
                else:
                    bill_first = bill_last = bill_recipient = bill_phone = bill_addr1 = bill_addr2 = bill_zip = bill_city = bill_prov = bill_country = ''
                # Customer info
                cust_first, cust_last = split_name(contact_name)
                location_role = 'Location admin' if contact_id == company_id else 'Ordering only'
                # Build row
                row = {
                    'Name': company_name,
                    'Command': 'NEW',
                    'Main Contact: Customer ID': '',  # To be defined later
                    'Location: Name': ship_addr1,
                    'Location: Command': 'NEW',
                    'Location: Phone': contact_phone,
                    'Location: Locale': 'es',
                    'Location: Tax ID': '',
                    'Location: Tax Setting': '',
                    'Location: Tax Exemptions': '',
                    'Location: Allow Shipping To Any Address': 'TRUE',
                    'Location: Checkout To Draft': 'FALSE',
                    'Location: Checkout Payment Terms': '',
                    'Location: Checkout Pay Now Only': 'FALSE',
                    'Location: Shipping First Name': ship_first,
                    'Location: Shipping Last Name': ship_last,
                    'Location: Shipping Recipient': ship_recipient,
                    'Location: Shipping Phone': ship_phone,
                    'Location: Shipping Address 1': ship_addr1,
                    'Location: Shipping Address 2': ship_addr2,
                    'Location: Shipping Zip': ship_zip,
                    'Location: Shipping City': ship_city,
                    'Location: Shipping Province Code': ship_prov,
                    'Location: Shipping Country Code': ship_country,
                    'Location: Billing First Name': bill_first,
                    'Location: Billing Last Name': bill_last,
                    'Location: Billing Recipient': bill_recipient,
                    'Location: Billing Phone': bill_phone,
                    'Location: Billing Address 1': bill_addr1,
                    'Location: Billing Address 2': bill_addr2,
                    'Location: Billing Zip': bill_zip,
                    'Location: Billing City': bill_city,
                    'Location: Billing Province Code': bill_prov,
                    'Location: Billing Country Code': bill_country,
                    'Location: Catalogs': '',
                    'Location: Catalogs Command': 'NEW',
                    'Customer: Email': contact_email,
                    'Customer: Command': 'MERGE',
                    'Customer: First Name': cust_first,
                    'Customer: Last Name': cust_last,
                    'Customer: Location Role': location_role,
                    'Metafield: brightpearl.contact_id [single_line_text_field]': contact_id,
                    'Metafield: brightpearl.wholesale [boolean]': wholesale,
                }
                rows.append(row)
    # Write output
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

if __name__ == '__main__':
    main()
