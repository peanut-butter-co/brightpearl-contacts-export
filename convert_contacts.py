import csv
import os
from collections import defaultdict
import openai
from dotenv import load_dotenv
import time

load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

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

# Mapping of 3-letter to 2-letter country codes for European countries
EU_COUNTRY_CODES = {
    'ESP': 'ES', 'FRA': 'FR', 'DEU': 'DE', 'ITA': 'IT', 'PRT': 'PT', 'NLD': 'NL', 'BEL': 'BE', 'GBR': 'GB', 'IRL': 'IE',
    'CHE': 'CH', 'AUT': 'AT', 'SWE': 'SE', 'NOR': 'NO', 'DNK': 'DK', 'FIN': 'FI', 'POL': 'PL', 'CZE': 'CZ', 'SVK': 'SK',
    'HUN': 'HU', 'ROU': 'RO', 'BGR': 'BG', 'HRV': 'HR', 'SVN': 'SI', 'EST': 'EE', 'LVA': 'LV', 'LTU': 'LT', 'GRC': 'GR',
    'CYP': 'CY', 'MLT': 'MT', 'LUX': 'LU', 'ISL': 'IS', 'LIE': 'LI'
}

BATCH_SIZE = 10

def normalize_addresses_llm_batch(addresses, address_type):
    if not OPENAI_API_KEY or not addresses:
        # Fallback: return original city and province
        return [(
            a.get('city', ''),
            a.get('addressLine4') or a.get('addressLine3', '')
        ) for a in addresses]
    prompt = f"""
For each of the following {address_type} addresses from a European country, return the corrected city name and the ISO 3166-2 province-level code (not autonomous community or region). For example, return ES-M for Madrid, not ES-MD. Respond as a JSON array of objects, each with 'city' and 'province_code'.

Addresses:
"""
    for idx, a in enumerate(addresses, 1):
        prompt += f"{idx}. Address Line 1: {a.get('addressLine1','')}, Address Line 2: {a.get('addressLine2','')}, City: {a.get('city','')}, Province: {a.get('addressLine4') or a.get('addressLine3','')}, Postcode: {a.get('postcode','')}, Country: {a.get('country','')}\n"
    prompt += "\nExample response: [{\"city\":\"Barcelona\",\"province_code\":\"ES-B\"}, ...]"
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            content = response.choices[0].message.content
            import json
            data = json.loads(content)
            if isinstance(data, list) and len(data) == len(addresses):
                return [(
                    item.get('city', addresses[i].get('city','')),
                    item.get('province_code', addresses[i].get('addressLine4') or addresses[i].get('addressLine3',''))
                ) for i, item in enumerate(data)]
        except Exception as e:
            print(f"[LLM] Error in batch {address_type} normalization: {str(e)}. Retrying... Error type: {type(e).__name__}")
            time.sleep(2)
    # Fallback: single requests
    print(f"[LLM] Batch failed, falling back to single {address_type} requests.")
    return [normalize_address_llm(a, address_type) for a in addresses]

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

def convert_country_code(code):
    code = (code or '').strip().upper()
    if len(code) == 3 and code in EU_COUNTRY_CODES:
        return EU_COUNTRY_CODES[code]
    return code if len(code) == 2 else ''

# Single-address LLM normalization function for fallback

def normalize_address_llm(address_dict, address_type):
    if not OPENAI_API_KEY:
        return address_dict.get('city', ''), address_dict.get('addressLine4') or address_dict.get('addressLine3', '')
    prompt = f"""
Given the following {address_type} address from a European country return:
- The corrected city name (if needed)
- The ISO 3166-2 province-level code (not autonomous community or region). For example, return ES-M for Madrid, not ES-MD.

Address:
Address Line 1: {address_dict.get('addressLine1','')}
Address Line 2: {address_dict.get('addressLine2','')}
City: {address_dict.get('city','')}
Province: {address_dict.get('addressLine4') or address_dict.get('addressLine3','')}
Postcode: {address_dict.get('postcode','')}
Country: {address_dict.get('country','')}

Respond in JSON like this:
{{"city": "<corrected_city>", "province_code": "<iso_code>"}}
"""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            content = response.choices[0].message.content
            import json
            data = json.loads(content)
            return data.get('city', address_dict.get('city','')), data.get('province_code', address_dict.get('addressLine4') or address_dict.get('addressLine3',''))
        except Exception as e:
            print(f"[LLM] Error normalizing {address_type} address (single): {str(e)}. Error type: {type(e).__name__}. Retrying...")
            time.sleep(2)
    # Fallback to original
    return address_dict.get('city', ''), address_dict.get('addressLine4') or address_dict.get('addressLine3', '')

def main():
    print("[INFO] Starting conversion...")
    os.makedirs(CONVERTED_DIR, exist_ok=True)
    print("[INFO] Loading input files...")
    companies = read_csv(COMPANIES_CSV)
    contacts = read_csv(CONTACTS_CSV)
    addresses = read_csv(ADDRESSES_CSV)
    print(f"[INFO] Loaded {len(companies)} companies, {len(contacts)} contacts, {len(addresses)} addresses.")

    # Index contacts by companyId
    contacts_by_company = defaultdict(list)
    for c in contacts:
        contacts_by_company[c.get('companyId','')].append(c)

    # Index addresses by contactId
    addresses_by_contact = defaultdict(list)
    for a in addresses:
        addresses_by_contact[a.get('contactId','')].append(a)

    rows = []
    # Collect all shipping and billing addresses to normalize in batch
    all_ship_addrs = []
    all_bill_addrs = []
    ship_addr_refs = []  # (row_idx, addr_dict)
    bill_addr_refs = []
    print("[INFO] Building output rows and collecting addresses for normalization...")
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
                all_ship_addrs.append(ship_addr)
                ship_addr_refs.append((len(rows), ship_addr))
                if billing_addr:
                    all_bill_addrs.append(billing_addr)
                    bill_addr_refs.append((len(rows), billing_addr))
                else:
                    bill_addr_refs.append((len(rows), None))
                # Build row with placeholders for city/province
                ship_first, ship_last = split_name(contact_name)
                ship_recipient = company_name
                ship_phone = contact_phone
                ship_addr1 = ship_addr.get('addressLine1','')
                ship_addr2 = ship_addr.get('addressLine2','')
                ship_zip = ship_addr.get('postcode','')
                ship_city = ship_addr.get('city','')
                ship_prov = ship_addr.get('addressLine4') or ship_addr.get('addressLine3','')
                ship_country = convert_country_code(ship_addr.get('country',''))
                if billing_addr:
                    bill_first, bill_last = split_name(contact_name)
                    bill_recipient = company_name
                    bill_phone = contact_phone
                    bill_addr1 = billing_addr.get('addressLine1','')
                    bill_addr2 = billing_addr.get('addressLine2','')
                    bill_zip = billing_addr.get('postcode','')
                    bill_city = billing_addr.get('city','')
                    bill_prov = billing_addr.get('addressLine4') or billing_addr.get('addressLine3','')
                    bill_country = convert_country_code(billing_addr.get('country',''))
                else:
                    bill_first = bill_last = bill_recipient = bill_phone = bill_addr1 = bill_addr2 = bill_zip = bill_city = bill_prov = bill_country = ''
                cust_first, cust_last = split_name(contact_name)
                location_role = 'Location admin' if contact_id == company_id else 'Ordering only'
                row = {
                    'Name': company_name,
                    'Command': 'NEW',
                    'Main Contact: Customer ID': '',
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
                    'Location: Catalogs Command': 'MERGE',
                    'Customer: Email': contact_email,
                    'Customer: Command': 'MERGE',
                    'Customer: First Name': cust_first,
                    'Customer: Last Name': cust_last,
                    'Customer: Location Role': location_role,
                    'Metafield: brightpearl.contact_id [single_line_text_field]': contact_id,
                    'Metafield: brightpearl.wholesale [boolean]': wholesale,
                }
                rows.append(row)
    print(f"[INFO] Collected {len(all_ship_addrs)} shipping and {len(all_bill_addrs)} billing addresses for normalization.")
    # Batch normalize shipping addresses
    for i in range(0, len(all_ship_addrs), BATCH_SIZE):
        batch = all_ship_addrs[i:i+BATCH_SIZE]
        print(f"[LLM] Normalizing shipping addresses {i+1}-{i+len(batch)}...")
        results = normalize_addresses_llm_batch(batch, 'shipping')
        for j, (city, prov) in enumerate(results):
            row_idx, _ = ship_addr_refs[i+j]
            rows[row_idx]['Location: Shipping City'] = city
            rows[row_idx]['Location: Shipping Province Code'] = prov
    # Batch normalize billing addresses
    for i in range(0, len(all_bill_addrs), BATCH_SIZE):
        batch = all_bill_addrs[i:i+BATCH_SIZE]
        print(f"[LLM] Normalizing billing addresses {i+1}-{i+len(batch)}...")
        results = normalize_addresses_llm_batch(batch, 'billing')
        for j, (city, prov) in enumerate(results):
            row_idx, _ = bill_addr_refs[i+j]
            if bill_addr_refs[i+j][1] is not None:
                rows[row_idx]['Location: Billing City'] = city
                rows[row_idx]['Location: Billing Province Code'] = prov
    print(f"[INFO] Writing {len(rows)} rows to {OUTPUT_CSV}")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print("[SUCCESS] Conversion complete!")

if __name__ == '__main__':
    main()
