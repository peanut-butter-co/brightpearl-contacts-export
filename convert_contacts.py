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

# Mapping of 3-letter to 2-letter country codes for European countries and US
EU_COUNTRY_CODES = {
    'ESP': 'ES', 'FRA': 'FR', 'DEU': 'DE', 'ITA': 'IT', 'PRT': 'PT', 'NLD': 'NL', 'BEL': 'BE', 'GBR': 'GB', 'IRL': 'IE',
    'CHE': 'CH', 'AUT': 'AT', 'SWE': 'SE', 'NOR': 'NO', 'DNK': 'DK', 'FIN': 'FI', 'POL': 'PL', 'CZE': 'CZ', 'SVK': 'SK',
    'HUN': 'HU', 'ROU': 'RO', 'BGR': 'BG', 'HRV': 'HR', 'SVN': 'SI', 'EST': 'EE', 'LVA': 'LV', 'LTU': 'LT', 'GRC': 'GR',
    'CYP': 'CY', 'MLT': 'MT', 'LUX': 'LU', 'ISL': 'IS', 'LIE': 'LI',
    'USA': 'US', 'GUM': 'GU', 'PRI': 'PR', 'VIR': 'VI', 'ASM': 'AS', 'MNP': 'MP'  # US and territories
}

# US State codes mapping
US_STATE_CODES = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
    'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH',
    'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
    'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC', 'AMERICAN SAMOA': 'AS', 'GUAM': 'GU', 'NORTHERN MARIANA ISLANDS': 'MP',
    'PUERTO RICO': 'PR', 'VIRGIN ISLANDS': 'VI'
}

BATCH_SIZE = 10

def strip_country_prefix(province_code):
    """Strip country prefix from province codes (e.g., 'ES-M' becomes 'M')"""
    if not province_code:
        return ''
    parts = province_code.split('-')
    return parts[-1] if len(parts) > 1 else province_code

def normalize_addresses_llm_batch(addresses, address_type):
    if not OPENAI_API_KEY or not addresses:
        return [(
            a.get('city', ''),
            a.get('addressLine4') or a.get('addressLine3', '')
        ) for a in addresses]
    prompt = f"""You are a JSON-only response API. Your task is to normalize addresses and return them in a specific JSON array format.

For the following {len(addresses)} addresses, create a JSON array where each item contains the normalized city name and province/state code.

Rules:
1. For European addresses: Use province codes without country prefix (e.g., "M" for Madrid, not "ES-M")
2. For US addresses: Use standard two-letter state codes (e.g., "NY" for New York)
3. Return ONLY a JSON array with exactly {len(addresses)} items
4. Each item must have exactly two fields: "city" and "province_code"
5. Do not include any explanation, numbering, or additional text
6. Use the provided postal code to help determine the correct province/state

Required JSON format:
[
  {{"city": "Madrid", "province_code": "M"}},
  {{"city": "New York", "province_code": "NY"}}
]

Input addresses:
"""
    for idx, a in enumerate(addresses, 1):
        normalized_postcode = normalize_spanish_postal_code(a.get('postcode', ''), a.get('country', ''))
        prompt += f"{idx}. Address Line 1: {a.get('addressLine1','')}, Address Line 2: {a.get('addressLine2','')}, City: {a.get('city','')}, Province/State: {a.get('addressLine4') or a.get('addressLine3','')}, Postcode: {normalized_postcode}, Country: {a.get('country','')}\n"

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            content = response.choices[0].message.content
            if not content.strip():
                print(f"[LLM] Warning: Empty response received on attempt {attempt + 1}")
                raise ValueError("Empty response from API")
            
            print(f"[DEBUG] Raw API response content: {content[:200]}...")
            
            # Try to clean the response if it's not pure JSON
            content = content.strip()
            if not (content.startswith('[') and content.endswith(']')):
                # Try to find JSON array within the response
                start = content.find('[')
                end = content.rfind(']')
                if start != -1 and end != -1:
                    content = content[start:end+1]
                    print(f"[DEBUG] Extracted JSON array: {content[:200]}...")
            
            import json
            data = json.loads(content)
            if isinstance(data, list) and len(data) == len(addresses):
                return [(
                    item.get('city', addresses[i].get('city','')),
                    strip_country_prefix(item.get('province_code', addresses[i].get('addressLine4') or addresses[i].get('addressLine3','')))
                ) for i, item in enumerate(data)]
            else:
                print(f"[LLM] Warning: Response length mismatch. Expected {len(addresses)}, got {len(data) if isinstance(data, list) else 'non-list'}")
                raise ValueError("Response length mismatch")
        except openai.RateLimitError as e:
            print(f"[LLM] Rate limit exceeded, falling back to basic address handling: {str(e)}")
            return [(
                a.get('city', ''),
                a.get('addressLine4') or a.get('addressLine3', '')
            ) for a in addresses]
        except json.JSONDecodeError as e:
            print(f"[LLM] JSON decode error in batch {address_type} normalization (attempt {attempt + 1}/3): {str(e)}. Position: {e.pos}, Line: {e.lineno}, Column: {e.colno}")
            time.sleep(2)
        except Exception as e:
            print(f"[LLM] Error in batch {address_type} normalization (attempt {attempt + 1}/3): {str(e)}. Error type: {type(e).__name__}. Retrying...")
            time.sleep(2)
    # Fallback: single requests
    print(f"[LLM] Batch failed after 3 attempts, falling back to single {address_type} requests.")
    return [normalize_address_llm(a, address_type) for a in addresses]

def normalize_address_llm(address_dict, address_type):
    if not OPENAI_API_KEY:
        return address_dict.get('city', ''), address_dict.get('addressLine4') or address_dict.get('addressLine3', '')
    prompt = f"""You are a JSON-only response API. Your task is to normalize a single address and return it in a specific JSON format.

Rules:
1. For European addresses: Use province code without country prefix (e.g., "M" for Madrid, not "ES-M")
2. For US addresses: Use standard two-letter state code (e.g., "NY" for New York)
3. Return ONLY a single JSON object
4. The object must have exactly two fields: "city" and "province_code"
5. Do not include any explanation or additional text
6. Use the provided postal code to help determine the correct province/state

Required JSON format:
{{"city": "Madrid", "province_code": "M"}}

Input address:
Address Line 1: {address_dict.get('addressLine1','')}
Address Line 2: {address_dict.get('addressLine2','')}
City: {address_dict.get('city','')}
Province/State: {address_dict.get('addressLine4') or address_dict.get('addressLine3','')}
Postcode: {normalize_spanish_postal_code(address_dict.get('postcode', ''), address_dict.get('country', ''))}
Country: {address_dict.get('country','')}"""

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            content = response.choices[0].message.content
            if not content.strip():
                print(f"[LLM] Warning: Empty response received on attempt {attempt + 1}")
                raise ValueError("Empty response from API")
            
            print(f"[DEBUG] Raw API response content: {content[:200]}...")
            
            # Try to clean the response if it's not pure JSON
            content = content.strip()
            if not (content.startswith('{') and content.endswith('}')):
                # Try to find JSON object within the response
                start = content.find('{')
                end = content.rfind('}')
                if start != -1 and end != -1:
                    content = content[start:end+1]
                    print(f"[DEBUG] Extracted JSON object: {content[:200]}...")
            
            import json
            data = json.loads(content)
            return data.get('city', address_dict.get('city','')), strip_country_prefix(data.get('province_code', address_dict.get('addressLine4') or address_dict.get('addressLine3','')))
        except openai.RateLimitError as e:
            print(f"[LLM] Rate limit exceeded, falling back to basic address handling: {str(e)}")
            return address_dict.get('city', ''), address_dict.get('addressLine4') or address_dict.get('addressLine3', '')
        except json.JSONDecodeError as e:
            print(f"[LLM] JSON decode error in {address_type} address normalization (attempt {attempt + 1}/3): {str(e)}. Position: {e.pos}, Line: {e.lineno}, Column: {e.colno}")
            time.sleep(2)
        except Exception as e:
            print(f"[LLM] Error normalizing {address_type} address (attempt {attempt + 1}/3): {str(e)}. Error type: {type(e).__name__}. Retrying...")
            time.sleep(2)
    # Fallback to original after all retries
    print(f"[LLM] Single address normalization failed after 3 attempts, using original values.")
    return address_dict.get('city', ''), address_dict.get('addressLine4') or address_dict.get('addressLine3', '')

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
    elif len(code) == 2:
        return code
    # Try to match US state code
    state_code = US_STATE_CODES.get(code.upper())
    if state_code:
        return state_code
    return ''

def normalize_spanish_postal_code(postcode, country):
    """Normalize Spanish postal codes to ensure 5 digits"""
    if not postcode or not country:
        return postcode
    
    # Check if it's a Spanish address (either ESP or ES)
    if country.upper() not in ['ESP', 'ES']:
        return postcode
        
    # Remove any non-digit characters
    digits = ''.join(c for c in str(postcode) if c.isdigit())
    if not digits:
        return postcode
        
    # If less than 5 digits, pad with leading zeros
    return digits.zfill(5)

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
        # Normalize Spanish postal codes before adding to the index
        a['postcode'] = normalize_spanish_postal_code(a.get('postcode', ''), a.get('country', ''))
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
