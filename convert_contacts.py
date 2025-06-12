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
CUSTOMERS_CSV = os.path.join(CONVERTED_DIR, 'customers.csv')
NORMALIZED_ADDRESSES_CSV = os.path.join(CONVERTED_DIR, 'normalized_addresses.csv')

# Output columns as per Shopify example
OUTPUT_COLUMNS = [
    'Name', 'Command', 'Main Contact: Customer ID',
    'Location: Name', 'Location: Command', 'Location: Phone', 'Location: Original Phone', 'Location: Locale', 'Location: Tax ID', 'Location: Tax Setting', 'Location: Tax Exemptions',
    'Location: Allow Shipping To Any Address', 'Location: Checkout To Draft', 'Location: Checkout Payment Terms', 'Location: Checkout Pay Now Only',
    'Location: Shipping First Name', 'Location: Shipping Last Name', 'Location: Shipping Recipient', 'Location: Shipping Phone', 'Location: Original Shipping Phone', 'Location: Shipping Address 1', 'Location: Shipping Address 2', 'Location: Shipping Zip', 'Location: Shipping City', 'Location: Shipping Province Code', 'Location: Shipping Country Code',
    'Location: Billing First Name', 'Location: Billing Last Name', 'Location: Billing Recipient', 'Location: Billing Phone', 'Location: Original Billing Phone', 'Location: Billing Address 1', 'Location: Billing Address 2', 'Location: Billing Zip', 'Location: Billing City', 'Location: Billing Province Code', 'Location: Billing Country Code',
    'Location: Catalogs', 'Location: Catalogs Command',
    'Customer: Email', 'Customer: Command', 'Customer: Location Role',
    'Metafield: brightpearl.contact_id [single_line_text_field]', 'Metafield: brightpearl.wholesale [boolean]'
]

CUSTOMERS_COLUMNS = [
    'Email', 'Command', 'First Name', 'Last Name',
    'State', 'Verified Email', 'Tax Exempt'
]

# Expected columns for each input file
COMPANIES_COLUMNS = ['companyId', 'companyName', 'email', 'phone', 'website', 'isPrimaryContact', 'priceListId', 'nominalCode', 'taxCodeId', 'creditTermDays', 'currencyId', 'discountPercentage', 'creditTermTypeId', 'taxNumber']
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

# Country code to phone prefix mapping
COUNTRY_PHONE_PREFIXES = {
    'ESP': '34', 'ES': '34',  # Spain
    'GBR': '44', 'GB': '44',  # UK
    'FRA': '33', 'FR': '33',  # France
    'DEU': '49', 'DE': '49',  # Germany
    'ITA': '39', 'IT': '39',  # Italy
    'PRT': '351', 'PT': '351',  # Portugal
    'NLD': '31', 'NL': '31',  # Netherlands
    'BEL': '32', 'BE': '32',  # Belgium
    'CHE': '41', 'CH': '41',  # Switzerland
    'AUT': '43', 'AT': '43',  # Austria
    'IRL': '353', 'IE': '353',  # Ireland
    'USA': '1', 'US': '1',  # United States
}

BATCH_SIZE = 10

NORMALIZED_ADDRESSES_COLUMNS = [
    'addressId', 'addressLine1', 'addressLine2', 'postcode', 'country',
    'normalized_city', 'normalized_province_code', 'last_updated'
]

def strip_country_prefix(province_code):
    """Strip country prefix from province codes (e.g., 'ES-M' becomes 'M')"""
    if not province_code:
        return ''
    parts = province_code.split('-')
    return parts[-1] if len(parts) > 1 else province_code

def load_normalized_addresses():
    """Load previously normalized addresses from cache file"""
    normalized = {}
    if os.path.exists(NORMALIZED_ADDRESSES_CSV):
        print(f"[CACHE] Loading normalized addresses from {NORMALIZED_ADDRESSES_CSV}")
        with open(NORMALIZED_ADDRESSES_CSV, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if not row['addressId']:  # Skip entries without addressId
                    continue
                normalized[row['addressId']] = (
                    row['addressLine1'],
                    row['addressLine2'],
                    row['postcode'],
                    row['country'],
                    row['normalized_city'],
                    row['normalized_province_code']
                )
                count += 1
            print(f"[CACHE] Loaded {count} normalized addresses")
    else:
        print(f"[CACHE] No cache file found at {NORMALIZED_ADDRESSES_CSV}")
    return normalized

def save_normalized_addresses(normalized_cache, address_data):
    """Save normalized addresses to cache file"""
    print(f"[CACHE] Saving normalized addresses to {NORMALIZED_ADDRESSES_CSV}")
    print(f"[CACHE] Cache contains {len(normalized_cache)} addresses")
    print(f"[CACHE] Current batch contains {len(address_data)} addresses")
    
    # Create a set to track unique addresses we've already saved
    saved_addresses = set()
    rows = []
    
    # First add all addresses from the current batch
    batch_saved = 0
    for addr in address_data:
        addr_id = addr.get('addressId', '')
        if not addr_id:  # Skip addresses without ID
            continue
            
        if addr_id in normalized_cache:
            saved_addresses.add(addr_id)
            addr_line1, addr_line2, postcode, country, city, province = normalized_cache[addr_id]
            rows.append({
                'addressId': addr_id,
                'addressLine1': addr_line1,
                'addressLine2': addr_line2,
                'postcode': postcode,
                'country': convert_country_code(country),  # Convert to 2-letter country code
                'normalized_city': city,
                'normalized_province_code': province,
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
            })
            batch_saved += 1
    
    print(f"[CACHE] Saved {batch_saved} addresses from current batch")
    
    # Now add any remaining addresses from the cache that weren't in address_data
    cache_saved = 0
    for addr_id, (addr_line1, addr_line2, postcode, country, city, province) in normalized_cache.items():
        if addr_id not in saved_addresses:
            rows.append({
                'addressId': addr_id,
                'addressLine1': addr_line1,
                'addressLine2': addr_line2,
                'postcode': postcode,
                'country': convert_country_code(country),  # Convert to 2-letter country code
                'normalized_city': city,
                'normalized_province_code': province,
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
            })
            cache_saved += 1
    
    print(f"[CACHE] Saved {cache_saved} additional addresses from cache")
    print(f"[CACHE] Total addresses saved: {len(rows)}")
    
    # Write all rows to the cache file
    with open(NORMALIZED_ADDRESSES_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=NORMALIZED_ADDRESSES_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def normalize_addresses_llm_batch(addresses, address_type):
    if not OPENAI_API_KEY or not addresses:
        return [(
            a.get('city', ''),
            a.get('addressLine4') or a.get('addressLine3', '')
        ) for a in addresses]

    # Load cached normalizations
    normalized_cache = load_normalized_addresses()
    results = []
    addresses_to_normalize = []
    address_indices = []

    # Check cache first
    print("[CACHE] Checking addresses against cache...")
    cache_hits = 0
    for i, addr in enumerate(addresses):
        addr_id = addr.get('addressId', '')
        if addr_id and addr_id in normalized_cache:
            cache_hits += 1
            addr_line1, addr_line2, postcode, country, city, province = normalized_cache[addr_id]
            print(f"[CACHE] Hit: {addr_line1} ({postcode}) [ID: {addr_id}]")
            results.append((city, province, country))
        else:
            print(f"[CACHE] Miss: {addr.get('addressLine1', '')} ({addr.get('postcode', '')}) [ID: {addr_id}]")
            addresses_to_normalize.append(addr)
            address_indices.append(i)

    print(f"[CACHE] Cache hits: {cache_hits}/{len(addresses)}")

    if not addresses_to_normalize:
        print(f"[CACHE] All {len(addresses)} addresses found in cache")
        return results

    print(f"[LLM] Normalizing {len(addresses_to_normalize)} addresses not found in cache")
    
    # Format addresses in a clear, structured way
    formatted_addresses = ""
    for idx, a in enumerate(addresses_to_normalize, 1):
        normalized_postcode = normalize_spanish_postal_code(a.get('postcode', ''), a.get('country', ''))
        formatted_addresses += f"{idx}.\n"
        formatted_addresses += f"  Address: {a.get('addressLine1','')}\n"
        if a.get('addressLine2'):
            formatted_addresses += f"  Address 2: {a.get('addressLine2','')}\n"
        formatted_addresses += f"  City: {a.get('city','')}\n"
        if a.get('addressLine4') or a.get('addressLine3'):
            formatted_addresses += f"  Province/State: {a.get('addressLine4') or a.get('addressLine3','')}\n"
        formatted_addresses += f"  Postal Code: {normalized_postcode}\n"
        formatted_addresses += f"  Country: {a.get('country','')}\n\n"

    prompt = f"""
You are a JSON-only geolocation API that normalizes addresses into a strict format.

For each of the following {len(addresses_to_normalize)} addresses, return a JSON array where each item contains:
- "city": the full, normalized city name
- "province_code": the official state/province code, based on the rules below

Rules:
1. For **Spain and other European countries**:
   - Use the **ISO 3166-2 province-level code**, but **omit the country prefix**.
     - ✅ Example: "MA" for Málaga (not "ES-MA")
     - ✅ Example: "M" for Madrid
     - ✅ Example: "B" for Barcelona
   - **Do NOT confuse provinces with autonomous communities**.
     - Madrid → "M", not "MD"
     - Andalucía → Not acceptable (too broad)
   - Use the **postal code and city name together** to resolve ambiguity.

2. For **US addresses**:
   - Use the standard **two-letter state abbreviation** (e.g., "NY" for New York, "CA" for California).

3. Response requirements:
   - Return ONLY a valid **JSON array** with exactly {len(addresses_to_normalize)} items.
   - Each item must have exactly two fields: `"city"` and `"province_code"`.
   - Do **not** include explanations, comments, or any extra text — JSON only.

Example format:
[
  {{"city": "Madrid", "province_code": "M"}},
  {{"city": "New York", "province_code": "NY"}}
]

Addresses:
{formatted_addresses}
"""

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
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
                start = content.find('[')
                end = content.rfind(']')
                if start != -1 and end != -1:
                    content = content[start:end+1]
                    print(f"[DEBUG] Extracted JSON array: {content[:200]}...")
            
            import json
            data = json.loads(content)
            if isinstance(data, list) and len(data) == len(addresses_to_normalize):
                # Update cache with new normalizations
                for i, item in enumerate(data):
                    addr = addresses_to_normalize[i]
                    addr_id = addr.get('addressId', '')
                    if addr_id:
                        normalized_value = (
                            addr_id,  # Store the addressId in the cache
                            item.get('city', addr.get('city','')),
                            strip_country_prefix(item.get('province_code', addr.get('addressLine4') or addr.get('addressLine3',''))),
                            addr.get('country', '')
                        )
                        normalized_cache[addr_id] = normalized_value
                        results.insert(address_indices[i], (normalized_value[1], normalized_value[2], normalized_value[3]))  # Only city, province, and country for results

                # Save updated cache
                save_normalized_addresses(normalized_cache, addresses)
                return results
            else:
                print(f"[LLM] Warning: Response length mismatch. Expected {len(addresses_to_normalize)}, got {len(data) if isinstance(data, list) else 'non-list'}")
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
        return (
            address_dict.get('city', ''),
            address_dict.get('addressLine4') or address_dict.get('addressLine3', ''),
            address_dict.get('country', '')
        )

    # Check cache first
    addr_id = address_dict.get('addressId', '')
    if addr_id:
        normalized_cache = load_normalized_addresses()
        if addr_id in normalized_cache:
            addr_line1, addr_line2, postcode, country, city, province = normalized_cache[addr_id]
            print(f"[CACHE] Hit in single request: {addr_line1} ({postcode}) [ID: {addr_id}]")
            return (city, province, country)
        else:
            print(f"[CACHE] Miss in single request: {address_dict.get('addressLine1', '')} ({address_dict.get('postcode', '')}) [ID: {addr_id}]")

    # Build address string separately to avoid f-string issues
    address_str = f"Address: {address_dict.get('addressLine1','')}\n"
    if address_dict.get('addressLine2'):
        address_str += f"Address 2: {address_dict.get('addressLine2','')}\n"
    address_str += f"City: {address_dict.get('city','')}\n"
    if address_dict.get('addressLine4') or address_dict.get('addressLine3'):
        address_str += f"Province/State: {address_dict.get('addressLine4') or address_dict.get('addressLine3','')}\n"
    address_str += f"Postal Code: {normalize_spanish_postal_code(address_dict.get('postcode', ''), address_dict.get('country', ''))}\n"
    address_str += f"Country: {address_dict.get('country','')}"

    prompt = """You are a JSON-only geolocation API that normalizes addresses into a strict format.

Rules:
1. For **Spain and other European countries**:
   - Use the **ISO 3166-2 province-level code**, but **omit the country prefix**.
     - ✅ Example: "MA" for Málaga (not "ES-MA")
     - ✅ Example: "M" for Madrid
     - ✅ Example: "B" for Barcelona
   - **Do NOT confuse provinces with autonomous communities**.
     - Madrid → "M", not "MD"
     - Andalucía → Not acceptable (too broad)
   - Use the **postal code and city name together** to resolve ambiguity.

2. For **US addresses**:
   - Use the standard **two-letter state abbreviation** (e.g., "NY" for New York, "CA" for California).

3. Response requirements:
   - Return ONLY a single JSON object
   - The object must have exactly two fields: `"city"` and `"province_code"`
   - Do **not** include explanations, comments, or any extra text — JSON only.

Example format:
{"city": "Madrid", "province_code": "M"}

Input address:
""" + address_str

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
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
            result = (
                data.get('city', address_dict.get('city','')),
                strip_country_prefix(data.get('province_code', address_dict.get('addressLine4') or address_dict.get('addressLine3',''))),
                address_dict.get('country', '')
            )

            # Update cache with the new normalization
            if addr_id:
                normalized_cache[addr_id] = (
                    address_dict.get('addressLine1', ''),
                    address_dict.get('addressLine2', ''),
                    address_dict.get('postcode', ''),
                    result[2],  # country
                    result[0],  # city
                    result[1]   # province
                )
                save_normalized_addresses(normalized_cache, [address_dict])

            return result

        except openai.RateLimitError as e:
            print(f"[LLM] Rate limit exceeded, falling back to basic address handling: {str(e)}")
            return (
                address_dict.get('city', ''),
                address_dict.get('addressLine4') or address_dict.get('addressLine3', ''),
                address_dict.get('country', '')
            )
        except json.JSONDecodeError as e:
            print(f"[LLM] JSON decode error in {address_type} address normalization (attempt {attempt + 1}/3): {str(e)}. Position: {e.pos}, Line: {e.lineno}, Column: {e.colno}")
            time.sleep(2)
        except Exception as e:
            print(f"[LLM] Error normalizing {address_type} address (attempt {attempt + 1}/3): {str(e)}. Error type: {type(e).__name__}. Retrying...")
            time.sleep(2)
    # Fallback to original after all retries
    print(f"[LLM] Single address normalization failed after 3 attempts, using original values.")
    return (
        address_dict.get('city', ''),
        address_dict.get('addressLine4') or address_dict.get('addressLine3', ''),
        address_dict.get('country', '')
    )

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
    """Return empty first name and full name as last name"""
    if not name:
        return '', ''
    return '', name.strip()

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

def normalize_phone_number(phone, country_code):
    """
    Normalize phone numbers to E.164 format based on country code.
    Returns tuple of (normalized_number, success_flag)
    """
    if not phone or not country_code:
        return '', False
        
    # Remove any non-digit characters
    digits = ''.join(c for c in str(phone) if c.isdigit())
    if not digits:
        return '', False
        
    # If already in international format, just ensure it starts with +
    if phone.startswith('+'):
        return phone, True
        
    # Get country prefix
    prefix = COUNTRY_PHONE_PREFIXES.get(country_code.upper())
    if not prefix:
        return phone, False  # Can't normalize without knowing the country prefix
        
    # Check if number already starts with country code
    if digits.startswith(prefix):
        return f"+{digits}", True
        
    # Handle special cases by country
    if country_code.upper() in ['ESP', 'ES']:
        # Spanish numbers are 9 digits
        if len(digits) == 9 and digits[0] in ['6', '7', '8', '9']:
            return f"+{prefix}{digits}", True
            
    elif country_code.upper() in ['GBR', 'GB']:
        # UK numbers: remove leading 0 if present
        if digits.startswith('0'):
            digits = digits[1:]
        return f"+{prefix}{digits}", True
        
    elif country_code.upper() in ['USA', 'US']:
        # US numbers should be 10 digits (area code + local number)
        # Remove leading 1 if present
        if digits.startswith('1') and len(digits) == 11:
            digits = digits[1:]
        # Check if we have a valid 10-digit number
        if len(digits) == 10:
            return f"+{prefix}{digits}", True
        
    elif country_code.upper() in ['FRA', 'FR', 'DEU', 'DE', 'ITA', 'IT', 'BEL', 'BE', 'CHE', 'CH']:
        # Most European numbers: remove leading 0 if present
        if digits.startswith('0'):
            digits = digits[1:]
        return f"+{prefix}{digits}", True
        
    # For any other country, just add the prefix if we have it
    if prefix:
        # Remove leading zeros
        digits = digits.lstrip('0')
        return f"+{prefix}{digits}", True
        
    return phone, False

def main():
    print("[INFO] Starting conversion...")
    os.makedirs(CONVERTED_DIR, exist_ok=True)
    print("[INFO] Loading input files...")
    companies = read_csv(COMPANIES_CSV)
    contacts = read_csv(CONTACTS_CSV)
    addresses = read_csv(ADDRESSES_CSV)
    print(f"[INFO] Loaded {len(companies)} companies, {len(contacts)} contacts, {len(addresses)} addresses.")

    # Load normalized addresses cache to get country codes
    normalized_cache = load_normalized_addresses()
    
    # Index contacts by companyId
    contacts_by_company = defaultdict(list)
    for c in contacts:
        contacts_by_company[c.get('companyId','')].append(c)

    # Index addresses by contactId and build contact-to-country mapping
    addresses_by_contact = defaultdict(list)
    contact_countries = {}  # Map contact IDs to their primary country
    for a in addresses:
        # Check if we have this address in normalized cache
        addr_id = a.get('addressId', '')
        if addr_id and addr_id in normalized_cache:
            addr_line1, addr_line2, postcode, country, city, province = normalized_cache[addr_id]
            a['country'] = country if country else a.get('country', '')
        
        # Normalize Spanish postal codes before adding to the index
        a['postcode'] = normalize_spanish_postal_code(a.get('postcode', ''), a.get('country', ''))
        contact_id = a.get('contactId','')
        addresses_by_contact[contact_id].append(a)
        
        # Use billing address country as primary if available, otherwise use first country found
        if a.get('isBilling','').upper() == 'TRUE':
            contact_countries[contact_id] = a.get('country', '')
        elif contact_id not in contact_countries:
            contact_countries[contact_id] = a.get('country', '')

    rows = []
    customers = []
    processed_emails = set()  # To track unique customers
    
    # Collect all shipping and billing addresses to normalize in batch
    all_ship_addrs = []
    all_bill_addrs = []
    ship_addr_refs = []  # (row_idx, addr_dict)
    bill_addr_refs = []
    print("[INFO] Building output rows and collecting addresses for normalization...")

    for company in companies:
        company_id = company.get('companyId', '')
        company_name = company.get('companyName', '')
        
        # Get all contacts for this company
        company_contacts = contacts_by_company.get(company_id, [])
        if not company_contacts:
            continue
            
        for contact in company_contacts:
            contact_id = contact.get('contactId', '')
            contact_name = contact.get('name', '')
            contact_email = contact.get('email', '')
            contact_phone = contact.get('phone', '')
            
            # Get addresses for this contact
            contact_addresses = addresses_by_contact.get(contact_id, [])
            
            # Try to get country code from normalized addresses first
            country_code = None
            for addr in contact_addresses:
                addr_id = addr.get('addressId', '')
                if addr_id and addr_id in normalized_cache:
                    addr_line1, addr_line2, postcode, country, city, province = normalized_cache[addr_id]
                    if country:
                        country_code = country
                        break
            
            # If no country code found in normalized addresses, fall back to contact_countries
            if not country_code:
                country_code = contact_countries.get(contact_id, '')

            # Normalize phone numbers
            normalized_phone, phone_success = normalize_phone_number(contact_phone, country_code)
            original_phone = contact_phone  # Store original phone before normalization
            
            # Get all delivery addresses for this contact
            delivery_addrs = [a for a in addresses_by_contact.get(contact_id, []) if a.get('isDelivery','').upper() == 'TRUE']
            
            # Remove duplicates based on normalized address
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
                original_ship_phone = original_phone  # Use original phone for shipping
                ship_phone, _ = normalize_phone_number(original_ship_phone, ship_addr.get('country', ''))
                ship_addr1 = ship_addr.get('addressLine1','')
                ship_addr2 = ship_addr.get('addressLine2','')
                ship_zip = ship_addr.get('postcode','')
                ship_city = ship_addr.get('city','')
                ship_prov = ship_addr.get('addressLine4') or ship_addr.get('addressLine3','')
                ship_country = convert_country_code(ship_addr.get('country',''))

                if billing_addr:
                    bill_first, bill_last = split_name(contact_name)
                    bill_recipient = company_name
                    original_bill_phone = original_phone  # Use original phone for billing
                    bill_phone, _ = normalize_phone_number(original_bill_phone, billing_addr.get('country', ''))
                    bill_addr1 = billing_addr.get('addressLine1','')
                    bill_addr2 = billing_addr.get('addressLine2','')
                    bill_zip = billing_addr.get('postcode','')
                    bill_city = billing_addr.get('city','')
                    bill_province = billing_addr.get('addressLine4') or billing_addr.get('addressLine3','')
                    bill_country = convert_country_code(billing_addr.get('country',''))

                    row = {
                        'Name': company_name,
                        'Command': 'NEW',
                        'Main Contact: Customer ID': '',
                        'Location: Name': ship_addr1,
                        'Location: Command': 'NEW',
                        'Location: Phone': ship_phone,
                        'Location: Original Phone': original_ship_phone,
                        'Location: Locale': 'es',
                        'Location: Tax ID': company.get('taxNumber', ''),
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
                        'Location: Original Shipping Phone': original_ship_phone,
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
                        'Location: Original Billing Phone': original_bill_phone,
                        'Location: Billing Address 1': bill_addr1,
                        'Location: Billing Address 2': bill_addr2,
                        'Location: Billing Zip': bill_zip,
                        'Location: Billing City': bill_city,
                        'Location: Billing Province Code': bill_province,
                        'Location: Billing Country Code': bill_country,
                        'Location: Catalogs': '',
                        'Location: Catalogs Command': 'MERGE',
                        'Customer: Email': contact_email,
                        'Customer: Command': 'MERGE',
                        'Customer: Location Role': 'Location admin',
                        'Metafield: brightpearl.contact_id [single_line_text_field]': contact_id,
                        'Metafield: brightpearl.wholesale [boolean]': contact.get('Wholesale', 'FALSE').upper(),
                    }
                else:
                    row = {
                        'Name': company_name,
                        'Command': 'NEW',
                        'Main Contact: Customer ID': '',
                        'Location: Name': ship_addr1,
                        'Location: Command': 'NEW',
                        'Location: Phone': ship_phone,
                        'Location: Original Phone': original_phone,
                        'Location: Locale': 'es',
                        'Location: Tax ID': company.get('taxNumber', ''),
                        'Location: Tax Setting': '',
                        'Location: Tax Exemptions': '',
                        'Location: Allow Shipping To Any Address': 'TRUE',
                        'Location: Checkout To Draft': 'FALSE',
                        'Location: Checkout Payment Terms': '',
                        'Location: Checkout Pay Now Only': 'FALSE',
                        'Location: Shipping First Name': ship_first,
                        'Location: Shipping Last Name': ship_last,
                        'Location: Shipping Recipient': company_name,
                        'Location: Shipping Phone': ship_phone,
                        'Location: Original Shipping Phone': original_phone,
                        'Location: Shipping Address 1': ship_addr1,
                        'Location: Shipping Address 2': ship_addr2,
                        'Location: Shipping Zip': ship_zip,
                        'Location: Shipping City': ship_city,
                        'Location: Shipping Province Code': ship_prov,
                        'Location: Shipping Country Code': ship_country,
                        'Location: Billing First Name': '',
                        'Location: Billing Last Name': '',
                        'Location: Billing Recipient': '',
                        'Location: Billing Phone': '',
                        'Location: Original Billing Phone': '',
                        'Location: Billing Address 1': '',
                        'Location: Billing Address 2': '',
                        'Location: Billing Zip': '',
                        'Location: Billing City': '',
                        'Location: Billing Province Code': '',
                        'Location: Billing Country Code': '',
                        'Location: Catalogs': '',
                        'Location: Catalogs Command': 'MERGE',
                        'Customer: Email': contact_email,
                        'Customer: Command': 'MERGE',
                        'Customer: Location Role': 'Ordering only',
                        'Metafield: brightpearl.contact_id [single_line_text_field]': contact_id,
                        'Metafield: brightpearl.wholesale [boolean]': contact.get('Wholesale', 'FALSE').upper(),
                    }

                rows.append(row)

                # Add customer record if we haven't seen this email before
                if contact_email and contact_email not in processed_emails:
                    processed_emails.add(contact_email)
                    customers.append({
                        'Email': contact_email,
                        'Command': 'MERGE',
                        'First Name': ship_first,
                        'Last Name': ship_last,
                        'State': 'enabled',
                        'Verified Email': 'TRUE',
                        'Tax Exempt': contact.get('Wholesale', 'FALSE').upper()
                    })

    print(f"[INFO] Collected {len(all_ship_addrs)} shipping and {len(all_bill_addrs)} billing addresses for normalization.")
    # Batch normalize shipping addresses
    for i in range(0, len(all_ship_addrs), BATCH_SIZE):
        batch = all_ship_addrs[i:i+BATCH_SIZE]
        print(f"[LLM] Normalizing shipping addresses {i+1}-{i+len(batch)}...")
        results = normalize_addresses_llm_batch(batch, 'shipping')
        for j, (city, prov, _) in enumerate(results):  # Unpack three values, ignore country
            row_idx, _ = ship_addr_refs[i+j]
            rows[row_idx]['Location: Shipping City'] = city
            rows[row_idx]['Location: Shipping Province Code'] = prov
    
    # Batch normalize billing addresses
    for i in range(0, len(all_bill_addrs), BATCH_SIZE):
        batch = all_bill_addrs[i:i+BATCH_SIZE]
        print(f"[LLM] Normalizing billing addresses {i+1}-{i+len(batch)}...")
        results = normalize_addresses_llm_batch(batch, 'billing')
        for j, (city, prov, _) in enumerate(results):  # Unpack three values, ignore country
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

    print(f"[INFO] Writing {len(customers)} customers to {CUSTOMERS_CSV}")
    with open(CUSTOMERS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CUSTOMERS_COLUMNS)
        writer.writeheader()
        for customer in customers:
            writer.writerow(customer)

    print("[SUCCESS] Conversion complete!")

if __name__ == '__main__':
    main()
