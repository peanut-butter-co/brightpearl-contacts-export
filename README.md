# Brightpearl Contact Export Tools

A collection of Python scripts for exporting and managing contact data from Brightpearl.

## Setup

1. Create a `.env` file in the project root with your Brightpearl credentials:
```
BRIGHTPEARL_ACCOUNT=your_account_name
BRIGHTPEARL_API_TOKEN=your_api_token
BRIGHTPEARL_API_DOMAIN=your_api_domain
BRIGHTPEARL_APP_REF=your_app_ref
```

2. Install required dependencies:
```bash
pip install requests python-dotenv
```

## Available Scripts

### export_b2b_contacts.py
Exports all B2B contacts from Brightpearl into CSV files.

**Usage:**
```bash
python export_b2b_contacts.py
```

**Output:**
- `exports/contacts.csv`: Basic contact information
- `exports/addresses.csv`: All addresses associated with contacts
- `exports/companies.csv`: Company information for contacts

The script will show real-time progress as it processes contacts and creates the export files.

**Features:**
- Fetches all contacts tagged as 'B2B'
- Handles pagination automatically
- Includes rate limiting and retry logic
- Processes addresses and company relationships
- Creates separate CSV files for contacts, addresses, and companies
- Shows real-time progress updates

### export_orders.py
Exports all orders from a specific department (default: department_id=11) into a CSV file.

**Usage:**
```bash
python export_orders.py
```

**Output:**
- `exports/orders.csv`: Order information with one row per line item

The script will show real-time progress as it processes orders and creates the export file.

**Features:**
- Fetches all orders from specified department
- Handles pagination automatically
- Includes rate limiting and retry logic
- Processes order line items
- Creates a CSV file with detailed order information
- Shows real-time progress updates
- Each line item gets its own row with shared order details

### get_contact.py
Displays detailed information about a single contact.

**Usage:**
```bash
python get_contact.py <contact_id>
```

**Example:**
```bash
python get_contact.py 12345
```

**Output:**
Displays comprehensive contact information including:
- Basic contact details (name, ID, title)
- Communication details (emails, phones, websites)
- Organization information
- All associated addresses
- Relationship status (customer/supplier/staff)

### get_order.py
Displays detailed information about a single order.

**Usage:**
```bash
python get_order.py <order_id>
```

**Example:**
```bash
python get_order.py 54321
```

**Output:**
Displays comprehensive order information including:
- Basic order details (ID, type, reference, status, totals)
- Customer, delivery, and billing party details
- All order rows (products, quantities, values)
- Custom fields if present

### convert_contacts.py

This script converts exported Brightpearl contact data into Shopify B2B format. It processes companies, contacts, and addresses, and includes special handling for:

- European province codes (e.g., Spanish provinces like "M" for Madrid)
- US state codes (e.g., "NY" for New York)
- Spanish postal codes (automatically normalized to 5 digits)
- Multiple addresses per contact
- Company and contact relationships

#### Prerequisites

1. Python 3.x
2. Required Python packages (install via pip):
   ```bash
   pip install openai python-dotenv
   ```

3. OpenAI API key:
   - Create a `.env` file in the script directory
   - Add your OpenAI API key:
     ```
     OPENAI_API_KEY=your-api-key-here
     ```

#### Input Files

Place the following CSV files in an `exports` directory:

1. `companies.csv` - Company information
2. `contacts.csv` - Contact details including custom fields
3. `addresses.csv` - Address information

#### Usage

1. Ensure your input files are in the `exports` directory
2. Run the script:
   ```bash
   python convert_contacts.py
   ```
3. The converted file will be created in the `converted` directory as `companies.csv`

#### Output Format

The script generates a Shopify B2B compatible CSV file with the following features:

- One row per unique shipping address
- Normalized city names and province/state codes
- Properly formatted Spanish postal codes (5 digits)
- Correct mapping of European province and US state codes
- Company and contact relationships preserved
- Custom fields included (e.g., Wholesale status)

#### Field Mapping

The output CSV includes the following Shopify B2B fields:

- Company Information:
  - Name
  - Command
  - Main Contact: Customer ID

- Location Information:
  - Name
  - Command
  - Phone
  - Tax Settings
  - Shipping/Billing Details

- Customer Information:
  - Email
  - First/Last Name
  - Location Role

- Metafields:
  - brightpearl.contact_id
  - brightpearl.wholesale

#### Error Handling

The script includes robust error handling for:
- Missing or invalid input files
- API rate limits
- JSON parsing errors
- Address normalization issues

If the OpenAI API is unavailable or rate-limited, the script will fall back to using original address data.

#### Notes

- Spanish postal codes are automatically padded with leading zeros if needed (e.g., "8700" → "08700")
- Province codes are normalized to remove country prefixes (e.g., "ES-M" → "M")
- The script uses GPT-3.5 Turbo for address normalization
- Multiple retries are implemented for API calls
- Batch processing is used to optimize API usage

## Error Handling

All scripts include:
- API rate limiting protection
- Error reporting for failed requests
- Validation of environment variables
- Proper UTF-8 encoding support

## Notes

- The export scripts process data in batches to avoid API rate limits
- All dates and times are in UTC
- Address types are marked as BIL (Billing), DEL (Delivery), and DEF (Default)
- Company relationships are preserved in the exports
- Order exports include one row per line item, with order details repeated for each line
