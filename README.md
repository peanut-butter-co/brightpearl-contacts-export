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

## Error Handling

Both scripts include:
- API rate limiting protection
- Error reporting for failed requests
- Validation of environment variables
- Proper UTF-8 encoding support

## Notes

- The export script processes contacts in batches to avoid API rate limits
- All dates and times are in UTC
- Address types are marked as BIL (Billing), DEL (Delivery), and DEF (Default)
- Company relationships are preserved in the exports
