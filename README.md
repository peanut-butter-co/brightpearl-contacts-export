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
