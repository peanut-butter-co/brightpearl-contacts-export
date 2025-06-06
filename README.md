# Brightpearl API Integration

This project contains Python scripts to interact with the Brightpearl API.

## Setup

1. **Clone the repository:**
   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install dependencies:**
   ```sh
   pip install requests python-dotenv
   ```

3. **Environment Variables:**
   - Copy `.env.example` to `.env`:
     ```sh
     cp .env.example .env
     ```
   - Edit `.env` and fill in your Brightpearl credentials:
     ```env
     BRIGHTPEARL_ACCOUNT=your-account-id
     BRIGHTPEARL_API_TOKEN=your-api-token
     ```

## Usage

### Test Connection
Run the following command to test your connection to the Brightpearl API:
```sh
python test_connection.py
```

### Export B2B Contacts
To export contacts with the "B2B" tag and generate CSV files, run:
```sh
python export_b2b_contacts.py
```
This will create three CSV files:
- `contacts.csv`: Basic contact information.
- `addresses.csv`: Address details for each contact.
- `companies.csv`: Company information for linked contacts.

## Notes
- Ensure your `.env` file is not committed to version control.
- For more details, refer to the individual script files.
