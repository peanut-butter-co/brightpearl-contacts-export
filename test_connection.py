import os
import requests
from dotenv import load_dotenv

load_dotenv()

BRIGHTPEARL_ACCOUNT = os.getenv('BRIGHTPEARL_ACCOUNT')
BRIGHTPEARL_API_TOKEN = os.getenv('BRIGHTPEARL_API_TOKEN')
BRIGHTPEARL_API_DOMAIN = os.getenv('BRIGHTPEARL_API_DOMAIN')
BRIGHTPEARL_APP_REF = os.getenv('BRIGHTPEARL_APP_REF')

if not all([BRIGHTPEARL_ACCOUNT, BRIGHTPEARL_API_TOKEN, BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_APP_REF]):
    print('Missing one or more required environment variables.')
    exit(1)

BASE_URL = 'https://{}/public-api/{}/contact-service/contact-search'.format(
    BRIGHTPEARL_API_DOMAIN, BRIGHTPEARL_ACCOUNT
)

headers = {
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'brightpearl-app-ref': BRIGHTPEARL_APP_REF,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

params = {
    'firstResult': 1,
    'maxResults': 1
}

print('Attempting to connect to:', BASE_URL)

try:
    response = requests.get(BASE_URL, headers=headers, params=params)
    print('Status code: {}'.format(response.status_code))
    try:
        print('Response JSON:', response.json())
    except Exception:
        print('Response content:', response.text)
    if response.ok:
        print('Connection successful!')
    else:
        print('Connection failed.')
except Exception as e:
    print('Error connecting to Brightpearl API:', e)
