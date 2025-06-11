# -*- coding: utf-8 -*-

import os
import requests
import csv
from dotenv import load_dotenv
import sys
import time
from typing import List, Dict, Any, Optional

# Set default encoding to UTF-8 for Python 2 compatibility
# reload(sys)
# sys.setdefaultencoding('utf8')

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

# Constants for rate limiting
REQUEST_DELAY = 0.5  # Half second delay between requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds to wait between retries

# Let's use simple text indicators instead of emojis for better compatibility
INDICATORS = {
    'error': '[ERROR]',
    'warning': '[WARNING]',
    'info': '[INFO]',
    'success': '[SUCCESS]',
    'progress': '[PROGRESS]'
}

def make_request(url, headers, params=None):
    """
    Make a rate-limited request with retries, including exponential backoff for 429 and 503 errors
    """
    max_retries = 5
    delay = REQUEST_DELAY
    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status in (429, 503):
                if attempt < max_retries - 1:
                    print("{} {} error on {} (attempt {}/{}), waiting {} seconds...".format(
                        INDICATORS['warning'], status, url, attempt + 1, max_retries, delay * 2
                    ))
                    time.sleep(delay * 2)
                    delay *= 2
                    continue
            print("{} HTTP error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            return None
        except Exception as e:
            print("{} Request error on {}: {}".format(
                INDICATORS['error'],
                url,
                str(e)
            ))
            return None
    print("{} Max retries exceeded for {}".format(INDICATORS['error'], url))
    return None

def get_orders(department_id=11):
    """
    Get all orders for the specified department using pagination
    """
    url = "{}/order-service/order-search".format(BASE_URL)
    all_order_ids = []
    first_result = 1
    page_size = 200  # Maximum allowed by Brightpearl
    
    while True:
        params = {
            "firstResult": first_result,
            "maxResults": page_size,
            "departmentId": department_id
        }
        
        try:
            resp = make_request(url, HEADERS, params)
            if not resp:
                break
                
            data = resp.json()
            results = data.get('response', {}).get('results', [])
            
            if not results:
                break
                
            # Extract order IDs from this page
            page_order_ids = [result[0] for result in results if result and len(result) > 0]
            all_order_ids.extend(page_order_ids)
            
            current_page = ((first_result - 1) // page_size) + 1
            print("{} Page {}: {} orders".format(INDICATORS['info'], current_page, len(page_order_ids)))
            
            # Update first_result for next page
            first_result += len(page_order_ids)
            
            # If we got fewer results than requested, we're done
            if len(page_order_ids) < page_size:
                break
            
        except Exception as e:
            print("{} Error fetching orders page {}: {}".format(INDICATORS['error'], current_page, e))
            break
    
    total = len(all_order_ids)
    print("{} Found {} total orders for department {}".format(INDICATORS['success'], total, department_id))
    return all_order_ids

def get_order_details(order_id):
    """
    Get full order details including all line items
    """
    url = "{}/order-service/order/{}".format(BASE_URL, order_id)
    try:
        resp = make_request(url, HEADERS)
        if not resp:
            return None
            
        data = resp.json()
        order = data.get('response', [])[0] if data.get('response') else None
        if not order:
            return None
            
        # Debug: Print the first order structure
        if order_id == data.get('response', [])[0].get('orderId'):
            print("\n{} First order structure:".format(INDICATORS['info']))
            print(order)
            
        return order
    except Exception as e:
        print("{} Error fetching order {} details: {}".format(INDICATORS['error'], order_id, str(e)))
        return None

def write_orders_csv(orders):
    """
    Write orders data to CSV file, with one row per order line item
    """
    if not os.path.exists('exports'):
        os.makedirs('exports')

    # Define the new column order and headers
    columns = [
        'Order ID', 'Order Type', 'Status', 'Payment Status', 'Item name', 'Order row SKU', 'Quantity', 'Invoice', 'Ref', 'Tax status', 'Date created', 'Currency', 'Exchange rate',
        'Delivery name', 'Delivery company', 'Delivery street', 'Delivery suburb', 'Delivery city', 'Delivery state', 'Delivery postcode', 'Delivery country', 'Delivery telephone', 'Delivery mobile', 'Delivery email',
        'Billing name', 'Billing company', 'Billing Street', 'Billing Suburb', 'Billing City', 'Billing State', 'Billing Postcode', 'Billing Country', 'Billing telephone', 'Billing mobile', 'Billing email',
        'Contact ID', 'Product ID', 'Order list price',
        'Row net', 'Row tax', 'Row gross',
        'Item tax class', 'Tax Rate', 'Shipping Method Id', 'Stock Status Code', 'Allocation Status Code', 'Shipping Status Code'
    ]

    with open('exports/orders.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for order in orders:
            # Extract invoice info (first invoice if present)
            invoice = order.get('invoices', [{}])[0] if order.get('invoices') else {}
            invoice_number = invoice.get('invoiceReference', '')
            tax_date = invoice.get('taxDate', '')

            # Base order data shared across all rows
            base_order = {
                'Order ID': order.get('id', ''),
                'Order Type': order.get('orderTypeCode', ''),
                'Status': order.get('orderStatus', {}).get('name', ''),
                'Payment Status': order.get('orderPaymentStatus', ''),
                'Ref': order.get('reference', ''),
                'Tax status': order.get('state', {}).get('tax', ''),
                'Date created': order.get('createdOn', ''),
                'Currency': order.get('currency', {}).get('orderCurrencyCode', ''),
                'Exchange rate': order.get('currency', {}).get('exchangeRate', ''),
                'Invoice': invoice_number,
                'Tax date': tax_date,
                'Delivery name': order.get('parties', {}).get('delivery', {}).get('addressFullName', ''),
                'Delivery company': order.get('parties', {}).get('delivery', {}).get('companyName', ''),
                'Delivery street': order.get('parties', {}).get('delivery', {}).get('addressLine1', ''),
                'Delivery suburb': order.get('parties', {}).get('delivery', {}).get('addressLine2', ''),
                'Delivery city': order.get('parties', {}).get('delivery', {}).get('addressLine3', ''),
                'Delivery state': order.get('parties', {}).get('delivery', {}).get('addressLine4', ''),
                'Delivery postcode': order.get('parties', {}).get('delivery', {}).get('postalCode', ''),
                'Delivery country': order.get('parties', {}).get('delivery', {}).get('country', ''),
                'Delivery telephone': order.get('parties', {}).get('delivery', {}).get('telephone', ''),
                'Delivery mobile': order.get('parties', {}).get('delivery', {}).get('mobileTelephone', ''),
                'Delivery email': order.get('parties', {}).get('delivery', {}).get('email', ''),
                'Billing name': order.get('parties', {}).get('billing', {}).get('addressFullName', ''),
                'Billing company': order.get('parties', {}).get('billing', {}).get('companyName', ''),
                'Billing Street': order.get('parties', {}).get('billing', {}).get('addressLine1', ''),
                'Billing Suburb': order.get('parties', {}).get('billing', {}).get('addressLine2', ''),
                'Billing City': order.get('parties', {}).get('billing', {}).get('addressLine3', ''),
                'Billing State': order.get('parties', {}).get('billing', {}).get('addressLine4', ''),
                'Billing Postcode': order.get('parties', {}).get('billing', {}).get('postalCode', ''),
                'Billing Country': order.get('parties', {}).get('billing', {}).get('country', ''),
                'Billing telephone': order.get('parties', {}).get('billing', {}).get('telephone', ''),
                'Billing mobile': order.get('parties', {}).get('billing', {}).get('mobileTelephone', ''),
                'Billing email': order.get('parties', {}).get('billing', {}).get('email', ''),
                'Contact ID': order.get('parties', {}).get('billing', {}).get('contactId', ''),
            }

            order_rows = order.get('orderRows', {})
            for row_id, row in order_rows.items():
                # Row-level values
                product_price = row.get('productPrice', {})
                row_value = row.get('rowValue', {})
                row_net = row_value.get('rowNet', {})
                row_tax = row_value.get('rowTax', {})
                # EUR values (base values)
                base_net = row_net.get('value', '') if row_net.get('currencyCode', '') == 'EUR' else ''
                base_tax = row_tax.get('value', '') if row_tax.get('currencyCode', '') == 'EUR' else ''
                base_gross = ''  # Not directly available, can be calculated if needed
                # Item net (productPrice.value)
                item_net = product_price.get('value', '')
                # Item gross and item tax (not directly available, can be calculated if needed)
                item_gross = ''
                item_tax = ''
                # EUR item net/gross/tax (not directly available, can be calculated if needed)
                eur_item_net = item_net if product_price.get('currencyCode', '') == 'EUR' else ''
                eur_item_gross = ''
                eur_item_tax = ''
                # Row gross (rowNet + rowTax)
                try:
                    row_gross = str(float(row_net.get('value', 0)) + float(row_tax.get('value', 0)))
                except:
                    row_gross = ''
                eur_row_net = base_net
                eur_row_tax = base_tax
                try:
                    eur_row_gross = str(float(eur_row_net or 0) + float(eur_row_tax or 0))
                except:
                    eur_row_gross = ''

                order_row = base_order.copy()
                order_row.update({
                    'Item name': row.get('productName', ''),
                    'Order row SKU': row.get('productSku', ''),
                    'Quantity': row.get('quantity', {}).get('magnitude', ''),
                    'Product ID': row.get('productId', ''),
                    'Order list price': item_net,
                    'Row net': row_net.get('value', ''),
                    'Row tax': row_tax.get('value', ''),
                    'Row gross': row_gross,
                    'Item tax class': row_value.get('taxCode', ''),
                    'Tax Rate': row_value.get('taxRate', ''),
                    'Shipping Method Id': order.get('delivery', {}).get('shippingMethodId', ''),
                    'Stock Status Code': order.get('stockStatusCode', ''),
                    'Allocation Status Code': order.get('allocationStatusCode', ''),
                    'Shipping Status Code': order.get('shippingStatusCode', ''),
                    'Invoice': invoice_number,
                    'Tax date': tax_date,
                    'Billing mobile': order.get('parties', {}).get('billing', {}).get('mobileTelephone', ''),
                })
                # Remove columns not in the new columns list
                filtered_row = {col: order_row.get(col, '') for col in columns}
                writer.writerow(filtered_row)

def main():
    # For testing - set to 0 for unlimited orders
    TEST_LIMIT = 0
    
    print("\n{} Starting orders export...".format(INDICATORS['info']))
    order_ids = get_orders(department_id=11)
    if TEST_LIMIT:
        original_count = len(order_ids)
        order_ids = order_ids[:TEST_LIMIT]
        print("\n{} TEST MODE: Processing {} of {} orders\n".format(
            INDICATORS['warning'],
            len(order_ids),
            original_count
        ))
    else:
        print("\n{} Processing all {} orders\n".format(INDICATORS['info'], len(order_ids)))
    
    orders = []
    total_orders = len(order_ids)
    for idx, oid in enumerate(order_ids, 1):
        try:
            # Progress indicator (update on same line)
            sys.stdout.write("\r{} Progress: {}/{} orders processed".format(
                INDICATORS['progress'],
                idx,
                total_orders
            ))
            sys.stdout.flush()
            
            # Get order details
            order = get_order_details(oid)
            if not order:
                print("\n{} Skipping order ID {} - no details found".format(INDICATORS['warning'], oid))
                continue
                
            orders.append(order)
                
        except Exception as e:
            print("\n{} Error processing order {}: {}".format(INDICATORS['error'], oid, str(e)))
            continue

    # Print a newline after progress is complete
    print("\n")
    print("{} Writing export file:".format(INDICATORS['info']))
    print("- orders.csv: {} orders with line items".format(len(orders)))
    write_orders_csv(orders)
    print('\n{} Export complete!'.format(INDICATORS['success']))

if __name__ == '__main__':
    main()
