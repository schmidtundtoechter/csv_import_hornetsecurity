# Copyright (c) 2025, ahmad mohammad and contributors
# For license information, please see license.txt
# File: csv_import_hornetsecurity/csv_import_hornetsecurity/csv_import_hornetsecurity_settings/csv_import_hornetsecurity_settings.py

import frappe
from frappe.model.document import Document
from frappe.utils import today, add_months, flt, cint
import csv
import io
from datetime import datetime
import traceback
import base64
import re

class CSVImportHornetsecuritySettings(Document):
    def before_save(self):
        """Validate settings before save"""
        if self.tax_account:
            # Validate that the tax account exists
            if not frappe.db.exists("Account", self.tax_account):
                frappe.throw(f"Tax Account {self.tax_account} does not exist")

@frappe.whitelist()
def process_csv_import(doc_name, file_content, file_name):
    """Main function to process Hornetsecurity CSV import"""
    try:
        settings_doc = frappe.get_doc("CSV Import Hornetsecurity Settings", doc_name)
        
        # Handle file content - it might be base64 encoded or already a string
        if isinstance(file_content, str):
            try:
                # Try to decode as base64 first
                file_bytes = base64.b64decode(file_content)
                csv_text = file_bytes.decode('utf-8')
            except:
                # If base64 decode fails, assume it's already text
                csv_text = file_content
        else:
            # If it's bytes, decode directly
            csv_text = file_content.decode('utf-8')
        
        # Save CSV file to folder structure
        saved_file_name = save_csv_file_to_folder(file_content, file_name, "Hornetsecurity")
        
        # Parse CSV content with semicolon delimiter (UTF-8 format)
        csv_reader = csv.DictReader(io.StringIO(csv_text), delimiter=';')
        
        # Process data - Group by Customer Reference Number AND Product Code
        customer_product_data = {}
        total_licenses_before = 0
        errors = []
        csv_currency = "EUR"  # Default currency for Hornetsecurity
        
        # Process each row
        rows = list(csv_reader)
        
        for i, row in enumerate(rows):
            try:
                customer_ref_nr = row.get('Customer Reference Number', '').strip()
                product_code = row.get('Product Code', '').strip()
                licenses_count_str = row.get('Licenses Count', '0').strip()
                
                # Extract currency from first row (assuming all rows have same currency)
                if i == 0:
                    csv_currency = row.get('Currency', 'EUR').strip()
                
                if not customer_ref_nr:
                    errors.append(f"Missing Customer Reference Number in line {i+2}")
                    continue
                    
                if not product_code:
                    errors.append(f"Missing Product Code in line {i+2}")
                    continue
                
                # Convert licenses count and prices (German format)
                licenses_count = convert_german_number(licenses_count_str)
                total_licenses_before += abs(licenses_count)
                
                # Create unique key for customer + product combination
                key = f"{customer_ref_nr}|{product_code}"
                
                if key not in customer_product_data:
                    customer_product_data[key] = {
                        'customer_ref_nr': customer_ref_nr,
                        'product_code': product_code,
                        'rows': []
                    }
                    
                customer_product_data[key]['rows'].append(row)
                        
            except Exception as e:
                errors.append(f"Error processing row {i+2}: {str(e)}")
                continue
        
        # Group by customer for invoice creation (one invoice per customer)
        customer_invoices = {}
        
        for key, data in customer_product_data.items():
            customer_ref_nr = data['customer_ref_nr']
            
            if customer_ref_nr not in customer_invoices:
                customer_invoices[customer_ref_nr] = []
            
            # Aggregate quantities and amounts for this customer-product combination
            total_qty = 0
            total_amount = 0
            rate = 0
            product_name = ""
            
            for row in data['rows']:
                try:
                    qty = convert_german_number(row.get('Licenses Count', 0))
                    price_per_license = convert_german_number(row.get('Customer Price Per License', 0))
                    customer_total = convert_german_number(row.get('Customer Total', 0))
                    
                    total_qty += qty
                    total_amount += customer_total
                    rate = price_per_license  # Should be same for all rows of same product
                    product_name = row.get('Product', '').strip()
                    
                except Exception as e:
                    errors.append(f"Error aggregating data for {customer_ref_nr} - {data['product_code']}: {str(e)}")
                    continue
            
            if total_qty > 0:  # Only add if we have valid quantity
                customer_invoices[customer_ref_nr].append({
                    'product_code': data['product_code'],
                    'product_name': product_name,
                    'total_qty': total_qty,
                    'rate': rate,
                    'total_amount': total_amount
                })
        
        # Create invoices - RESILIENT APPROACH
        invoices_created = 0
        total_licenses_after = 0
        successful_customers = []
        
        for customer_ref_nr, items_data in customer_invoices.items():
            try:
                # Validate customer exists first
                customer = frappe.get_all('Customer', 
                    filters={'custom_interne_kundennummer': customer_ref_nr}, 
                    fields=['name', 'customer_name']
                )
                
                if not customer:
                    errors.append(f"Customer not found for reference number: {customer_ref_nr}")
                    continue
                
                # Validate all items exist before creating invoice
                valid_items = []
                for item_data in items_data:
                    product_code = item_data['product_code']
                    
                    # Find item by Product Code (external article number)
                    item = frappe.get_all('Item', 
                        filters={'custom_externe_artikelnummer': product_code}, 
                        fields=['name', 'item_name', 'description']
                    )
                    
                    if not item:
                        errors.append(f"Item not found for product code: {product_code} (Customer: {customer_ref_nr})")
                        continue
                    
                    # Check if quantity is valid
                    if item_data['total_qty'] <= 0:
                        errors.append(f"Invalid quantity {item_data['total_qty']} for product {product_code} (Customer: {customer_ref_nr})")
                        continue
                    
                    item_data['item_code'] = item[0]['name']
                    item_data['item_name'] = item[0].get('item_name', '')
                    item_data['description'] = item[0].get('description', '')
                    valid_items.append(item_data)
                
                # Only create invoice if we have valid items
                if valid_items:
                    invoice = create_hornetsecurity_sales_invoice_safe(customer_ref_nr, valid_items, settings_doc, errors, csv_currency)
                    if invoice:
                        invoices_created += 1
                        successful_customers.append(customer_ref_nr)
                        for item in invoice.items:
                            total_licenses_after += flt(item.qty)
                else:
                    errors.append(f"No valid items found for customer {customer_ref_nr}")
                    
            except Exception as e:
                errors.append(f"Error processing customer {customer_ref_nr}: {str(e)}")
                continue
        
        # Generate report
        report = generate_hornetsecurity_report(total_licenses_before, total_licenses_after, invoices_created, errors, successful_customers)
        
        # Update history and results with file link
        settings_doc.append('hornetsecurity_importhistorie', {
            'importdatum': datetime.now(),
            'name_der_csv': saved_file_name  # Now links to File doctype
        })
        
        settings_doc.append('hornetsecurity_importergebnis', {
            'datum': datetime.now(),
            'name_der_csv': saved_file_name,  # Now links to File doctype
            'importergebnis': report
        })
        
        settings_doc.save()
        
        return {
            'status': 'success',
            'message': f"Import completed. {invoices_created} invoices created successfully. {len(errors)} errors logged.",
            'invoices_created': invoices_created,
            'errors_count': len(errors),
            'report': report
        }
        
    except Exception as e:
        frappe.log_error(f"Hornetsecurity CSV Import Error: {str(e)}\n{traceback.format_exc()}")
        return {
            'status': 'error',
            'message': f"Import failed: {str(e)}"
        }

def convert_german_number(number_str):
    """Convert German number format (4,5) to float (4.5)"""
    if not number_str:
        return 0.0
    try:
        return flt(str(number_str).replace(',', '.'))
    except:
        return 0.0

def get_currency_mapping():
    """Currency mapping from CSV values to ERPNext currency codes"""
    # Mapping for common currencies - add more as needed
    currency_map = {
        # Hornetsecurity uses ISO codes (likely no mapping needed)
        "EUR": "EUR",
        "USD": "USD",
        "CHF": "CHF",
        "GBP": "GBP",
        "JPY": "JPY",
        "CNY": "CNY",
        "AUD": "AUD",
        "CAD": "CAD",
        
        # Full currency names (in case they're used)
        "Euro": "EUR",
        "US Dollar": "USD", 
        "United States Dollar": "USD",
        "Swiss Franc": "CHF",
        "Pound Sterling": "GBP",
        "British Pound": "GBP",
        "Japanese Yen": "JPY",
        "Chinese Yuan": "CNY",
        "Australian Dollar": "AUD",
        "Canadian Dollar": "CAD"
    }
    return currency_map

def get_company_default_currency():
    """Get default currency from the current company"""
    try:
        # Get the default company
        company = frappe.defaults.get_user_default("Company")
        if not company:
            # Fallback to first company found
            companies = frappe.get_all("Company", fields=["name"], limit=1)
            company = companies[0]["name"] if companies else None
        
        if company:
            return frappe.get_cached_value("Company", company, "default_currency") or "EUR"
        
        return "EUR"  # Final fallback
        
    except Exception as e:
        frappe.log_error(f"Error getting company default currency: {str(e)}")
        return "EUR"

def get_invoice_currency(csv_currency):
    """Get ERPNext currency code from CSV currency value"""
    try:
        currency_map = get_currency_mapping()
        default_company_currency = get_company_default_currency()
        
        # Clean the CSV currency value
        csv_currency = str(csv_currency).strip() if csv_currency else ""
        
        # Try to map the currency
        if csv_currency in currency_map:
            return currency_map[csv_currency]
        
        # If currency exists in ERPNext as-is, use it
        if frappe.db.exists("Currency", csv_currency):
            return csv_currency
            
        # Fallback to company default currency
        frappe.log_error(f"Unknown currency '{csv_currency}', using default: {default_company_currency}")
        return default_company_currency
        
    except Exception as e:
        frappe.log_error(f"Error mapping currency '{csv_currency}': {str(e)}")
        return get_company_default_currency()

def ensure_currency_exchange_rate(from_currency, to_currency, exchange_date=None):
    """Ensure currency exchange rate exists, return rate if found"""
    try:
        if from_currency == to_currency:
            # Same currency, no exchange rate needed
            return 1.0
            
        if not exchange_date:
            exchange_date = today()
            
        # Check if exchange rate already exists
        existing_rate = frappe.get_all('Currency Exchange',
            filters={
                'from_currency': from_currency,
                'to_currency': to_currency,
                'date': exchange_date
            },
            fields=['exchange_rate']
        )
        
        if existing_rate:
            return existing_rate[0]['exchange_rate']
        
        # No exchange rate found - log warning and return None
        frappe.log_error(f"No exchange rate found for {from_currency} to {to_currency} on {exchange_date}")
        return None
        
    except Exception as e:
        frappe.log_error(f"Error checking exchange rate {from_currency} to {to_currency}: {str(e)}")
        return None
    

    
def create_app_folder_if_not_exists(app_name):
    """Create folder for app in File doctype if it doesn't exist"""
    try:
        folder_name = f"{app_name} CSV Imports"
        
        # Check if folder already exists
        existing_folder = frappe.get_all('File', 
            filters={
                'file_name': folder_name,
                'is_folder': 1
            }, 
            fields=['name']
        )
        
        if existing_folder:
            return existing_folder[0]['name']
        
        # Create new folder
        folder_doc = frappe.new_doc('File')
        folder_doc.file_name = folder_name
        folder_doc.is_folder = 1
        folder_doc.folder = 'Home'
        folder_doc.insert(ignore_permissions=True)
        
        return folder_doc.name
        
    except Exception as e:
        frappe.log_error(f"Error creating folder for {app_name}: {str(e)}")
        return None

def save_csv_file_to_folder(file_content, file_name, app_name):
    """Save CSV file to app-specific folder and return file doc name"""
    try:
        # Create or get app folder
        folder_name = create_app_folder_if_not_exists(app_name)
        if not folder_name:
            return file_name  # Fallback to original filename if folder creation fails
        
        # Create unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_filename = f"{timestamp}_{file_name}"
        
        # Decode base64 content if needed
        if isinstance(file_content, str):
            try:
                file_bytes = base64.b64decode(file_content)
            except:
                file_bytes = file_content.encode('utf-8')
        else:
            file_bytes = file_content
        
        # Create file doc
        file_doc = frappe.new_doc('File')
        file_doc.file_name = unique_filename
        file_doc.folder = folder_name
        file_doc.content = file_bytes
        file_doc.is_private = 0  # Make it accessible
        file_doc.insert(ignore_permissions=True)
        
        return file_doc.name
        
    except Exception as e:
        frappe.log_error(f"Error saving CSV file {file_name}: {str(e)}")
        return file_name  # Fallback to original filename

def get_dynamic_tax_rate(settings_doc):
    """Get tax rate from dynamic tax account field"""
    try:
        if not settings_doc.tax_account:
            frappe.log_error("No tax account configured in settings")
            return 19.0  # Default fallback
        
        account = frappe.get_doc("Account", settings_doc.tax_account)
        
        # Check various possible tax rate fields
        if hasattr(account, 'tax_rate') and account.tax_rate:
            return flt(account.tax_rate)
        elif hasattr(account, 'rate') and account.rate:
            return flt(account.rate)
        else:
            # Extract rate from account name if pattern exists (e.g., "19 %" in name)
            rate_match = re.search(r'(\d+(?:\.\d+)?)\s*%', account.account_name)
            if rate_match:
                return flt(rate_match.group(1))
            
            return 19.0  # Default fallback
            
    except Exception as e:
        frappe.log_error(f"Error getting tax rate from account {settings_doc.tax_account}: {str(e)}")
        return 19.0  # Default fallback

def create_hornetsecurity_sales_invoice_safe(customer_ref_nr, items_data, settings_doc, errors, csv_currency):
    """Create sales invoice for Hornetsecurity customer - SAFE VERSION with Currency"""
    
    try:
        # Get customer (already validated to exist)
        customer = frappe.get_all('Customer', 
            filters={'custom_interne_kundennummer': customer_ref_nr}, 
            fields=['name', 'customer_name']
        )[0]
        
        # Get company default currency
        company_currency = get_company_default_currency()
        
        # Determine invoice currency from CSV
        invoice_currency = get_invoice_currency(csv_currency)
        
        # Ensure exchange rate exists
        ensure_currency_exchange_rate(invoice_currency, company_currency)
        
        # Create sales invoice
        invoice = frappe.new_doc('Sales Invoice')
        invoice.customer = customer['name']
        invoice.currency = invoice_currency  # SET THE CURRENCY
        invoice.posting_date = today()
        invoice.due_date = add_months(today(), 1)
        invoice.update_stock = 0
        
        # Get customer discount if available
        customer_discount_percentage = get_customer_discount(customer['customer_name'], settings_doc.hornetsecurity_rabattwerte_je_kunde)
        
        # Add items to invoice
        items_added = 0
        for item_data in items_data:
            try:
                # Add item to invoice
                invoice.append('items', {
                    'item_code': item_data['item_code'],
                    'customer_item_code': item_data['product_code'],
                    'description': item_data.get('description') or item_data.get('item_name') or item_data.get('product_name'),
                    'qty': item_data['total_qty'],
                    'rate': item_data['rate'],
                    'amount': item_data['total_amount']
                })
                items_added += 1
                
            except Exception as e:
                errors.append(f"Error adding item {item_data['product_code']} to invoice for customer {customer_ref_nr}: {str(e)}")
                continue
        
        if items_added == 0:
            return None  # No valid items added
        
        # Apply customer discount at invoice level
        if customer_discount_percentage > 0:
            invoice.additional_discount_percentage = customer_discount_percentage
        
        # Add taxes with dynamic rate from settings
        try:
            if not settings_doc.tax_account:
                errors.append(f"No tax account configured for customer {customer_ref_nr}")
            else:
                tax_rate = get_dynamic_tax_rate(settings_doc)
                
                invoice.append('taxes', {
                    'charge_type': 'On Net Total',
                    'account_head': settings_doc.tax_account,
                    'rate': tax_rate,
                    'description': f'VAT {tax_rate}%'
                })
                
        except Exception as e:
            errors.append(f"Error adding tax to invoice for customer {customer_ref_nr}: {str(e)}")
        
        # Calculate totals 
        try:
            invoice.run_method('calculate_taxes_and_totals')
        except Exception as e:
            errors.append(f"Error calculating totals for customer {customer_ref_nr}: {str(e)}")
        
        # Check if invoice should be suppressed (zero amount)
        if settings_doc.nullrechnungen_unterdruecken and flt(invoice.grand_total) == 0:
            return None
        
        # Save invoice
        invoice.insert(ignore_permissions=True)
        
        return invoice
        
    except Exception as e:
        errors.append(f"Error creating invoice for customer {customer_ref_nr}: {str(e)}")
        return None

def get_customer_discount(customer_name, discount_table):
    """Get customer discount percentage"""
    try:
        for row in discount_table:
            if row.kundenname and row.kundenname.strip() == customer_name.strip():
                return flt(row.rabatt_wert_in_prozent)
    except Exception as e:
        frappe.log_error(f"Error getting customer discount for {customer_name}: {str(e)}")
    return 0

def generate_hornetsecurity_report(licenses_before, licenses_after, invoices_created, errors, successful_customers):
    """Generate import report"""
    report_lines = [
        f"Gesamtzahl Lizenzen vorher: {licenses_before}",
        f"Gesamtzahl Lizenzen nachher: {licenses_after}",
        f"Gesamtzahl erz. Rechnungen: {invoices_created}"
    ]
    
    if successful_customers:
        report_lines.append(f"Erfolgreiche Kunden: {', '.join(successful_customers)}")
    
    if errors:
        report_lines.append(f"\nFehler ({len(errors)}):")
        for error in errors:
            report_lines.append(f"- {error}")
    
    return "\n".join(report_lines)