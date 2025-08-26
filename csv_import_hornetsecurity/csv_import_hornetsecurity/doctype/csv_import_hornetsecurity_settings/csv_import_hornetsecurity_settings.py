# Copyright (c) 2025, ahmad mohammad and contributors
# For license information, please see license.txt
# Copyright (c) 2025, ahmad mohammad and contributors
# For license information, please see license.txt
# File: csv_import_hornetsecurity/csv_import_hornetsecurity/csv_import_hornetsecurity_settings/csv_import_hornetsecurity_settings.py
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

class CSVImportHornetsecuritySettings(Document):
    def before_save(self):
        """Validate settings before save"""
        pass

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
        
        # Parse CSV content with comma delimiter (UTF-8 format)
        csv_reader = csv.DictReader(io.StringIO(csv_text), delimiter=';')
        
        # Process data - Group by Customer Reference Number AND Product Code
        customer_product_data = {}
        total_licenses_before = 0
        errors = []
        
        # Process each row
        rows = list(csv_reader)
        
        for i, row in enumerate(rows):
            try:
                customer_ref_nr = row.get('Customer Reference Number', '').strip()
                product_code = row.get('Product Code', '').strip()
                licenses_count_str = row.get('Licenses Count', '0').strip()
                
                if not customer_ref_nr:
                    errors.append(f"Missing Customer Reference Number in line {i+2}")
                    continue
                    
                if not product_code:
                    errors.append(f"Missing Product Code in line {i+2}")
                    continue
                
                # Convert licenses count to float
                licenses_count = flt(licenses_count_str)
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
                    qty = flt(row.get('Licenses Count', 0))
                    price_per_license = flt(row.get('Customer Price Per License', 0))
                    customer_total = flt(row.get('Customer Total', 0))
                    
                    total_qty += qty
                    total_amount += customer_total
                    rate = price_per_license  # Should be same for all rows of same product
                    product_name = row.get('Product Name', '').strip()
                    
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
                    invoice = create_hornetsecurity_sales_invoice_safe(customer_ref_nr, valid_items, settings_doc, errors)
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
        
        # Update history and results
        settings_doc.append('hornetsecurity_importhistorie', {
            'importdatum': datetime.now(),
            'name_der_csv': file_name
        })
        
        settings_doc.append('hornetsecurity_importergebnis', {
            'datum': datetime.now(),
            'name_der_csv': file_name,
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

def get_tax_account_rate(tax_account_name):
    """Fetch tax rate dynamically from Account DocType"""
    try:
        account = frappe.get_doc("Account", tax_account_name)
        # The tax rate might be stored in different fields depending on your setup
        # Common field names: tax_rate, rate, account_rate
        if hasattr(account, 'tax_rate') and account.tax_rate:
            return flt(account.tax_rate)
        elif hasattr(account, 'rate') and account.rate:
            return flt(account.rate)
        else:
            # If no rate found, default to 19%
            return 19.0
    except Exception as e:
        frappe.log_error(f"Error fetching tax rate for account {tax_account_name}: {str(e)}")
        return 19.0  # Default fallback

def create_hornetsecurity_sales_invoice_safe(customer_ref_nr, items_data, settings_doc, errors):
    """Create sales invoice for Hornetsecurity customer - SAFE VERSION"""
    
    try:
        # Get customer (already validated to exist)
        customer = frappe.get_all('Customer', 
            filters={'custom_interne_kundennummer': customer_ref_nr}, 
            fields=['name', 'customer_name']
        )[0]
        
        # Create sales invoice
        invoice = frappe.new_doc('Sales Invoice')
        invoice.customer = customer['name']
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
        
        # Add taxes with dynamic rate from Account DocType
        try:
            tax_account = "1520 - Abziehbare Vorsteuer 19 % - AZ ITD - ÜJ"
            
            # Fetch tax rate dynamically from Account
            tax_rate = get_tax_account_rate(tax_account)
            
            invoice.append('taxes', {
                'charge_type': 'On Net Total',
                'account_head': tax_account,
                'rate': tax_rate,  # Dynamic rate from Account DocType
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