// Copyright (c) 2025, ahmad mohammad and contributors
// For license information, please see license.txt

// frappe.ui.form.on("CSV Import Hornetsecurity Settings", {
// 	refresh(frm) {

// 	},
// });
frappe.ui.form.on("CSV Import Hornetsecurity Settings", {
    csv_import_button: function(frm) {
        let d = new frappe.ui.Dialog({
            title: 'Upload Hornetsecurity CSV File',
            fields: [
                {
                    fieldname: 'csv_file',
                    fieldtype: 'Attach',
                    label: 'CSV File (Hornetsecurity Format)',
                    reqd: 1,
                    description: 'Upload CSV file with Hornetsecurity data. Encoding: UTF-8'
                }
            ],
            primary_action_label: 'Import CSV',
            primary_action(values) {
                if (!values.csv_file) {
                    frappe.msgprint('Please select a CSV file');
                    return;
                }
                
                // Show progress indicator
                frappe.show_progress('Processing CSV...', 30, 100, 'Please wait...');
                
                // Get the file URL and read it
                let file_url = values.csv_file;
                
                // Use fetch to get file content
                fetch(file_url)
                    .then(response => response.arrayBuffer())
                    .then(buffer => {
                        // Convert to base64
                        let binary = '';
                        let bytes = new Uint8Array(buffer);
                        for (let i = 0; i < bytes.byteLength; i++) {
                            binary += String.fromCharCode(bytes[i]);
                        }
                        let base64Content = btoa(binary);
                        
                        frappe.show_progress('Processing CSV...', 60, 100, 'Creating invoices...');
                        
                        // Process the import
                        frappe.call({
                            method: 'csv_import_hornetsecurity.csv_import_hornetsecurity.doctype.csv_import_hornetsecurity_settings.csv_import_hornetsecurity_settings.process_csv_import',
                            args: {
                                doc_name: frm.doc.name,
                                file_content: base64Content,
                                file_name: file_url.split('/').pop()
                            },
                            callback: function(r) {
                                frappe.hide_progress();
                                
                                if (r.message && r.message.status === 'success') {
                                    frappe.msgprint({
                                        title: 'Import Successful',
                                        message: r.message.message,
                                        indicator: 'green'
                                    });
                                    frm.reload_doc();
                                } else {
                                    frappe.msgprint({
                                        title: 'Import Failed',
                                        message: r.message ? r.message.message : 'Unknown error occurred',
                                        indicator: 'red'
                                    });
                                }
                            },
                            error: function(r) {
                                frappe.hide_progress();
                                frappe.msgprint({
                                    title: 'Import Error',
                                    message: 'An error occurred during import. Please check the error logs.',
                                    indicator: 'red'
                                });
                                console.error('Import error:', r);
                            }
                        });
                    })
                    .catch(error => {
                        frappe.hide_progress();
                        frappe.msgprint({
                            title: 'File Read Error',
                            message: 'Failed to read the uploaded file: ' + error.message,
                            indicator: 'red'
                        });
                        console.error('File read error:', error);
                    });
                
                d.hide();
            }
        });
        
        d.show();
    }
});