from ast import Try
import email
from flask import Flask, render_template, request, redirect, url_for, session
import pyodbc
import os
import traceback
from dotenv import load_dotenv
from flask import request
from werkzeug.utils import secure_filename
from flask import flash
from fpdf import FPDF
from flask import Response
import io
from flask import send_file, abort
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
import pandas as pd
from werkzeug.utils import secure_filename
import logging
from io import BytesIO
from flask import current_app as app
load_dotenv()

UPLOAD_FOLDER = 'static/uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Needed for session handling

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# Build the connection string, handling optional port
db_driver = os.getenv('DB_DRIVER')
db_server = os.getenv('DB_SERVER')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_timeout = os.getenv('DB_TIMEOUT')

server_with_port = f"{db_server},{db_port}" if db_port else db_server

conn_str = (
    f"DRIVER={{{db_driver}}};"
    f"SERVER={server_with_port};"
    f"DATABASE={db_name};"
    f"UID={db_user};"
    f"PWD={db_pass};"
    f"Timeout={db_timeout};"
)

def validate_user(email, password):
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EMP_NAME, EMP_CODE, EMAIL_ID, PERMISSIONS
                FROM EDS_EMP
                WHERE EMAIL_ID = ? AND PASSWORD = ?
            """, (email, password))
            user = cursor.fetchone()
            if user:
                return {
                    "EMP_NAME": user[0],
                    "EMP_CODE": user[1],
                    "EMAIL_ID": user[2],
                    "PERMISSIONS": user[3]
                }
    except Exception as e:
        print("DB Error:", e)
    return None

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/')
def index():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    email = request.form.get('email', '').strip()
    password = request.form.get('password', '').strip()

    if not email or not password:
        return render_template('login.html', error="Please enter both email and password")

    user_data = validate_user(email, password)

    if user_data:
        session['user'] = user_data  # Store user info in session

        role = user_data.get('PERMISSIONS', '').lower()

        # Redirect based on role
        if role == 'admin':
            return redirect(url_for('admin_dashboard'))
        elif role == 'user':
            return redirect(url_for('user_dashboard'))
        else:
            # Optional: Handle unknown roles
            return render_template('login.html', error="Unauthorized role access")

    else:
        return render_template('login.html', error="Invalid email or password")

@app.route('/admin_dashboard')
def admin_dashboard():
    if 'user' not in session:
        return redirect(url_for('index'))  # Redirect if user not logged in

    user = session['user']  # Get logged-in user data (if needed for UI)

    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                # Total outlets with completed survey
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_COMPLETE_OUTLET_SURVEY")
                total_outlet_complete = cursor.fetchone()[0]

                # Total outlets assigned (from original survey list)
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_OUTLET_SURVEY")
                total_outlet_assigned = cursor.fetchone()[0]

                # Total new customers added
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_NEW_OUTLET_SURVEY")
                total_customer = cursor.fetchone()[0]

                # Total pending surveys
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_OUTLET_SURVEY WHERE STATUS = 'PENDING'")
                total_pending_survey = cursor.fetchone()[0]

                # Total completed surveys (duplicated with complete outlets?)
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_COMPLETE_OUTLET_SURVEY")
                total_complete_survey = cursor.fetchone()[0]
                # Total new surveys
                cursor.execute("SELECT COUNT(*) FROM dbo.EDS_NEW_OUTLET_SURVEY")
                total_new_survey = cursor.fetchone()[0]

    except Exception as e:
        print(f"❌ Error fetching admin dashboard data: {e}")
        flash("Error loading dashboard data. Please try again later.", "error")
        return redirect(url_for('index'))

    return render_template('admin_dashboard.html',
    total_outlet_assigned=int(total_outlet_assigned or 0),
    total_complete_survey=int(total_complete_survey or 0),
    total_pending_survey=int(total_pending_survey or 0),
    total_new_survey=int(total_new_survey or 0),
    total_customer=int(total_customer or 0),
    total_outlet_complete=int(total_outlet_complete or 0),
    user=user.get('EMAIL_ID')  # or whatever user object you're passing
)

@app.route('/logout')
def logout():
    session.pop('user', None)
    return render_template('login.html', error="You have been logged out")

@app.route('/user_dashboard')
def user_dashboard():
    if 'user' not in session:
        return redirect(url_for('index'))

    user = session['user']
    email_id = user.get('EMAIL_ID')

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) 
                FROM EDS_OUTLET_SURVEY 
                WHERE EMAIL_ID = ? AND STATUS = 'PENDING'
            """, (email_id,))
            outlet_survey_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) 
                FROM EDS_COMPLETE_OUTLET_SURVEY 
                WHERE EMAIL_ID = ? 
            """, (email_id,))
            complete_survey_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) 
                FROM EDS_NEW_OUTLET_SURVEY 
                WHERE EMAIL_ID = ? 
            """, (email_id,))
            new_complete_outlet = cursor.fetchone()[0]

    except Exception as e:
        print("DB Error:", e)
        outlet_survey_count = 0
        complete_survey_count = 0
        outlet_survey_count = int(outlet_survey_count or 0)
        complete_survey_count = int(complete_survey_count or 0)

    return render_template(
        'user_dashboard.html',
        user=user,
        outlet_survey_count=outlet_survey_count,
        complete_survey_count=complete_survey_count,
        new_complete_outlet=new_complete_outlet
    )

@app.route('/outlet_survey')
def outlet_survey():
    if 'user' not in session:
        return redirect(url_for('index'))

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT s.TRANSACTIONS_ID, s.OUTLET_CODE, s.OUTLET_NAME, s.OUTLET_ADDRESS, d.distributor_code
                FROM EDS_OUTLET_SURVEY s
                INNER JOIN EDS_DISTRIBUTOR_EMAILS d ON s.email_id = d.email_id
                WHERE s.STATUS = 'PENDING' AND s.email_id = ?
            """, (session['user']['EMAIL_ID'],))

            data = cursor.fetchall()

    except Exception as e:
        print("DB Error:", e)
        data = []

    return render_template('outlet_survey.html', data=data, user=session['user'])

import re
def get_next_filename(cursor, table, column, prefix, num_digits, extension):
    cursor.execute(f"""
        SELECT MAX({column}) FROM {table}
        WHERE {column} LIKE '{prefix}%'
    """)
    result = cursor.fetchone()[0]

    if result:
        match = re.search(rf"{prefix}(\d+)", result)
        if match:
            number = int(match.group(1))
        else:
            number = 0
    else:
        number = 0

    number += 1
    return f"{prefix}{str(number).zfill(num_digits)}.{extension}"

from flask import render_template, request, redirect, url_for, session

from PIL import Image
import os
   
@app.route('/edit_survey/<transaction_id>', methods=['GET', 'POST'])
def edit_survey(transaction_id):
    # Redirect to login page if user not logged in
    if 'user' not in session:
        return redirect(url_for('index'))
    def get_channels():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT CHANNEL_NAME FROM Channel_Master ORDER BY CHANNEL_NAME")
                channels = [row[0] for row in cursor.fetchall()]
                return channels
        except Exception as e:
            print("Error fetching channels:", e)
            return []
    def get_distributors():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME, DISTRIBUTOR_CODE FROM Distributor_Master ORDER BY DISTRIBUTOR_NAME")
                distributors = cursor.fetchall()  # List of tuples (name, code)
                return distributors
        except Exception as e:
            print("Error fetching distributors:", e)
            return []
            channels = get_channels()
            distributors = get_distributors()
    # Helper function: Fetch all SGA types from the database
    def get_sga_types():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT SGA_TYPE FROM SGA_TYPE")
                # Return list of all SGA_TYPE strings
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("Error fetching SGA types:", e)
            return []  # Return empty list on error

    sga_types = get_sga_types()  # Retrieve SGA types for dropdown/select options

    # List of all fields required in the complete outlet survey record
    complete_fields = [
        'EMAIL_ID', 'TRANSACTIONS_ID', 'STATUS', 'OUTLET_CODE', 'OUTLET_NAME',
        'OUTLET_MOBILE', 'CITY', 'STATE', 'PINCODE', 'OUTLET_ADDRESS',
        'LAT', 'LONG', 'VPO', 'DISTRIBUTOR_CODE', 'DISTRIBUTOR_NAME', 'CHANNEL',
        'VOLUME', 'SGA',
        # Fields for each of 5 SGA units (working conditions, product type, etc.)
        'SGA_WORKING_CONDITIONS_1', 'SGA_PRODUCT_TYPE_1', 'SGA_SERIAL_NO_1', 'SGA_ASSEST_TAG_NO_1', 'BAIL_ID_1', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_1',
        'OUTLET_IMAGE',
        'SGA_WORKING_CONDITIONS_2', 'SGA_WORKING_CONDITIONS_3', 'SGA_WORKING_CONDITIONS_4', 'SGA_WORKING_CONDITIONS_5',
        'SGA_PRODUCT_TYPE_2', 'SGA_PRODUCT_TYPE_3', 'SGA_PRODUCT_TYPE_4', 'SGA_PRODUCT_TYPE_5',
        'SGA_SERIAL_NO_2', 'SGA_SERIAL_NO_3', 'SGA_SERIAL_NO_4', 'SGA_SERIAL_NO_5',
        'SGA_ASSEST_TAG_NO_2', 'SGA_ASSEST_TAG_NO_3', 'SGA_ASSEST_TAG_NO_4', 'SGA_ASSEST_TAG_NO_5',
        'IMAGE_UPLOAD_ASSEST_SERIAL_NO_2', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_3', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_4', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_5',
        'BAIL_ID_2', 'BAIL_ID_3', 'BAIL_ID_4', 'BAIL_ID_5',
        'SGA_COUNT'
    ]

    # POST request: Handle form submission for updating/creating survey record
    if request.method == 'POST':
        form = request.form  # Access submitted form data
        files = request.files  # Access uploaded files

        # Get uploaded files from form (only first SGA image and outlet image)
        image_upload_file_1 = files.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_1')
        outlet_image_file = files.get('OUTLET_IMAGE')

        image_upload_filename_1 = ''  # Filename to store in DB for first SGA image
        outlet_image_filename = ''    # Filename to store in DB for outlet image

        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()

                # If SGA image 1 uploaded, generate filename and save file
                if image_upload_file_1 and image_upload_file_1.filename:
                    ext1 = image_upload_file_1.filename.rsplit('.', 1)[-1].lower()
                    image_upload_filename_1 = get_next_filename(
                        cursor, "EDS_COMPLETE_OUTLET_SURVEY", "IMAGE_UPLOAD_ASSEST_SERIAL_NO_1",
                        prefix="OUTSERAST", num_digits=10, extension=ext1
                    )
                    image_upload_file_1.save(os.path.join(UPLOAD_FOLDER, image_upload_filename_1))

                # If outlet image uploaded, generate filename and save file
                if outlet_image_file and outlet_image_file.filename:
                    ext2 = outlet_image_file.filename.rsplit('.', 1)[-1].lower()
                    outlet_image_filename = get_next_filename(
                        cursor, "EDS_COMPLETE_OUTLET_SURVEY", "OUTLET_IMAGE",
                        prefix="OUTIMG", num_digits=10, extension=ext2
                    )
                    outlet_image_file.save(os.path.join(UPLOAD_FOLDER, outlet_image_filename))

                # Extract SGA form fields for 5 SGA units into lists
                sga_working_conditions = [form.get(f'SGA_WORKING_CONDITIONS_{i}', '') for i in range(1, 6)]
                sga_product_type = [form.get(f'SGA_PRODUCT_TYPE_{i}', '') for i in range(1, 6)]
                sga_serial_no = [form.get(f'SGA_SERIAL_NO_{i}', '') for i in range(1, 6)]
                sga_assest_tag_no = [form.get(f'SGA_ASSEST_TAG_NO_{i}', '') for i in range(1, 6)]
                bail_ids = [form.get(f'BAIL_ID_{i}', '') for i in range(1, 6)]
                # Image upload filenames for all 5 SGA units, first replaced if new file uploaded
                image_upload_assest_serials = [
                    image_upload_filename_1 or form.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_1', '')
                ] + [form.get(f'IMAGE_UPLOAD_ASSEST_SERIAL_NO_{i}', '') for i in range(2, 6)]

                # Construct values list in exact order of complete_fields for DB insert/update
                complete_values = [
                    session['user']['EMAIL_ID'],   # EMAIL_ID
                    transaction_id,                # TRANSACTIONS_ID
                    'COMPLETE',                   # STATUS
                    form.get('OUTLET_CODE'),
                    form.get('OUTLET_NAME'),
                    form.get('OUTLET_MOBILE'),
                    form.get('CITY'),
                    form.get('STATE'),
                    form.get('PINCODE'),
                    form.get('OUTLET_ADDRESS'),
                    form.get('LAT'),
                    form.get('LONG'),
                    form.get('VPO'),
                    form.get('DISTRIBUTOR_CODE'),
                    form.get('DISTRIBUTOR_NAME'),
                    form.get('CHANNEL'),
                    form.get('VOLUME'),
                    form.get('SGA'),
                    # SGA 1 details
                    sga_working_conditions[0],
                    sga_product_type[0],
                    sga_serial_no[0],
                    sga_assest_tag_no[0],
                    bail_ids[0],
                    image_upload_assest_serials[0],
                    # Outlet image filename (new uploaded or existing)
                    outlet_image_filename or form.get('OUTLET_IMAGE', ''),
                    # SGA 2-5 working conditions
                    sga_working_conditions[1],
                    sga_working_conditions[2],
                    sga_working_conditions[3],
                    sga_working_conditions[4],
                    # SGA 2-5 product types
                    sga_product_type[1],
                    sga_product_type[2],
                    sga_product_type[3],
                    sga_product_type[4],
                    # SGA 2-5 serial numbers
                    sga_serial_no[1],
                    sga_serial_no[2],
                    sga_serial_no[3],
                    sga_serial_no[4],
                    # SGA 2-5 asset tag numbers
                    sga_assest_tag_no[1],
                    sga_assest_tag_no[2],
                    sga_assest_tag_no[3],
                    sga_assest_tag_no[4],
                    # SGA 2-5 image upload filenames
                    image_upload_assest_serials[1],
                    image_upload_assest_serials[2],
                    image_upload_assest_serials[3],
                    image_upload_assest_serials[4],
                    # SGA 2-5 bail IDs
                    bail_ids[1],
                    bail_ids[2],
                    bail_ids[3],
                    bail_ids[4],
                    form.get('SGA_COUNT', '0')  # Number of SGA items
                ]

                # SQL MERGE statement to UPSERT data into EDS_COMPLETE_OUTLET_SURVEY table
                upsert_query = f"""
                    MERGE EDS_COMPLETE_OUTLET_SURVEY AS target
                    USING (SELECT ? AS TRANSACTIONS_ID, ? AS EMAIL_ID) AS source
                    ON (target.TRANSACTIONS_ID = source.TRANSACTIONS_ID AND target.EMAIL_ID = source.EMAIL_ID)
                    WHEN MATCHED THEN
                        UPDATE SET
                            STATUS = ?, OUTLET_CODE = ?, OUTLET_NAME = ?, OUTLET_MOBILE = ?,
                            CITY = ?, STATE = ?, PINCODE = ?, OUTLET_ADDRESS = ?,
                            LAT = ?, LONG = ?, VPO = ?,
                            DISTRIBUTOR_CODE = ?, DISTRIBUTOR_NAME = ?, CHANNEL = ?, VOLUME = ?, SGA = ?,
                            SGA_WORKING_CONDITIONS_1 = ?, SGA_PRODUCT_TYPE_1 = ?, SGA_SERIAL_NO_1 = ?, SGA_ASSEST_TAG_NO_1 = ?, BAIL_ID_1 = ?, IMAGE_UPLOAD_ASSEST_SERIAL_NO_1 = ?,
                            OUTLET_IMAGE = ?, SGA_WORKING_CONDITIONS_2 = ?, SGA_WORKING_CONDITIONS_3 = ?, SGA_WORKING_CONDITIONS_4 = ?, SGA_WORKING_CONDITIONS_5 = ?,
                            SGA_PRODUCT_TYPE_2 = ?, SGA_PRODUCT_TYPE_3 = ?, SGA_PRODUCT_TYPE_4 = ?, SGA_PRODUCT_TYPE_5 = ?,
                            SGA_SERIAL_NO_2 = ?, SGA_SERIAL_NO_3 = ?, SGA_SERIAL_NO_4 = ?, SGA_SERIAL_NO_5 = ?,
                            SGA_ASSEST_TAG_NO_2 = ?, SGA_ASSEST_TAG_NO_3 = ?, SGA_ASSEST_TAG_NO_4 = ?, SGA_ASSEST_TAG_NO_5 = ?,
                            IMAGE_UPLOAD_ASSEST_SERIAL_NO_2 = ?, IMAGE_UPLOAD_ASSEST_SERIAL_NO_3 = ?, IMAGE_UPLOAD_ASSEST_SERIAL_NO_4 = ?, IMAGE_UPLOAD_ASSEST_SERIAL_NO_5 = ?,
                            BAIL_ID_2 = ?, BAIL_ID_3 = ?, BAIL_ID_4 = ?, BAIL_ID_5 = ?,
                            SGA_COUNT = ?
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(complete_fields)})
                        VALUES ({', '.join(['?'] * len(complete_fields))});
                """

                # Parameters for MERGE:
                # First two keys for USING clause, then update values, then insert values
                params = [transaction_id, session['user']['EMAIL_ID']] + complete_values[2:] + complete_values

                cursor.execute(upsert_query, params)    

                # Also update original EDS_OUTLET_SURVEY status to 'COMPLETE'
                cursor.execute("""
                    UPDATE EDS_OUTLET_SURVEY
                    SET STATUS = 'COMPLETE'
                    WHERE EMAIL_ID = ? AND TRANSACTIONS_ID = ?
                """, (session['user']['EMAIL_ID'], transaction_id))

                conn.commit()  # Commit all DB changes

            flash('✅ Survey completed and data saved successfully.')
        except Exception as e:
            print("DB Save Error:", e)  # Print DB error for debugging
            flash('❌ Error updating data.')

        return redirect(url_for('outlet_survey'))  # Redirect after POST

    # GET request: Load existing complete survey record to prefill form
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Select all columns from complete survey table for this user & transaction
            cursor.execute(f"""
                SELECT {', '.join(complete_fields)}
                FROM EDS_OUTLET_SURVEY
                WHERE EMAIL_ID = ? AND TRANSACTIONS_ID = ?
            """, (session['user']['EMAIL_ID'], transaction_id))
            record = cursor.fetchone()
    except Exception as e:
        print("DB Fetch Error:", e)
        record = None

    # Convert fetched record tuple to dictionary keyed by field names for easy access in template
    record_dict = dict(zip(complete_fields, record)) if record else {}
    record_dict['CHANNEL'] = record_dict.get('CHANNEL', '')
    record_dict['DISTRIBUTOR_CODE'] = record_dict.get('DISTRIBUTOR_CODE', '')
    record_dict['sga_count'] = record_dict.get('SGA_COUNT', '0')
    print("Record dict loaded for edit:", record_dict)  # Debug print to check data fetched

    # Prepare list of SGA items dictionaries (for 5 SGA units) to pass to template
    sga_items = []
    for i in range(1, 6):
        sga_items.append({
            "WORKING_CONDITIONS": record_dict.get(f'SGA_WORKING_CONDITIONS_{i}', ''),
            "PRODUCT_TYPE": record_dict.get(f'SGA_PRODUCT_TYPE_{i}', ''),
            "SERIAL_NO": record_dict.get(f'SGA_SERIAL_NO_{i}', ''),
            "ASSEST_TAG_NO": record_dict.get(f'SGA_ASSEST_TAG_NO_{i}', ''),
            "BAIL_ID": record_dict.get(f'BAIL_ID_{i}', ''),
            "IMAGE_UPLOAD_ASSEST_SERIAL_NO": record_dict.get(f'IMAGE_UPLOAD_ASSEST_SERIAL_NO_{i}', '')
        })

    # Render the edit survey form template with pre-filled data
    return render_template(
        'edit_survey.html',
        record=record_dict,   # All record fields as dictionary for easy access
        sga_items=sga_items,  # List of SGA sub-items for iteration in template
        sga_types=sga_types,   # List of SGA types for dropdown selections
        channels=get_channels(),
        distributors=get_distributors()
    )

# ✅ This MUST be on a NEW line
@app.route('/complete_survey' , methods=['GET', 'POST'])
def complete_survey():
    if 'user' not in session:
        return redirect(url_for('index'))

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    EMAIL_ID,
                    TRANSACTIONS_ID,
                    STATUS,
                    OUTLET_CODE,
                    OUTLET_NAME,
                    OUTLET_MOBILE,
                    OUTLET_ADDRESS,
                    DISTRIBUTOR_CODE,
                    DISTRIBUTOR_NAME
                FROM EDS_COMPLETE_OUTLET_SURVEY where STATUS = 'COMPLETE' and EMAIL_ID = ?
            """, (session['user']['EMAIL_ID'],))
            columns = [column[0] for column in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print("DB Error:", e)
        data = []

    return render_template('complete_survey.html', data=data, user=session['user'])


@app.route('/download_data')
def download_data():
    return "Download Complete Data (To be implemented)"
from flask import jsonify

@app.route('/check_duplicate_mobile')
def check_duplicate_mobile():
    mobile = request.args.get('mobile', '').strip()

    if not mobile:
        return jsonify({"exists": False, "source": None})

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check in EDS_NEW_OUTLET_SURVEY
            cursor.execute("SELECT 1 FROM EDS_NEW_OUTLET_SURVEY WHERE OUTLET_MOBILE = ?", (mobile,))
            if cursor.fetchone():
                return jsonify({"exists": True, "source": "EDS_NEW_OUTLET_SURVEY"})

            # If not found, check in EDS_COMPLETE_OUTLET_SURVEY
            cursor.execute("SELECT 1 FROM EDS_COMPLETE_OUTLET_SURVEY WHERE OUTLET_MOBILE = ?", (mobile,))
            if cursor.fetchone():
                return jsonify({"exists": True, "source": "EDS_COMPLETE_OUTLET_SURVEY"})

            # Not found in either table
            return jsonify({"exists": False, "source": None})

    except Exception as e:
        print("Error in /check_duplicate_mobile:", e)
        return jsonify({"exists": False, "error": str(e)}), 500

@app.route('/new_outlet_survey', methods=['GET', 'POST'])
def new_outlet_survey():
    if 'user' not in session:
        return redirect(url_for('index'))

    def get_next_transaction_id():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(TRANSACTIONS_ID) FROM EDS_NEW_OUTLET_SURVEY WHERE TRANSACTIONS_ID LIKE 'AGN%'")
                result = cursor.fetchone()[0]
                if result:
                    numeric_part = int(result.replace("AGN", ""))
                    new_numeric_part = numeric_part + 1
                else:
                    new_numeric_part = 1
                return f"AGN{new_numeric_part:07d}"
        except Exception as e:
            print("Error fetching next transaction ID:", e)
            return "AGN0000001"  # fallback

    def get_next_image_number(prefix, cursor, column_name):
        cursor.execute(f"SELECT MAX({column_name}) FROM EDS_NEW_OUTLET_SURVEY WHERE {column_name} LIKE '{prefix}-%'")
        result = cursor.fetchone()[0]
        if result:
            match = re.search(rf"{prefix}-(\d+)", result)
            if match:
                current_num = int(match.group(1))
                return current_num + 1
        return 1

    def get_distributors():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME, DISTRIBUTOR_CODE FROM Distributor_Master ORDER BY DISTRIBUTOR_NAME")
                return cursor.fetchall()
        except Exception as e:
            print("Error fetching distributors:", e)
            return []

    def get_channels():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT CHANNEL_NAME FROM Channel_Master ORDER BY CHANNEL_NAME")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("Error fetching channels:", e)
            return []

    def get_sga_types():
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT SGA_TYPE FROM SGA_TYPE")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("Error fetching SGA types:", e)
            return []

    sga_types = get_sga_types()
    distributors = get_distributors()
    channels = get_channels()
    transaction_id = get_next_transaction_id()

    if request.method == 'POST':
        try:
            email_id = session['user']['EMAIL_ID']
            outlet_name = request.form.get('OUTLET_NAME', '').strip()
            outlet_mobile = request.form.get('OUTLET_MOBILE', '').strip()
            city = request.form.get('CITY', '').strip()
            state = request.form.get('STATE', '').strip()
            pincode = request.form.get('PINCODE', '').strip()
            address = request.form.get('OUTLET_ADDRESS', '').strip()
            lat = request.form.get('LAT', '').strip()
            long = request.form.get('LONG', '').strip()
            vpo = request.form.get('VPO', '').strip()
            dist_code = request.form.get('DISTRIBUTOR_CODE', '').strip()
            dist_name = request.form.get('DISTRIBUTOR_NAME', '').strip()
            channel = request.form.get('CHANNEL', '').strip()
            volume = request.form.get('VOLUME', '').strip()
            sga = request.form.get('SGA', '').strip().lower()
            sga_count = int(request.form.get('SGA_COUNT', 0))

            outlet_image = request.files.get('OUTLET_IMAGE')

            sga_working_conditions = []
            sga_product_types = []
            sga_serial_nos = []
            sga_asset_tag_nos = []
            image_upload_asset_serial_nos = []
            bail_ids = []

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            for i in range(1, 6):
                if sga == 'yes':
                    sga_working_conditions.append(request.form.get(f'SGA_WORKING_CONDITIONS_{i}', '').strip() or None)
                    sga_product_types.append(request.form.get(f'SGA_PRODUCT_TYPE_{i}', '').strip() or None)
                    sga_serial_nos.append(request.form.get(f'SGA_SERIAL_NO_{i}', '').strip() or None)
                    sga_asset_tag_nos.append(request.form.get(f'SGA_ASSEST_TAG_NO_{i}', '').strip() or None)
                    bail_ids.append(request.form.get(f'BAIL_ID_{i}', '').strip() or None)

                    file = request.files.get(f'IMAGE_UPLOAD_ASSEST_SERIAL_NO_{i}')
                    filename = None
                    if file and file.filename != '':
                        next_num = get_next_image_number('UPASN', cursor, f'IMAGE_UPLOAD_ASSEST_SERIAL_NO_{i}')
                        ext = os.path.splitext(file.filename)[1].lower()
                        filename = f"UPASN-{next_num:07d}{ext}"
                        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                    image_upload_asset_serial_nos.append(filename)
                else:
                    sga_working_conditions.append(None)
                    sga_product_types.append(None)
                    sga_serial_nos.append(None)
                    sga_asset_tag_nos.append(None)
                    image_upload_asset_serial_nos.append(None)
                    bail_ids.append(None)

            outlet_image_filename = None
            if outlet_image and outlet_image.filename != '':
                next_num = get_next_image_number('UPOI', cursor, 'OUTLET_IMAGE')
                ext = os.path.splitext(outlet_image.filename)[1].lower()
                outlet_image_filename = f"UPOI-{next_num:07d}{ext}"
                outlet_image.save(os.path.join(app.config['UPLOAD_FOLDER'], outlet_image_filename))

            query = """
                INSERT INTO EDS_NEW_OUTLET_SURVEY (
                    TRANSACTIONS_ID, OUTLET_NAME, OUTLET_MOBILE, CITY, STATE, PINCODE, OUTLET_ADDRESS, LAT, LONG,
                    VPO, DISTRIBUTOR_CODE, DISTRIBUTOR_NAME, CHANNEL, VOLUME, SGA, SGA_COUNT,
                    SGA_WORKING_CONDITIONS_1, SGA_PRODUCT_TYPE_1, SGA_SERIAL_NO_1, SGA_ASSEST_TAG_NO_1, IMAGE_UPLOAD_ASSEST_SERIAL_NO_1, BAIL_ID_1,
                    SGA_WORKING_CONDITIONS_2, SGA_PRODUCT_TYPE_2, SGA_SERIAL_NO_2, SGA_ASSEST_TAG_NO_2, IMAGE_UPLOAD_ASSEST_SERIAL_NO_2, BAIL_ID_2,
                    SGA_WORKING_CONDITIONS_3, SGA_PRODUCT_TYPE_3, SGA_SERIAL_NO_3, SGA_ASSEST_TAG_NO_3, IMAGE_UPLOAD_ASSEST_SERIAL_NO_3, BAIL_ID_3,
                    SGA_WORKING_CONDITIONS_4, SGA_PRODUCT_TYPE_4, SGA_SERIAL_NO_4, SGA_ASSEST_TAG_NO_4, IMAGE_UPLOAD_ASSEST_SERIAL_NO_4, BAIL_ID_4,
                    SGA_WORKING_CONDITIONS_5, SGA_PRODUCT_TYPE_5, SGA_SERIAL_NO_5, SGA_ASSEST_TAG_NO_5, IMAGE_UPLOAD_ASSEST_SERIAL_NO_5, BAIL_ID_5,
                    OUTLET_IMAGE, EMAIL_ID
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?)
            """

            params = [
                transaction_id,
                outlet_name,
                outlet_mobile,
                city,
                state,
                pincode,
                address,
                lat,
                long,
                vpo,
                dist_code,
                dist_name,
                channel,
                volume,
                sga,
                sga_count,
                # SGA 1–5 data
                sga_working_conditions[0], sga_product_types[0], sga_serial_nos[0], sga_asset_tag_nos[0], image_upload_asset_serial_nos[0], bail_ids[0],
                sga_working_conditions[1], sga_product_types[1], sga_serial_nos[1], sga_asset_tag_nos[1], image_upload_asset_serial_nos[1], bail_ids[1],
                sga_working_conditions[2], sga_product_types[2], sga_serial_nos[2], sga_asset_tag_nos[2], image_upload_asset_serial_nos[2], bail_ids[2],
                sga_working_conditions[3], sga_product_types[3], sga_serial_nos[3], sga_asset_tag_nos[3], image_upload_asset_serial_nos[3], bail_ids[3],
                sga_working_conditions[4], sga_product_types[4], sga_serial_nos[4], sga_asset_tag_nos[4], image_upload_asset_serial_nos[4], bail_ids[4],
                outlet_image_filename,
                email_id,
            ]

            cursor.execute(query, params)
            conn.commit()
            cursor.close()
            conn.close()

            flash("New outlet survey saved successfully.", "success")
            return redirect(url_for('new_outlet_survey'))

        except Exception as e:
            flash(f"Failed to save outlet survey: {e}", "danger")
            print("Error in /new_outlet_survey POST:", e)

    return render_template('new_outlet_survey.html',
                           transaction_id=transaction_id,
                           user=session.get('user', {}),
                           distributors=distributors,
                           channels=channels,
                           sga_types=sga_types)

from flask import Response
from fpdf import FPDF

def view_data(transaction_id):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # Query the database for the record with the given transaction_id
    cursor.execute("SELECT * FROM dbo.EDS_COMPLETE_OUTLET_SURVEY WHERE TRANSACTIONS_ID = ?", transaction_id)
    row = cursor.fetchone()
    if not row:
        cursor.execute("SELECT * FROM dbo.EDS_NEW_OUTLET_SURVEY WHERE TRANSACTIONS_ID = ?", transaction_id)
        row = cursor.fetchone()
        if not row:
            abort(404)
    
    # Convert result to dictionary for easier access in template
    data = dict(zip([column[0] for column in cursor.description], row))
    
    return render_template('view_data.html', data=data)


@app.route('/view_data/<transaction_id>')
def view_data(transaction_id):
    data = get_transaction_data(transaction_id)  # <-- correct function name
    return render_template('view_data.html', data=data)

@app.route('/generate_pdf/<transaction_id>')
def generate_pdf(transaction_id):
    # Fetch data from DB or wherever you store it
    data = get_transaction_data(transaction_id)  # <-- Replace with your DB call
    
    if not data:
        abort(404, description="Transaction not found")

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []

    styles = getSampleStyleSheet()
    title_style = styles['Title']
    normal_style = styles['Normal']
    heading_style = ParagraphStyle(name='Heading', fontSize=14, leading=16, spaceAfter=10, textColor=colors.darkgreen)

    # Title
    elements.append(Paragraph("Outlet Survey - Complete Application", title_style))
    elements.append(Spacer(1, 12))

    # Transaction info
    elements.append(Paragraph(f"Transaction ID: {data['TRANSACTIONS_ID']}", heading_style))
    elements.append(Paragraph(f"Status: {data['STATUS']}", normal_style))
    elements.append(Spacer(1, 12))

    # Outlet details as table data
    table_data = [
        ['Email ID', data['EMAIL_ID']],
        ['Outlet Code', data['OUTLET_CODE']],
        ['Outlet Name', data['OUTLET_NAME']],
        ['Phone No', data['OUTLET_MOBILE']],
        ['Address', data['OUTLET_ADDRESS']],
        ['Distributor Code', data['DISTRIBUTOR_CODE']],
        ['Distributor Name', data['DISTRIBUTOR_NAME']],
        ['Channel', data['CHANNEL']],
        ['Volume', data['VOLUME']],
        ['VPO', data['VPO']],
        ['SGA Data', data.get('SGA', 'N/A')],
        ['SGA WORKING CONDITIONS', data['SGA_WORKING_CONDITIONS_1']],
        ['SGA PRODUCT TYPE', data['SGA_PRODUCT_TYPE_1']],
        ['SGA PRODUCT TYPE', data['SGA_SERIAL_NO_1']],
        ['Bail ID', data.get('BAIL_ID_1', 'Not Available')],
        ['Asset Image', data['IMAGE_UPLOAD_ASSEST_SERIAL_NO_1']],
        ['SGA WORKING CONDITIONS', data['SGA_WORKING_CONDITIONS_2']],
        ['SGA PRODUCT TYPE', data['SGA_PRODUCT_TYPE_2']],
        ['SGA PRODUCT TYPE', data['SGA_SERIAL_NO_3']],
        ['Bail ID', data.get('BAIL_ID_3', 'Not Available')],
        ['Asset Image', data['IMAGE_UPLOAD_ASSEST_SERIAL_NO_3']],
        ['Outlet Image', data['OUTLET_IMAGE']],
        ['SGA WORKING CONDITIONS', data['SGA_WORKING_CONDITIONS_4']],
        ['SGA PRODUCT TYPE', data['SGA_PRODUCT_TYPE_4']],
        ['SGA PRODUCT TYPE', data['SGA_SERIAL_NO_4']],
        ['Bail ID', data.get('BAIL_ID_4', 'Not Available')],
        ['Asset Image', data['IMAGE_UPLOAD_ASSEST_SERIAL_NO_4']],
        ['SGA WORKING CONDITIONS', data['SGA_WORKING_CONDITIONS_5']],
        ['SGA PRODUCT TYPE', data['SGA_PRODUCT_TYPE_5']],
        ['SGA PRODUCT TYPE', data['SGA_SERIAL_NO_5']],
        ['Bail ID', data.get('BAIL_ID_5', 'Not Available')],
        ['Asset Image', data['IMAGE_UPLOAD_ASSEST_SERIAL_NO_5']],
    ]
    table = Table(table_data, colWidths=[150, 350])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Thank you for completing the outlet survey.", normal_style))

    doc.build(elements)
    buffer.seek(0)

    return send_file(buffer, as_attachment=True, download_name=f'OutletSurvey_{transaction_id}.pdf', mimetype='application/pdf')

def get_transaction_data(transaction_id):
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM EDS_COMPLETE_OUTLET_SURVEY WHERE EMAIL_ID = ? AND TRANSACTIONS_ID = ?", (session['user']['EMAIL_ID'], transaction_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
    except Exception as e:
        print("Database error:", e)
        return None
from flask import request, redirect, url_for, flash

@app.route('/add_employee', methods=['GET', 'POST'])
def add_employee():
    if 'user' not in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        emp_name = request.form.get('emp_name')
        emp_code = request.form.get('emp_code')
        mobile_no = request.form.get('mobile_no')
        email_id = request.form.get('email_id')
        password = request.form.get('password')
        city = request.form.get('city')
        permissions = request.form.get('permissions')

        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()

                # ✅ Check if EMAIL_ID already exists
                cursor.execute("SELECT 1 FROM EDS_EMP WHERE EMAIL_ID = ?", (email_id,))
                existing = cursor.fetchone()

                if existing:
                    flash("❌ An employee with this email already exists.", "error")
                    return redirect(url_for('add_employee'))

                # ✅ Insert new employee
                cursor.execute("""
                    INSERT INTO EDS_EMP (EMP_NAME, EMP_CODE, MOBILE_NO, EMAIL_ID, PASSWORD, CITY, PERMISSIONS)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (emp_name, emp_code, mobile_no, email_id, password, city, permissions))
                
                conn.commit()
                flash("✅ Employee added successfully.", "success")
                return redirect(url_for('add_employee'))

        except Exception as e:
            print("DB Error:", e)
            flash("❌ Failed to add employee.", "error")
            return redirect(url_for('add_employee'))

    user = {'EMP_NAME': session['user']['EMP_NAME']}
    return render_template('add_employee.html', user=user)

@app.route('/employee_list')
def employee_list():
    if 'user' not in session:
        return redirect(url_for('index'))

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT EMP_NAME, EMP_CODE, MOBILE_NO, EMAIL_ID, CITY, PERMISSIONS FROM EDS_EMP WHERE EMP_NAME IS NOT NULL")
            employees = cursor.fetchall()
    except Exception as e:
        print("DB Error:", e)
        flash("❌ Could not fetch employee records.", "error")
        employees = []

    user = {'EMP_NAME': session['user']['EMP_NAME']}
    return render_template('employee_list.html', user=user, employees=employees)


@app.route('/add_distributor', methods=['GET', 'POST'])
def add_distributor():
    if 'user' not in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        Distributor_Name = request.form.get('Distributor_Name')
        Distributor_Code = request.form.get('Distributor_Code')
        DIST_MOBILE = request.form.get('DIST_MOBILE')
        DIST_EMAIL = request.form.get('DIST_EMAIL')
        ADDRESS = request.form.get('ADDRESS')
        CITY = request.form.get('CITY')
        STATE = request.form.get('STATE')

        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()

                # Check if distributor already exists (by email or mobile)
                cursor.execute("""
                    SELECT 1 FROM Distributor_Master 
                    WHERE DIST_EMAIL = ? OR DIST_MOBILE = ?
                """, (DIST_EMAIL, DIST_MOBILE))
                existing = cursor.fetchone()

                if existing:
                    flash("❌ Distributor with same email or mobile already exists.", "error")
                    return redirect(url_for('add_distributor'))

                # Insert distributor
                cursor.execute("""
                    INSERT INTO Distributor_Master 
                    (Distributor_Name, Distributor_Code, DIST_MOBILE, DIST_EMAIL, ADDRESS, CITY, STATE)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (Distributor_Name, Distributor_Code, DIST_MOBILE, DIST_EMAIL, ADDRESS, CITY, STATE))

                conn.commit()
                flash("✅ Distributor added successfully.", "success")
                return redirect(url_for('add_distributor'))

        except Exception as e:
            print("DB Error:", e)
            flash("❌ Failed to add distributor.", "error")
            return redirect(url_for('add_distributor'))

    user = {'EMP_NAME': session['user']['EMP_NAME']}
    return render_template('add_distributor.html', user=user)


@app.route('/distributor_list')
def distributor_list():
    if 'user' not in session:
        return redirect(url_for('index'))

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM Distributor_Master
                WHERE Distributor_Name IS NOT NULL
            """)
            distributors = cursor.fetchall()
    except Exception as e:
        print("DB Error:", e)
        flash("❌ Could not fetch distributor records.", "error")
        distributors = []

    user = {'EMP_NAME': session['user']['EMP_NAME']}
    return render_template('distributor_list.html', user=user, distributors=distributors)

from flask import Flask, render_template, request, redirect, url_for, session, flash
import pandas as pd
import pyodbc
import datetime

@app.route('/add_outlet', methods=['GET', 'POST'])
def add_outlet():
    # Check if user is logged in
    if 'user' not in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        file = request.files.get('file')

        # Validate file presence and extension
        if not file or not file.filename.endswith('.xlsx'):
            flash("❌ Invalid file format. Please upload a .xlsx file.", "error")
            return redirect(url_for('add_outlet'))

        try:
            # Read Excel file
            df = pd.read_excel(file)
            df.columns = df.columns.str.strip()

            # Expected columns based on your SQL table structure
            expected_columns = [
                'TRANSACTIONS_ID', 'STATUS', 'OUTLET_CODE', 'OUTLET_NAME', 'OUTLET_MOBILE',
                'CITY', 'STATE', 'PINCODE', 'OUTLET_ADDRESS', 'LAT', 'LONG',
                'VPO',
                'DISTRIBUTOR_CODE', 'DISTRIBUTOR_NAME', 'CHANNEL', 'VOLUME', 'SGA',
                'SGA_WORKING_CONDITIONS_1', 'SGA_PRODUCT_TYPE_1', 'SGA_SERIAL_NO_1', 'SGA_ASSEST_TAG_NO_1',
                'BAIL_ID_1', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_1', 'OUTLET_IMAGE',
                'SGA_WORKING_CONDITIONS_2', 'SGA_WORKING_CONDITIONS_3', 'SGA_WORKING_CONDITIONS_4', 'SGA_WORKING_CONDITIONS_5',
                'SGA_PRODUCT_TYPE_2', 'SGA_PRODUCT_TYPE_3', 'SGA_PRODUCT_TYPE_4', 'SGA_PRODUCT_TYPE_5',
                'SGA_SERIAL_NO_2', 'SGA_SERIAL_NO_3', 'SGA_SERIAL_NO_4', 'SGA_SERIAL_NO_5',
                'SGA_ASSEST_TAG_NO_2', 'SGA_ASSEST_TAG_NO_3', 'SGA_ASSEST_TAG_NO_4', 'SGA_ASSEST_TAG_NO_5',
                'EMAIL_ID', 'CREATED_AT',
                'IMAGE_UPLOAD_ASSEST_SERIAL_NO_2', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_3',
                'IMAGE_UPLOAD_ASSEST_SERIAL_NO_4', 'IMAGE_UPLOAD_ASSEST_SERIAL_NO_5',
                'BAIL_ID_2', 'BAIL_ID_3', 'BAIL_ID_4', 'BAIL_ID_5'
            ]

            # Validate required columns
            if not set(expected_columns).issubset(set(df.columns)):
                missing = set(expected_columns) - set(df.columns)
                flash(f"❌ Excel file is missing required columns: {', '.join(missing)}", "error")
                return redirect(url_for('add_outlet'))

            # Clean values function
            def clean_value(val):
                if pd.isna(val):
                    return None
                val_str = str(val).strip()
                return val_str if val_str else None

            # Connect and insert data
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                inserted_count = 0
                skipped_count = 0

                for index, row in df.iterrows():
                    try:
                        outlet_code = clean_value(row['OUTLET_CODE'])
                        print(f"Processing row {index} (OUTLET_CODE={outlet_code})")

                        # Skip if OUTLET_CODE already exists
                        cursor.execute("SELECT 1 FROM EDS_OUTLET_SURVEY WHERE OUTLET_CODE = ?", outlet_code)
                        if cursor.fetchone():
                            print(f"Skipping duplicate OUTLET_CODE: {outlet_code}")
                            skipped_count += 1
                            continue

                        # Prepare row values (handle CREATED_AT)
                        values = []
                        for col in expected_columns:
                            if col == 'CREATED_AT':
                                val = row.get(col)
                                if pd.isna(val) or not str(val).strip():
                                    values.append(datetime.datetime.now())
                                else:
                                    values.append(clean_value(val))
                            else:
                                values.append(clean_value(row.get(col)))

                        # Build and execute SQL insert dynamically
                        cursor.execute(f"""
                            INSERT INTO EDS_OUTLET_SURVEY (
                                {', '.join(expected_columns)}
                            ) VALUES ({', '.join(['?'] * len(expected_columns))})
                        """, tuple(values))

                        inserted_count += 1

                    except Exception as row_error:
                        print(f"❌ Error in row {index + 2} (OUTLET_CODE={outlet_code}): {repr(row_error)}")
                        flash(f"❌ Error inserting row {index + 2} (OUTLET_CODE={outlet_code}): {str(row_error)}", "error")
                        return redirect(url_for('add_outlet'))

                conn.commit()

                # Show flash messages
                if inserted_count:
                    flash(f"✅ Successfully imported {inserted_count} outlet(s).", "success")
                if skipped_count:
                    flash(f"ℹ️ Skipped {skipped_count} duplicate outlet(s).", "info")

        except Exception as e:
            print("Import Error:", repr(e))
            flash(f"❌ Error importing data. Check Excel file and try again. Details: {str(e)}", "error")

        return redirect(url_for('add_outlet'))

    # GET request: render upload page
    user = {'EMP_NAME': session['user']['EMP_NAME']}
    return render_template('add_outlet.html', user=user)


@app.route('/outlet_list')
def outlet_list():
    if 'user' not in session:
        return redirect(url_for('index'))

    distributor_filter = request.args.get('distributor')
    area_filter = request.args.get('area')
    limit = int(request.args.get('limit', 100))
    page = int(request.args.get('page', 1))

    offset = (page - 1) * limit
    outlets = []
    total_records = 0

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()

        # Base query
        query = "SELECT * FROM EDS_OUTLET_SURVEY WHERE 1=1"
        count_query = "SELECT COUNT(*) FROM EDS_OUTLET_SURVEY WHERE 1=1"
        params = []
        count_params = []

        if distributor_filter:
            query += " AND DISTRIBUTOR_NAME = ?"
            count_query += " AND DISTRIBUTOR_NAME = ?"
            params.append(distributor_filter)
            count_params.append(distributor_filter)

        if area_filter:
            query += " AND CITY = ?"
            count_query += " AND CITY = ?"
            params.append(area_filter)
            count_params.append(area_filter)

        # Count total filtered records
        total_records = cursor.execute(count_query, count_params).fetchone()[0]
        total_pages = (total_records + limit - 1) // limit

        # Add pagination to query
        paginated_query = query + " ORDER BY OUTLET_CODE OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, limit])

        outlets = cursor.execute(paginated_query, params).fetchall()

        # Dropdowns
        distributor_names = [row[0] for row in cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME FROM EDS_OUTLET_SURVEY").fetchall()]
        areas = [row[0] for row in cursor.execute("SELECT DISTINCT CITY FROM EDS_OUTLET_SURVEY").fetchall()]

    user = {'EMP_NAME': session['user']['EMP_NAME']}

    return render_template(
        'outlet_list.html',
        user=user,
        outlets=outlets,
        distributor_names=distributor_names,
        areas=areas,
        page=page,
        limit=limit,
        total_pages=total_pages
    )


from flask import request, render_template
import pyodbc

@app.route('/complete_survey_report')
def complete_survey_report():
    email = request.args.get('email_id', '')
    distributor = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    # Build query and params based on filters
    query = "SELECT * FROM dbo.EDS_COMPLETE_OUTLET_SURVEY WHERE 1=1"
    params = []

    if email:
        query += " AND EMAIL_ID = ?"
        params.append(email)
    if distributor:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor)
    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")
    # Fetch dropdown lists
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT EMAIL_ID FROM dbo.EDS_COMPLETE_OUTLET_SURVEY")
        email_list = [row[0] for row in cursor.fetchall() if row[0]]

        cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME FROM dbo.EDS_COMPLETE_OUTLET_SURVEY")
        distributor_list = [row[0] for row in cursor.fetchall() if row[0]]

    # Only fetch data if any filter applied
    filters_applied = any([email, distributor, status])
    rows = []
    columns = []
    if filters_applied:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]

    return render_template(
        'complete_survey_report.html',
        data=rows,
        columns=columns,
        email_list=email_list,
        distributor_list=distributor_list,
        filters={
            'email': email,
            'distributor': distributor,
            'status': status
        },
        filters_applied=filters_applied
    )
@app.route('/export_complete_survey_excel')
def export_complete_survey_excel():
    # Read filters from query params
    email = request.args.get('email_id', '')
    distributor = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    # Build your query with filters (make sure to prevent SQL injection)
    query = "SELECT * FROM dbo.EDS_COMPLETE_OUTLET_SURVEY WHERE 1=1"
    params = []

    if email:
        query += " AND EMAIL_ID = ?"
        params.append(email)
    if distributor:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor)
    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")

    # Connect to DB and fetch data
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn, params=params)

    # Create an in-memory Excel file
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SurveyData')

    output.seek(0)

    # Send the file to client
    return send_file(output,
                     download_name="Complete_Survey_Report.xlsx",
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
@app.route('/total_outlet_complete')
def total_outlet_complete():
    if 'user' not in session:
        return redirect(url_for('index'))

    email_id = request.args.get('email_id', '')
    distributor_name = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    filters = {
        'email': email_id,
        'distributor': distributor_name,
        'status': status
    }

    # Use plain column names — no aliases like n.EMAIL_ID
    query = "SELECT * FROM dbo.vw_OutletSurveyCombined WHERE STATUS = 'Complete'"
    params = []

    if email_id:
        query += " AND EMAIL_ID = ?"
        params.append(email_id)

    if distributor_name:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor_name)

    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()

        # Main data query
        cursor.execute(query, params)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # Dropdown values
        cursor.execute("SELECT DISTINCT EMAIL_ID FROM dbo.vw_OutletSurveyCombined WHERE EMAIL_ID IS NOT NULL")
        email_list = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME FROM dbo.vw_OutletSurveyCombined WHERE DISTRIBUTOR_NAME IS NOT NULL")
        distributor_list = [row[0] for row in cursor.fetchall()]

    return render_template(
        'total_outlet_complete.html',
        data=data,
        columns=columns,
        email_list=email_list,
        distributor_list=distributor_list,
        filters=filters,
        request=request
    )

@app.route('/exportfiltered_outlets_to_excel')
def exportfiltered_outlets_to_excel():
    email_id = request.args.get('email_id', '')
    distributor_name = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    query = "SELECT * FROM dbo.vw_OutletSurveyCombined WHERE STATUS = 'Complete'"
    params = []

    if email_id:
        query += " AND EMAIL_ID = ?"
        params.append(email_id)

    if distributor_name:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor_name)

    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")

    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn, params=params)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Filtered Outlets')

    output.seek(0)
    return send_file(output,
                     download_name="filtered_outlets.xlsx",
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
@app.route('/exportall_outlets_to_excel')
def exportall_outlets_to_excel():
    query = "SELECT * FROM dbo.vw_OutletSurveyCombined WHERE STATUS = 'Complete'"

    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Outlets')

    output.seek(0)
    return send_file(output,
                     download_name="all_outlets_complete.xlsx",
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


from flask import send_file, abort
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
import io
import os

@app.route('/new_generate_pdf/<transaction_id>')
def new_generate_pdf(transaction_id):
    data = get_transaction_data1(transaction_id)
    if not data:
        abort(404, description="Transaction not found")
    
    sga_details = get_sga_details(transaction_id)

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []

    styles = getSampleStyleSheet()
    title_style = styles['Title']
    normal_style = styles['Normal']
    heading_style = ParagraphStyle(name='Heading', fontSize=14, leading=16, spaceAfter=10, textColor=colors.darkgreen)

    # Title
    elements.append(Paragraph("Outlet Survey - Complete Application", title_style))
    elements.append(Spacer(1, 12))

    # Transaction info
    elements.append(Paragraph(f"Transaction ID: {data['TRANSACTIONS_ID']}", heading_style))
    elements.append(Paragraph(f"Status: {data['STATUS']}", normal_style))
    elements.append(Spacer(1, 12))

    # Outlet details table
    table_data = [
        ['Email ID', data.get('EMAIL_ID', 'N/A')],
        ['Outlet Name', data.get('OUTLET_NAME', 'N/A')],
        ['Phone No', data.get('OUTLET_MOBILE', 'N/A')],
        ['Address', data.get('OUTLET_ADDRESS', 'N/A')],
        ['Distributor Code', data.get('DISTRIBUTOR_CODE', 'N/A')],
        ['Distributor Name', data.get('DISTRIBUTOR_NAME', 'N/A')],
        ['Channel', data.get('CHANNEL', 'N/A')],
        ['Volume', data.get('VOLUME', 'N/A')],
        ['VPO', data.get('VPO', 'N/A')],
        ['SGA Data', data.get('SGA', 'N/A')],
        ['SGA WORKING CONDITIONS',data.get('SGA_WORKING_CONDITIONS_1', 'Not Available')],
        ['SGA WORKING CONDITIONS',data.get('SGA_WORKING_CONDITIONS_2', 'Not Available')],
        ['SGA WORKING CONDITIONS',data.get('SGA_WORKING_CONDITIONS_3', 'Not Available')],
        ['SGA WORKING CONDITIONS',data.get('SGA_WORKING_CONDITIONS_4', 'Not Available')],
        ['SGA WORKING CONDITIONS',data.get('SGA_WORKING_CONDITIONS_5', 'Not Available')],
        ['SGA PRODUCT TYPE',data.get('SGA_PRODUCT_TYPE_1', 'Not Available')],
        ['SGA PRODUCT TYPE',data.get('SGA_PRODUCT_TYPE_2', 'Not Available')],
        ['SGA PRODUCT TYPE',data.get('SGA_PRODUCT_TYPE_3', 'Not Available')],
        ['SGA PRODUCT TYPE',data.get('SGA_PRODUCT_TYPE_4', 'Not Available')],
        ['SGA PRODUCT TYPE',data.get('SGA_PRODUCT_TYPE_5', 'Not Available')],
        ['SGA SERIAL NO',data.get('SGA_SERIAL_NO_1', 'Not Available')],
        ['SGA SERIAL NO',data.get('SGA_SERIAL_NO_2', 'Not Available')],
        ['SGA SERIAL NO',data.get('SGA_SERIAL_NO_3', 'Not Available')],
        ['SGA SERIAL NO',data.get('SGA_SERIAL_NO_4', 'Not Available')],
        ['SGA SERIAL NO',data.get('SGA_SERIAL_NO_5', 'Not Available')],
        ['BAIL ID',data.get('BAIL_ID_1', 'Not Available')],
        ['BAIL ID',data.get('BAIL_ID_2', 'Not Available')],
        ['BAIL ID',data.get('BAIL_ID_3', 'Not Available')],
        ['BAIL ID',data.get('BAIL_ID_4', 'Not Available')],
        ['BAIL ID',data.get('BAIL_ID_5', 'Not Available')],
        ['IMAGE UPLOAD ASSEST SERIAL NO',data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_1', 'Not Available')],
        ['IMAGE UPLOAD ASSEST SERIAL NO',data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_2', 'Not Available')],
        ['IMAGE UPLOAD ASSEST SERIAL NO',data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_3', 'Not Available')],
        ['IMAGE UPLOAD ASSEST SERIAL NO',data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_4', 'Not Available')],
        ['IMAGE UPLOAD ASSEST SERIAL NO',data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO_5', 'Not Available')],
        ]

    table = Table(table_data, colWidths=[150, 350])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 20))

    # Embed Asset Image (if exists)
    asset_image_name = data.get('IMAGE_UPLOAD_ASSEST_SERIAL_NO')
    asset_image_path = os.path.join('static', 'uploads', asset_image_name) if asset_image_name else None

    if asset_image_path and os.path.exists(asset_image_path):
        elements.append(Paragraph("Asset Serial Number Image:", heading_style))
        elements.append(Spacer(1, 8))
        elements.append(RLImage(asset_image_path, width=200, height=150))
        elements.append(Spacer(1, 12))
    else:
        elements.append(Paragraph("Asset image not available.", normal_style))

    # Embed Outlet Image (if exists)
    outlet_image_name = data.get('OUTLET_IMAGE')
    outlet_image_path = os.path.join('static', 'uploads', outlet_image_name) if outlet_image_name else None

    if outlet_image_path and os.path.exists(outlet_image_path):
        elements.append(Paragraph("Outlet Image:", heading_style))
        elements.append(Spacer(1, 8))
        elements.append(RLImage(outlet_image_path, width=200, height=150))
        elements.append(Spacer(1, 12))
    else:
        elements.append(Paragraph("Outlet image not available.", normal_style))

    # SGA Details Section
    elements.append(Spacer(1, 20))
    elements.append(Paragraph("SGA Details", heading_style))
    elements.append(Spacer(1, 12))

    if sga_details:
        sga_table_data = [['No.', 'Working Conditions', 'Product Type', 'Serial No', 'Asset Tag No']]
        for i, sga in enumerate(sga_details, start=1):
            sga_table_data.append([
                str(i),
                sga.get('WORKING_CONDITIONS', ''),
                sga.get('PRODUCT_TYPE', ''),
                sga.get('SERIAL_NO', ''),
                sga.get('ASSET_TAG_NO', ''),
            ])

        sga_table = Table(sga_table_data, colWidths=[40, 120, 120, 90, 90])
        sga_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(sga_table)
    else:
        elements.append(Paragraph("No SGA details available.", normal_style))

    elements.append(Spacer(1, 20))
    elements.append(Paragraph("Thank you for completing the outlet survey.", normal_style))

    doc.build(elements)
    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f'OutletSurvey_{transaction_id}.pdf',
        mimetype='application/pdf'
    )

def get_transaction_data1(transaction_id):
    query = """
    SELECT TOP 1
        [TRANSACTIONS_ID],
        [STATUS],
        [OUTLET_NAME],
        [OUTLET_MOBILE],
        [CITY],
        [STATE],
        [PINCODE],
        [OUTLET_ADDRESS],
        [LAT],
        [LONG],
        [DISTRIBUTOR_CODE],
        [DISTRIBUTOR_NAME],
        [CHANNEL],
        [VOLUME],
        [VPO],
        [SGA],
        [SGA_WORKING_CONDITIONS_1],
        [SGA_PRODUCT_TYPE_1],
        [SGA_SERIAL_NO_1],
        [SGA_ASSEST_TAG_NO_1],
        [BAIL_ID_1],
        [IMAGE_UPLOAD_ASSEST_SERIAL_NO],
        [OUTLET_IMAGE],
        [SGA_WORKING_CONDITIONS_2],
        [SGA_WORKING_CONDITIONS_3],
        [SGA_WORKING_CONDITIONS_4],
        [SGA_WORKING_CONDITIONS_5],
        [SGA_PRODUCT_TYPE_2],
        [SGA_PRODUCT_TYPE_3],
        [SGA_PRODUCT_TYPE_4],
        [SGA_PRODUCT_TYPE_5],
        [SGA_SERIAL_NO_2],
        [SGA_SERIAL_NO_3],
        [SGA_SERIAL_NO_4],
        [SGA_SERIAL_NO_5],
        [SGA_ASSEST_TAG_NO_2],
        [SGA_ASSEST_TAG_NO_3],
        [SGA_ASSEST_TAG_NO_4],
        [SGA_ASSEST_TAG_NO_5],
        [BAIL_ID_2],
        [BAIL_ID_3],
        [BAIL_ID_4],
        [BAIL_ID_5],
        [EMAIL_ID]
    FROM [EDS_SURVEY].[dbo].[EDS_NEW_OUTLET_SURVEY]
    WHERE [TRANSACTIONS_ID] = ?
    """

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (transaction_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
    except Exception as e:
        print(f"Error fetching transaction data: {e}")
        return None

@app.route('/new_view_data/<transaction_id>')
def new_view_data(transaction_id):
    # Connect to DB
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        # Fetch the data from EDS_NEW_OUTLET_SURVEY based on TRANSACTIONS_ID
    cursor.execute("""
        SELECT * FROM dbo.EDS_NEW_OUTLET_SURVEY WHERE TRANSACTIONS_ID = ?
    """, (transaction_id,))
    
    row = cursor.fetchone()
    
    if not row:
        abort(404, description=f"Transaction ID {transaction_id} not found in new outlet survey.")
    
    columns = [column[0] for column in cursor.description]
    data = dict(zip(columns, row))
    
    # Render a new template to display data
    return render_template('new_view_data.html', data=data)

@app.route('/new_complete_outlet')
def new_complete_outlet():
    # Connect to DB
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
    
    query = """
    SELECT
          TRANSACTIONS_ID,
          STATUS,
          OUTLET_NAME,
          OUTLET_MOBILE,
          OUTLET_ADDRESS,
          DISTRIBUTOR_CODE,
          DISTRIBUTOR_NAME
    FROM [EDS_SURVEY].[dbo].[EDS_NEW_OUTLET_SURVEY]
    """
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    return render_template('new_complete_outlet.html', rows=rows)

@app.route('/total_outlet_assigned')
def total_outlet_assigned():
    return "Total Outlet Assigned Page"

@app.route('/total_customer')
def total_customer():
    return "Total Customer Page"


@app.route('/eds_new_survey_report')
def eds_new_survey_report():
    if 'user' not in session:
        return redirect(url_for('index'))

    # Get filters from request
    email = request.args.get('email_id', '')
    distributor = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    filters = {
        'email': email,
        'distributor': distributor,
        'status': status
    }

    # Prepare base query
    query = "SELECT * FROM dbo.EDS_NEW_OUTLET_SURVEY WHERE 1=1"
    params = []

    if email:
        query += " AND EMAIL_ID = ?"
        params.append(email)
    if distributor:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor)
    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")

    # Fetch data
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()

        # Dropdown values
        cursor.execute("SELECT DISTINCT EMAIL_ID FROM dbo.EDS_NEW_OUTLET_SURVEY")
        email_list = [row[0] for row in cursor.fetchall() if row[0]]

        cursor.execute("SELECT DISTINCT DISTRIBUTOR_NAME FROM dbo.EDS_NEW_OUTLET_SURVEY")
        distributor_list = [row[0] for row in cursor.fetchall() if row[0]]

        # Main data
        cursor.execute(query, params)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    return render_template(
        'eds_new_survey_report.html',
        data=data,
        columns=columns,
        email_list=email_list,
        distributor_list=distributor_list,
        filters=filters
    )


@app.route('/change_distributor', methods=['GET', 'POST'])
def change_distributor():
    if 'user' not in session:
        return redirect(url_for('index'))

    try:
        page = int(request.args.get('page', 1))  # current page
        per_page = 10
        offset = (page - 1) * per_page

        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Handle POST: update distributor
            if request.method == 'POST':
                outlet_code = request.form['OUTLET_CODE']
                new_distributor = request.form['DISTRIBUTOR_CODE']

                cursor.execute("""
                    UPDATE EDS_OUTLET_SURVEY
                    SET DISTRIBUTOR_CODE = ?
                    WHERE OUTLET_CODE = ?
                """, (new_distributor, outlet_code))
                conn.commit()
                flash("Distributor updated successfully!", "info")

            # Count total rows for pagination
            cursor.execute("""
                SELECT COUNT(*) 
                FROM EDS_OUTLET_SURVEY s
                INNER JOIN EDS_DISTRIBUTOR_EMAILS d 
                    ON s.email_id = d.email_id 
                    AND s.DISTRIBUTOR_CODE = d.DISTRIBUTOR_CODE
                WHERE s.STATUS = 'PENDING' AND s.email_id = ?
            """, (session['user']['EMAIL_ID'],))
            total_records = cursor.fetchone()[0]
            total_pages = (total_records + per_page - 1) // per_page

            # Fetch paginated outlet data
            cursor.execute("""
                SELECT 
                    s.OUTLET_CODE, 
                    s.OUTLET_NAME, 
                    s.OUTLET_ADDRESS, 
                    s.DISTRIBUTOR_CODE,
                    dm.DISTRIBUTOR_NAME
                FROM EDS_OUTLET_SURVEY s
                INNER JOIN EDS_DISTRIBUTOR_EMAILS d 
                    ON s.email_id = d.email_id 
                    AND s.DISTRIBUTOR_CODE = d.DISTRIBUTOR_CODE
                LEFT JOIN Distributor_Master dm 
                    ON s.DISTRIBUTOR_CODE = dm.DISTRIBUTOR_CODE
                WHERE s.STATUS = 'PENDING' AND s.email_id = ?
                ORDER BY s.OUTLET_NAME
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """, (session['user']['EMAIL_ID'], offset, per_page))
            data = cursor.fetchall()

            # Fetch distributor list
            cursor.execute("""
                SELECT DISTINCT DISTRIBUTOR_CODE, DISTRIBUTOR_NAME
                FROM Distributor_Master
                ORDER BY DISTRIBUTOR_CODE
            """)
            distributor_list = cursor.fetchall()

            # Calculate pagination range
            start_page = max(1, page - 2)
            end_page = min(total_pages, page + 2)

    except Exception as e:
        print("DB Error:", e)
        data = []
        distributor_list = []
        total_pages = 1
        page = 1
        start_page = 1
        end_page = 1

    return render_template(
        'change_distributor.html',
        data=data,
        distributors=distributor_list,
        user=session['user'],
        page=page,
        total_pages=total_pages,
        start_page=start_page,
        end_page=end_page
    )

@app.route('/export_all_data_excel')
def export_all_data_excel():
    query = "SELECT * FROM dbo.EDS_COMPLETE_OUTLET_SURVEY"
    
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='All Survey Data')
    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='Complete_Survey_All_Data.xlsx'
    )
@app.route('/view_channels')
def view_channels():
    query = "SELECT CHANNEL_CODE, CHANNEL_NAME FROM dbo.CHANNEL_MASTER"

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

    return render_template(
        'view_channels.html',
        data=rows,
        columns=columns
    )
def get_data(transaction_id):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1
                TRANSACTIONS_ID,
                STATUS,
                OUTLET_NAME,
                OUTLET_MOBILE,
                CITY,
                STATE,
                PINCODE,
                OUTLET_ADDRESS,
                LAT,
                LONG,
                DISTRIBUTOR_CODE,
                DISTRIBUTOR_NAME,
                CHANNEL,
                VOLUME,
                VPO,
                SGA_WORKING_CONDITIONS_1,
                SGA_PRODUCT_TYPE_1,
                SGA_SERIAL_NO_1,
                SGA_ASSEST_TAG_NO_1,
                BAIL_ID,
                IMAGE_UPLOAD_ASSEST_SERIAL_NO,
                OUTLET_IMAGE,
                SGA_WORKING_CONDITIONS_2,
                SGA_WORKING_CONDITIONS_3,
                SGA_WORKING_CONDITIONS_4,
                SGA_WORKING_CONDITIONS_5,
                SGA_PRODUCT_TYPE_2,
                SGA_PRODUCT_TYPE_3,
                SGA_PRODUCT_TYPE_4,
                SGA_PRODUCT_TYPE_5,
                SGA_SERIAL_NO_2,
                SGA_SERIAL_NO_3,
                SGA_SERIAL_NO_4,
                SGA_SERIAL_NO_5,
                SGA_ASSEST_TAG_NO_2,
                SGA_ASSEST_TAG_NO_3,
                SGA_ASSEST_TAG_NO_4,
                SGA_ASSEST_TAG_NO_5,
                EMAIL_ID
            FROM dbo.EDS_NEW_OUTLET_SURVEY
            WHERE TRANSACTIONS_ID = ?
        """, transaction_id)

        row = cursor.fetchone()
        if row:
            # Convert row to dict with keys
            columns = [column[0] for column in cursor.description]
            return dict(zip(columns, row))
        return None

def get_sga_details(transaction_id):
    # Collect SGA data from multiple fields, grouping them for display
    data = get_data(transaction_id)
    if not data:
        return []
    sga_list = []
    # Assuming max 5 SGA records per transaction, extract them
    for i in range(1, 6):
        sga_serial = data.get(f'SGA_SERIAL_NO_{i}')
        if sga_serial:  # Only add if exists
            sga_list.append({
                'SGA_SERIAL_NO': sga_serial,
                'SGA_PRODUCT_TYPE': data.get(f'SGA_PRODUCT_TYPE_{i}'),
                'SGA_WORKING_CONDITIONS': data.get(f'SGA_WORKING_CONDITIONS_{i}'),
                'SGA_ASSEST_TAG_NO': data.get(f'SGA_ASSEST_TAG_NO_{i}'),
            })
    return sga_list
import csv
@app.route('/export_all_newdata_excel')
def export_all_newdata_excel():
    query = "SELECT * FROM dbo.vw_OutletSurveyCombined"
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=outlet_survey_data.csv"
    return response

@app.route('/export_filtered_newdata_excel')
def export_filtered_newdata_excel():
    # Get filter params from query string
    email_id = request.args.get('email_id', '')
    distributor_name = request.args.get('distributor_name', '')
    status = request.args.get('status', '')

    query = "SELECT * FROM dbo.vw_OutletSurveyCombined WHERE 1=1"
    params = []

    if email_id:
        query += " AND EMAIL_ID = ?"
        params.append(email_id)

    if distributor_name:
        query += " AND DISTRIBUTOR_NAME = ?"
        params.append(distributor_name)

    if status:
        query += " AND STATUS LIKE ?"
        params.append(f"%{status}%")

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    output = io.StringIO()
    writer = csv.writer(output)

    # Write header
    writer.writerow(columns)
    # Write data rows
    for row in rows:
        writer.writerow(row)

    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={
            "Content-Disposition": "attachment;filename=filtered_outlet_survey.csv"
        }
    )

if __name__ == '__main__':
    app.run(debug=True)