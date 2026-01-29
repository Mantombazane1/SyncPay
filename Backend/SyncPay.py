from flask import Flask, render_template, request, redirect, url_for, flash
import mysql.connector
import requests
import json
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, abort,jsonify
import os
from datetime import datetime, date
from pdf2image import convert_from_bytes
import pytesseract
import re
import traceback
from playwright.sync_api import sync_playwright
import pdfplumber




app = Flask(__name__)
app.secret_key = "supersecretkey"

# ---------------- Database Connection ----------------
def get_db_connection():
    db_config = {
        "host": "sql39.cpt3.host-h.net",
        "user": "mindwuejax_1",
        "password": "zS07NwN90b4LlMNyRpIu",
        "database": "mindworx_academy_db",
        "port": 3306
    }
    return mysql.connector.connect(**db_config)





# ---------------- Moodle Config ----------------
MOODLE_URL = "https://lms.mindworx.co.za/academy/pluginfile.php"
MOODLE_TOKEN = "c392e2a842cfde675c2aaf9699fba5d1"
MOODLE_COOKIE = "0d5d2785fbd0b8a78b272d94df3e5464"


# ---------------- OCR Paths ----------------
TESSERACT_PATH = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPLER_PATH = r"C:\Users\Lenovo-User\Pictures\poppler-25.07.0\Library\bin"

# ---------------- SimplePay API Setup ----------------
API_BASE = "https://api.payroll.simplepay.cloud/v1"
API_KEY = "d291a57bb9e1c1bfb8f49e859d1c096c"
CLIENT_ID = "332431"
headers = {
    "Authorization": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}# ---------------- Helper Functions ----------------
def format_start_date(date_val):
    if isinstance(date_val, (datetime, date)):
        return date_val.strftime("%Y-%m-%d")
    if date_val in [None, "NULL"]:
        return "2025-01-01"

def fetch_pdf(url):
    """Download PDF using MoodleSession cookie"""
    try:
        session = requests.Session()
        if MOODLE_COOKIE:
            session.cookies.set("MoodleSession", MOODLE_COOKIE)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        if resp.content[:4] != b"%PDF":
            return None
        return resp.content
    except:
        return None

def extract_text_or_ocr(pdf_bytes):
    """Extract text from PDF using OCR"""
    try:
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
        images = convert_from_bytes(pdf_bytes, poppler_path=POPLER_PATH)
        text = "\n".join([pytesseract.image_to_string(img) for img in images])
        return text
    except:
        return ""
def filter_account_fields(text, db_record=None):
    """Extract main bank account fields from text, compare with DB, and show mismatches only or Verified if all match"""
    import re
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)

    patterns = {
        "bank": r"(ABSA|FNB|STANDARD BANK|CAPITEC|NEDBANK|DISCOVERY BANK|OLD MUTUAL|TYME BANK|AFRICAN BANK|BIDVEST BANK)",
        "Account Status": r"(Account Status[:\s]*)(Active|Inactive)",
        "Account Type": r"(Account Type[:\s]*)(Savings|Cheque|Transmission|Current)",
        "Account Number": r"(Account Number[:\s]*)(\d{6,12})",
        "Branch Code": r"(Branch Code[:\s]*)(\d{6})"
    }

    results = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if key == "bank":
            results[key] = match.group(1).strip() if match else "Not Available"
        else:
            results[key] = match.group(2).strip() if match else "Not Available"

    output_lines = []
    mismatches = []

    fields_map = {
        "bank": "bank",
        "Account Number": "account_no",
        "Account Type": "account_type",
        "Branch Code": "Branch_Code"
    }

    for ocr_field, db_field in fields_map.items():
        ocr_value = results.get(ocr_field, "Not Available")
        db_value = str(db_record.get(db_field, "Not Available")) if db_record else "N/A"
        if ocr_value.strip().upper() != db_value.strip().upper():
            mismatches.append(f"{ocr_field}: {ocr_value} | Status: Mismatch")

    # If there are mismatches, show them; otherwise, show "Verified"
    if mismatches:
        output_lines.extend(mismatches)
    else:
        output_lines.append("Verified")

    # Always show Account Status
    output_lines.append(f"Account Status: {results['Account Status']}")

    # Join lines with <br> for HTML display
    return "<br>".join(output_lines)

# ---------------- Fetch Learners ----------------
def fetch_learners_by_job(job_title=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True, buffered=True)
    
     # Fetch learners and their PDF info
    sql = """
    SELECT 
        u.id,
        u.firstname,
        u.lastname,
        u.idnumber,
        u.email,
        u.city AS Group_name,
        u.timecreated AS startdate,
        c.fullname AS course_name,

        -- Custom user fields
        MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
        MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
        MIN(e.enrolstartdate) AS enrol_startdate,
        MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
        MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
        MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
        MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
        MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
        MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
        MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
        MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
        MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
        MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
        MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,



MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
        MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
        MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
        MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
        MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
        MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
        MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
        MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
        MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
        MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,




        -- Bank confirmation PDF info
        MAX(mf.filename) AS bank_confirmation_filename,
        MAX(CONCAT(
            'https://lms.mindworx.co.za/academy/pluginfile.php/',
            mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
            mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
            '?token=c392e2a842cfde675c2aaf9699fba5d1'
        )) AS bank_confirmation_url

    FROM mdl_user u
    LEFT JOIN mdl_user_info_data d ON d.userid = u.id
    LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
    LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
    LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
    LEFT JOIN mdl_course c ON c.id = e.courseid
    LEFT JOIN mdl_files mf 
        ON mf.userid = u.id
        AND mf.filename LIKE '%.pdf'
        AND mf.filearea NOT IN ('draft', 'private')
        AND mf.filename LIKE '%confirmation%'
        AND mf.filename != '.'
    WHERE u.deleted = 0
    GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
    ORDER BY u.id;
    """
    
    cursor.execute(sql)
    learners = cursor.fetchall()

    for learner in learners:
        pdf_url = learner.get("bank_confirmation_url")
        if pdf_url:
            pdf_bytes = fetch_pdf(pdf_url)
            if pdf_bytes:
                text = extract_text_or_ocr(pdf_bytes)
                learner["ocr_status"] = filter_account_fields(text, db_record=learner)
            else:
                learner["ocr_status"] = "Unable to fetch PDF."
        else:
            learner["ocr_status"] = "pending"

    conn.close()
    return learners





# ---------------- Routes ----------------
@app.route("/")
def admin_home():
    return render_template("adminhome.html")


@app.route("/view-data")
def view_learn():
    learners = fetch_learners_by_job()
    return render_template("ViewAlllearners.html", learners=learners)


#courses routes 
@app.route("/business-analysis")
def business_analysis():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,
       

                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Business Analysis",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("businessanalysisdata.html", learners=learners)

    finally:
        cursor.close()
        db.close()

  


#courses routes 
@app.route("/cyber_data")
def cyber_data():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,
       

                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Cyber security",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("cyberdata.html", learners=learners)

    finally:
        cursor.close()
        db.close()











#courses routes 

@app.route("/data_eng")
def data_eng():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Data engineering",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("dataengdata.html", learners=learners)

    finally:
        cursor.close()
        db.close()








@app.route("/data_scie")
def data_scie():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Data science",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("datasciencedata.html", learners=learners)

    finally:
        cursor.close()
        db.close()







@app.route("/rp_data")
def rp_data():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Robotic Process Automation",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("rpadata.html", learners=learners)

    finally:
        cursor.close()
        db.close()





@app.route("/sys_eng")
def sys_eng():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("Software engineering",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("sedataview.html", learners=learners)

    finally:
        cursor.close()
        db.close()







@app.route("/sys_data")
def sys_data():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("System development",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("Systemdevdata.html", learners=learners)

    finally:
        cursor.close()
        db.close()






@app.route("/sys_sup")
def sys_sup():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                u.id,
                u.firstname,
                u.lastname,
                u.idnumber,
                u.email,
                u.city AS Group_name,
                u.timecreated AS startdate,
                c.fullname AS course_name,

                -- Custom user fields
                MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                MIN(e.enrolstartdate) AS enrol_startdate,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                MAX(CASE WHEN f.shortname = 'bank_confirmation' THEN d.data END) AS bank_confirmation_flag,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,

                -- Address fields
                MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street_Name,
                MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex_Building,
                MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb_District,
                MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City_Town,
                MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Postal_Code,
                MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country,
                MAX(CASE WHEN f.shortname = 'certified_identity_copy' THEN d.data END) AS certified_identity_copy,
MAX(CASE WHEN f.shortname = 'qualifications' THEN d.data END) AS certified_qualifications,
MAX(CASE WHEN f.shortname = 'employment_contract' THEN d.data END) AS signed_contract,
MAX(CASE WHEN f.shortname = 'tax_reference' THEN d.data END) AS tax_reference_letter,


                -- Bank confirmation PDF info
                MAX(mf.filename) AS bank_confirmation_filename,
                MAX(CONCAT(
                    'https://lms.mindworx.co.za/academy/pluginfile.php/',
                    mf.contextid, '/', mf.component, '/', mf.filearea, '/', 
                    mf.itemid, '/', REPLACE(mf.filename, ' ', '%20'),
                    '?token=c392e2a842cfde675c2aaf9699fba5d1'
                )) AS bank_confirmation_url

            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
            LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
            LEFT JOIN mdl_course c ON c.id = e.courseid
            LEFT JOIN mdl_files mf 
                ON mf.userid = u.id
                AND mf.filename LIKE '%%.pdf'
                AND mf.filearea NOT IN ('draft', 'private')
                AND mf.filename LIKE '%%confirmation%%'
                AND mf.filename != '.'
            WHERE u.deleted = 0
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            HAVING Course = %s
            ORDER BY u.id
        """, ("System support",))

        learners = cursor.fetchall()

        # 🔹 Perform OCR status detection
        for learner in learners:
            pdf_url = learner.get("bank_confirmation_url")
            if pdf_url:
                pdf_bytes = fetch_pdf(pdf_url)
                if pdf_bytes:
                    text = extract_text_or_ocr(pdf_bytes)
                    learner["ocr_status"] = filter_account_fields(text, db_record=learner)
                else:
                    learner["ocr_status"] = "Unable to fetch PDF."
            else:
                learner["ocr_status"] = "Pending"

        return render_template("systemsupportdata.html", learners=learners)

    finally:
        cursor.close()
        db.close()

# ---------------- Upload Learner to SimplePay ----------------
@app.route("/upload_single/<int:id>", methods=["POST"])
def upload_single(id):
    db = get_db_connection()
    cursor = db.cursor(dictionary=True, buffered=True)  # <-- buffered to fix unread result
    try:
        # 1️ Fetch learner info
        cursor.execute("""
        SELECT    
            u.id,
            u.firstname,
            u.lastname,
            u.idnumber,
            u.email,
            u.city AS Group_name,
            u.timecreated AS startdate,
            c.fullname AS course_name,
            MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
            MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
            MIN(e.enrolstartdate) AS enrol_startdate,
            MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
            MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
            MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
            MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
            MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
            MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
            MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
            MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
            MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,
            MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street,
            MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
            MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
            MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex,
            MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb,
            MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City,
            MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Code,
            MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
            MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country
        FROM mdl_user u
        LEFT JOIN mdl_user_info_data d ON d.userid = u.id
        LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
        LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
        LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
        LEFT JOIN mdl_course c ON c.id = e.courseid
        WHERE u.deleted = 0 AND u.id = %s
        GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
        """, (id,))
        pi = cursor.fetchone()

        if not pi:
            return jsonify({"status": "error", "message": "Learner not found"}), 404

        # 2️ Check if simplepay_id exists
        simplepay_id = pi.get("simplepay_id")
        if simplepay_id not in [None, "", "NULL"]:
            resp = requests.get(f"{API_BASE}/clients/{CLIENT_ID}/employees/{simplepay_id}", headers=headers)
            if resp.status_code == 404:
                # Reset simplepay_id in Moodle
                cursor.execute("""
                    UPDATE mdl_user_info_data
                    SET data = NULL
                    WHERE userid = %s AND fieldid = (SELECT id FROM mdl_user_info_field WHERE shortname='simplepay_id')
                """, (id,))
                db.commit()
            else:
                return jsonify({"status": "exists", "message": "Learner already uploaded"}), 200

        # 3️ Prepare Bank Info
        bank_map = {
            "ABSA": "ABSA",
            "ABSA BANK": "ABSA",
            "FNB": "FNB",
            "FIRST NATIONAL BANK": "FNB",
            "STANDARD BANK": "STANDARD BANK",
            "CAPITEC": "CAPITEC",
            "NEDBANK": "NEDBANK",
            "DISCOVERY BANK": "DISCOVERY BANK",
            "TYME BANK": "TYME BANK",
        }
        db_bank_name = (pi.get("bank") or "").upper().strip()
        normalized_bank = bank_map.get(db_bank_name, "OTHER")
        branch_code = str(pi.get("Branch_Code") or "")
        if branch_code == "4700010":
            branch_code = "470010"
        holder_map = {"employee": "1", "joint": "2", "third_party": "3"}
        db_holder = (pi.get("holder_relationship") or "").lower().replace(" ", "_")
        normalized_holder = holder_map.get(db_holder, "1")
        account_type_map = {"Cheque": 1, "Savings": 2, "Transmission": 3}
        bank_info = {
            "bank_name": normalized_bank,
            "account_number": str(pi.get("account_no") or ""),
            "branch_code": branch_code,
            "account_type": account_type_map.get((pi.get("account_type") or "Cheque").capitalize(), 1),
            "holder_relationship": normalized_holder
        }

        # 4️ Build employee payload
        employee_data = {
            "employee": {
                "wave_id": 1351111380,
                "first_name": pi.get("firstname"),
                "last_name": pi.get("lastname"),
                "birthdate": str(pi.get("birthdate")),
                "appointment_date": str(pi.get("startdate")),
                "identification_type": "rsa_id",
                "id_number": pi.get("idnumber"),
                "email": pi.get("email"),
                "job_title": pi.get("Course") or pi.get("course_name"),
                "income_tax_number": pi.get("Tax_number"),
                "payment_method": "eft_manual",
                "bank_account": bank_info,
                "physical_address": {
                    "street_number": pi.get("Street_Number"),
                    "street_or_farm_name": pi.get("Street"),
                    "suburb_or_district": pi.get("Suburb"),
                    "city_or_town": pi.get("City"),
                    "unit_number": pi.get("Unit_Number"),
                    "complex": pi.get("Complex"),
                    "code": pi.get("Code"),
                },
                "postal_address": {"same_as_physical": True},
                "custom_fields": {"70922": pi.get("firstname")}
            }
        }

        # 5️ Send to SimplePay API
        response = requests.post(
            f"{API_BASE}/clients/{CLIENT_ID}/employees",
            headers=headers,
            data=json.dumps(employee_data)
        )

        try:
            data = response.json()
        except Exception:
            data = response.text
            return jsonify({"status": "error", "message": f"SimplePay response error: {data}"}), 500

        # 6️ Update simplepay_id in Moodle
        if isinstance(data, dict) and data.get("id"):
            cursor.execute("SELECT id FROM mdl_user_info_field WHERE shortname = 'simplepay_id'")
            field = cursor.fetchone()
            if not field:
                return jsonify({"status": "error", "message": "'simplepay_id' field not found."}), 500
            field_id = field["id"]
            cursor.execute("""
                INSERT INTO mdl_user_info_data (userid, fieldid, data)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE data = VALUES(data)
            """, (id, field_id, data["id"]))
            db.commit()
            return jsonify({"status": "success", "message": f"Learner uploaded successfully! SimplePay ID: {data['id']}"}), 200

        return jsonify({"status": "error", "message": f"Error uploading to SimplePay: {data}"}), 500

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        cursor.close()
        db.close()




# ---------------- Bulk Upload Learners to SimplePay ----------------
@app.route("/upload_bulk", methods=["POST"])
def upload_bulk():
    selected_ids = request.form.getlist("selected_ids")
    if not selected_ids:
        return "No learners selected", 400

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    results = []

    try:
        for id in selected_ids:
            # 1️ Fetch learner info (same as single upload)
            cursor.execute("""
                SELECT 
                    u.id,
                    u.firstname,
                    u.lastname,
                    u.idnumber,
                    u.email,
                    u.city AS Group_name,
                    u.timecreated AS startdate,
                    c.fullname AS course_name,
                    MAX(CASE WHEN f.shortname = 'Start_Date' THEN d.data END) AS startdate,
                    MAX(CASE WHEN f.shortname = 'Date_of_birth' THEN d.data END) AS birthdate,
                    MIN(e.enrolstartdate) AS enrol_startdate,
                    MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                    MAX(CASE WHEN f.shortname = 'account_name' THEN d.data END) AS account_name,
                    MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                    MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                    MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                    MAX(CASE WHEN f.shortname = 'Sponsor' THEN d.data END) AS Sponsor,
                    MAX(CASE WHEN f.shortname = 'Course' THEN d.data END) AS Course,
                    MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS Branch_Code,
                    MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS Tax_number,
                    MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id,
                    MAX(CASE WHEN f.shortname = 'Street' THEN d.data END) AS Street,
                    MAX(CASE WHEN f.shortname = 'Street_Number' THEN d.data END) AS Street_Number,
                    MAX(CASE WHEN f.shortname = 'Unit_Number' THEN d.data END) AS Unit_Number,
                    MAX(CASE WHEN f.shortname = 'Complex' THEN d.data END) AS Complex,
                    MAX(CASE WHEN f.shortname = 'Suburb' THEN d.data END) AS Suburb,
                    MAX(CASE WHEN f.shortname = 'City' THEN d.data END) AS City,
                    MAX(CASE WHEN f.shortname = 'Code' THEN d.data END) AS Code,
                    MAX(CASE WHEN f.shortname = 'Province' THEN d.data END) AS Province,
                    MAX(CASE WHEN f.shortname = 'Country' THEN d.data END) AS Country
                FROM mdl_user u
                LEFT JOIN mdl_user_info_data d ON d.userid = u.id
                LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
                LEFT JOIN mdl_user_enrolments ue ON ue.userid = u.id
                LEFT JOIN mdl_enrol e ON e.id = ue.enrolid
                LEFT JOIN mdl_course c ON c.id = e.courseid
                WHERE u.deleted = 0 AND u.id = %s
                GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated, c.fullname
            """, (id,))
            pi = cursor.fetchone()

            if not pi:
                results.append(f"ID {id}: Learner not found")
                continue

            # 2️ Skip already uploaded learners
            if pi.get("simplepay_id") not in [None, "", "NULL"]:
                results.append(f"ID {id}: Already uploaded to SimplePay")
                continue

            # 3️ Prepare bank info
            bank_info = None
            if pi.get("account_no"):
                db_acc_type = pi.get("account_type", "Cheque")
                branch_code = str(pi.get("Branch_Code") or "")
                if branch_code == "4700010":
                    branch_code = "470010"

                account_type_map = {"Cheque": 1, "Savings": 2, "Transmission": 3}
                bank_info = {
                    "bank_name": (pi.get("bank") or "").upper().strip(),
                    "account_number": str(pi.get("account_no")),
                    "branch_code": branch_code,
                    "account_type": account_type_map.get(db_acc_type.capitalize(), 2)
                }

            # 4️ Build employee data payload
            employee_data = {
                "employee": {
                    "wave_id": 1351111380,
                    "first_name": pi.get("firstname"),
                    "last_name": pi.get("lastname"),
                    "birthdate": str(pi.get("birthdate")),
                    "appointment_date": str(pi.get("startdate")),
                    "identification_type": "rsa_id",
                    "id_number": pi.get("idnumber"),
                    "email": pi.get("email"),
                "job_title": pi.get("Course_name"),
                    
                    "income_tax_number": pi.get("Tax_number"),
                    "payment_method": "eft_manual",
                    "bank_account": bank_info,
                    "physical_address": {
                        "street_number": pi.get("Street_Number"),
                        "street_or_farm_name": pi.get("Street"),
                        "suburb_or_district": pi.get("Suburb"),
                        "city_or_town": pi.get("City"),
                        "unit_number": pi.get("Unit_Number"),
                        "complex": pi.get("Complex"),
                        "code": pi.get("Code"),
                    },
                    "postal_address": {"same_as_physical": True},
                    "custom_fields": {
                        "70922": pi.get("firstname"),
                        "53621": pi.get("email")
                    }
                }
            }

            # 5️ Send to SimplePay API
            response = requests.post(
                f"{API_BASE}/clients/{CLIENT_ID}/employees",
                headers=headers,
                data=json.dumps(employee_data)
            )

            try:
                data = response.json()
            except:
                data = {"error": response.text}

            #  Handle both success formats
            if response.status_code in [200, 201]:
                simplepay_id = None

                # Case 1: response includes employee object
                if isinstance(data, dict) and "employee" in data:
                    simplepay_id = data["employee"].get("id")

                # Case 2: SimplePay returned only message + id
                elif isinstance(data, dict) and "id" in data:
                    simplepay_id = data["id"]

                if simplepay_id:
                    cursor.execute("""
                        UPDATE mdl_user_info_data
                        SET data = %s
                        WHERE userid = %s AND fieldid = (SELECT id FROM mdl_user_info_field WHERE shortname='simplepay_id')
                    """, (simplepay_id, id))
                    db.commit()
                    results.append(f"ID {id}: Uploaded successfully → SimplePay ID {simplepay_id}")
                    continue

            #  Any other case = failure
            results.append(f"ID {id}: Failed → {data}")

    except Exception as e:
        db.rollback()
        return f"Error during bulk upload: {str(e)}", 500

    finally:
        cursor.close()
        db.close()

    return "<br>".join(results)

#ENDS BULK

import requests

@app.route("/update/<int:id>", methods=["GET", "POST"])
def update_learner(id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        if request.method == "POST":
            user_id = id

            # --- Collect user info ---
            user_fields = {
                "firstname": request.form.get("first_name"),
                "lastname": request.form.get("last_name"),
                "idnumber": request.form.get("id_no"),
                "city": request.form.get("group_name"),
                "email": request.form.get("email"),
                "timecreated": request.form.get("start_date"),
            }

            info_fields = {
                "Tax_number": request.form.get("tax_reference"),
                "bank": request.form.get("bank"),
                "account_type": request.form.get("account_type"),
                "account_number": request.form.get("account_no"),
                "Branch_Code": request.form.get("branch_code"),
                "holder_relationship": request.form.get("holder_relationship"),
            }

            # --- Update mdl_user table ---
            updates = []
            values = []
            for col, val in user_fields.items():
                if val:
                    updates.append(f"{col}=%s")
                    values.append(val)

            if updates:
                sql = f"UPDATE mdl_user SET {', '.join(updates)} WHERE id=%s"
                values.append(user_id)
                cursor.execute(sql, values)

            # --- Update or insert mdl_user_info_data fields ---
            for shortname, value in info_fields.items():
                if not value:
                    continue
                cursor.execute("SELECT id FROM mdl_user_info_field WHERE shortname=%s", (shortname,))
                field = cursor.fetchone()
                if not field:
                    continue
                fieldid = field["id"]

                cursor.execute(
                    "SELECT id FROM mdl_user_info_data WHERE userid=%s AND fieldid=%s",
                    (user_id, fieldid)
                )
                existing = cursor.fetchone()
                if existing:
                    cursor.execute(
                        "UPDATE mdl_user_info_data SET data=%s WHERE id=%s",
                        (value, existing["id"])
                    )
                else:
                    cursor.execute(
                        "INSERT INTO mdl_user_info_data (userid, fieldid, data) VALUES (%s,%s,%s)",
                        (user_id, fieldid, value)
                    )

            # Commit all changes
            conn.commit()

            # --- Fetch simplepay_id from mdl_user_info_data ---
            cursor.execute("""
                SELECT d.data AS simplepay_id
                FROM mdl_user_info_data d
                JOIN mdl_user_info_field f ON f.id = d.fieldid
                WHERE d.userid=%s AND f.shortname='simplepay_id'
            """, (user_id,))
            row = cursor.fetchone()
            simplepay_id = row["simplepay_id"] if row else None

            # --- Sync with SimplePay if ID exists ---
            if simplepay_id:
                API_BASE = "https://api.payroll.simplepay.cloud/v1"
                API_KEY = "d291a57bb9e1c1bfb8f49e859d1c096c"  # replace with real key
                headers = {"Authorization": API_KEY, "Content-Type": "application/json"}

                # Employee info
                employee_data = {}
                if user_fields["firstname"]:
                    employee_data["first_name"] = user_fields["firstname"]
                if user_fields["lastname"]:
                    employee_data["last_name"] = user_fields["lastname"]
                if user_fields["idnumber"]:
                    employee_data["id_number"] = user_fields["idnumber"]
                if info_fields.get("Tax_number"):
                    employee_data["tax_number"] = info_fields["Tax_number"]
                if user_fields["timecreated"]:
                    employee_data["start_date"] = str(user_fields["timecreated"])

                if employee_data:
                    url_patch = f"{API_BASE}/employees/{simplepay_id}"
                    response = requests.patch(url_patch, headers=headers, json={"employee": employee_data})
                    if response.status_code not in [200, 201]:
                        print(f"SimplePay employee update failed → {response.status_code}: {response.text}")

                # Bank info
                bank_data = {}
                if info_fields.get("bank"):
                    bank_data["bank_name"] = info_fields["bank"]
                if info_fields.get("account_number"):
                    bank_data["account_number"] = info_fields["account_number"]
                if info_fields.get("account_type"):
                    bank_data["account_type"] = info_fields["account_type"]
                if info_fields.get("Branch_Code"):
                    bank_data["branch_code"] = info_fields["Branch_Code"]

                if bank_data:
                    bank_url = f"{API_BASE}/employees/{simplepay_id}/bank_account"
                    response = requests.patch(bank_url, headers=headers, json={"bank_account": bank_data})
                    if response.status_code not in [200, 201]:
                        print(f"SimplePay bank update failed → {response.status_code}: {response.text}")

            cursor.close()
            conn.close()
            flash(" Learner updated successfully (Moodle + SimplePay)!", "success")
            return redirect(url_for("view_learn"))

        # ---------------- GET request ----------------
        cursor.execute("""
            SELECT 
                u.id, u.firstname, u.lastname, u.idnumber, u.email,
                u.city AS group_name, u.timecreated AS start_date,
                MAX(CASE WHEN f.shortname = 'Tax_number' THEN d.data END) AS tax_reference,
                MAX(CASE WHEN f.shortname = 'bank' THEN d.data END) AS bank,
                MAX(CASE WHEN f.shortname = 'account_number' THEN d.data END) AS account_no,
                MAX(CASE WHEN f.shortname = 'account_type' THEN d.data END) AS account_type,
                MAX(CASE WHEN f.shortname = 'Branch_Code' THEN d.data END) AS branch_code,
                MAX(CASE WHEN f.shortname = 'holder_relationship' THEN d.data END) AS holder_relationship,
                MAX(CASE WHEN f.shortname = 'simplepay_id' THEN d.data END) AS simplepay_id
            FROM mdl_user u
            LEFT JOIN mdl_user_info_data d ON d.userid = u.id
            LEFT JOIN mdl_user_info_field f ON f.id = d.fieldid
            WHERE u.id=%s
            GROUP BY u.id, u.firstname, u.lastname, u.idnumber, u.email, u.city, u.timecreated
        """, (id,))
        learner_info = cursor.fetchone()

        cursor.close()
        conn.close()

        if not learner_info:
            flash(" Learner not found!", "warning")
            return redirect(url_for("view_learn"))

        return render_template("update_all.html", user_id=id, learner=learner_info)

    except Exception as e:
        flash(f" Update failed: {e}", "danger")
        print(f" Update Error: {e}")
        return redirect(url_for("view_learn"))



@app.route("/log-in")
def log_in():
    return render_template("login.html")


# ---------------- Run Flask App ----------------
if __name__ == "__main__":
    app.run(debug=True)