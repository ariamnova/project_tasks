import snowflake.connector
import pandas as pd
from tqdm import tqdm
from googletrans import Translator
from spellchecker import SpellChecker
from snowflake.connector.pandas_tools import write_pandas
import os

# snowflake connection parameters

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}


# load leads data from snowflake
def load_leads_from_snowflake():
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        query = "SELECT * FROM LEADS_CLEANED;"
        leads = pd.read_sql(query, conn)
    return leads

# translate to english
def translate_to_english(text):
    translator = Translator()
    try:
        translated = translator.translate(text, dest="en")
        return translated.text
    except Exception:  # Adjust exception type for better handling
        return text

# spell correction
def correct_spelling(text, spell_checker):
    words = text.split()
    corrected_words = [spell_checker.correction(word) or word for word in words]
    return " ".join(corrected_words)

# job title categorization
def categorize_job_title(title):
    title_lower = str(title).lower()
    if any(keyword in title_lower for keyword in [
        "developer", "engineer", "scientist", "analyst", "specialist", "senior"
    ]):
        return "Individual Contributor"
    elif any(keyword in title_lower for keyword in ["intern", "junior", "trainee"]):
        return "Entry-level"
    elif any(keyword in title_lower for keyword in ["manager", "lead", "supervisor"]):
        return "Managerial-level"
    elif any(keyword in title_lower for keyword in ["director", "head"]):
        return "Director-level"
    elif any(keyword in title_lower for keyword in ["chief", "officer", "ceo"]):
        return "Executive-level"
    return "Uncategorized"

# industry categorization
def categorize_lead_industry(industry):
    industry_lower = str(industry).lower()
    if "manufacturer" in industry_lower:
        return "Manufacturing"
    elif "software" in industry_lower:
        return "Technology"
    elif "education" in industry_lower:
        return "Education and Training"
    elif "utilities" in industry_lower:
        return "Energy and Utilities"
    elif "transportation" in industry_lower:
        return "Transport and Automotive"
    elif "finance" in industry_lower:
        return "Finance and Insurance"
    elif "retail" in industry_lower:
        return "Retail and Consumer Services"
    elif "government" in industry_lower:
        return "Public Sector and Non-Profit"
    elif "healthcare" in industry_lower:
        return "Healthcare and Life Sciences"
    elif "business" in industry_lower:
        return "Business and Professional Services"
    elif "entertainment" in industry_lower:
        return "Entertainment"
    return "Uncategorized"

# finalize and save processed data
def save_to_snowflake(dataframe, table_name):
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            LEAD_HASHED_ID VARCHAR,
            CAMPAIGN_JOINED_DATE DATE,
            LEAD_INDUSTRY VARCHAR,
            COUNTRY VARCHAR,
            REGION VARCHAR,
            CAMPAIGN_NAME VARCHAR,
            LEAD_SOURCE VARCHAR,
            SOURCE_CATEGORY VARCHAR,
            LEAD_STATUS VARCHAR,
            JOB_CATEGORY VARCHAR,
            LEAD_INDUSTRY_CATEGORY VARCHAR
        );
        """)
        success, _, nrows, _ = write_pandas(conn, dataframe, table_name)
        print(f"Successfully loaded {nrows} rows into {table_name}." if success else "Failed to load data.")

# execution
if __name__ == "__main__":
    leads = load_leads_from_snowflake()

    # process job titles
    tqdm.pandas(desc="Processing Job Titles")
    spell_checker = SpellChecker()
    leads["JOB_TITLE_CLEANED"] = leads["JOB_TITLE_CLEANED"].progress_apply(correct_spelling, args=(spell_checker,))
    leads["JOB_CATEGORY"] = leads["JOB_TITLE_CLEANED"].apply(categorize_job_title)

    # process industries
    tqdm.pandas(desc="Processing Industries")
    leads["LEAD_INDUSTRY"] = leads["LEAD_INDUSTRY"].progress_apply(translate_to_english)
    leads["LEAD_INDUSTRY_CATEGORY"] = leads["LEAD_INDUSTRY"].apply(categorize_lead_industry)

    columns_to_select = [
        "LEAD_HASHED_ID", "CAMPAIGN_JOINED_DATE", "LEAD_INDUSTRY", "COUNTRY", "REGION",
        "CAMPAIGN_NAME", "LEAD_SOURCE", "SOURCE_CATEGORY", "LEAD_STATUS", "JOB_CATEGORY", "LEAD_INDUSTRY_CATEGORY"
    ]
    filtered_data = leads[columns_to_select]

    save_to_snowflake(filtered_data, "LEADS_PROCESSED")
