import os, sys 
import pandas as pd 
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
import sqlparse
from collections import OrderedDict, Counter
from github import Github
from databricks import sql 
import streamlit_authenticator as stauth
import yaml 
from yaml.loader import SafeLoader
from dotenv import load_dotenv
load_dotenv()

# LLM libraries
from langchain_core.prompts import PromptTemplate
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain.chains.llm import LLMChain
from langchain_openai import ChatOpenAI

# Function to load the user_query_history table
@st.cache_data
def load_user_query_history(user_name):
    # Getting the sample details of the selected table
    conn = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))

    query = f"SELECT * FROM dev_tools.sqlgenpro_user_query_history WHERE user_name = '{user_name}' AND timestamp > current_date - 7 ORDER BY query_count DESC LIMIT 5"
    df = pd.read_sql(sql=query,con=conn)
    return df


# Function to list all the catalog, schema and tables present in the database 
@st.cache_data
def list_catalog_schema_tables():
    with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN")) as connection:
        with connection.cursor() as cursor:
            # cursor.catalogs()
            # result_catalogs = cursor.fetchall()

            # cursor.schemas()
            # result_schemas = cursor.fetchall()

            cursor.tables()
            result_tables = cursor.fetchall()

            return result_tables
        

# Function to render the mermaid diagram
def process_llm_response_for_mermaid(response: str) -> str:
    # Extract the Mermaid code block from the response
    start_idx = response.find("```mermaid") + len("```mermaid")
    end_idx = response.find("```", start_idx)
    mermaid_code = response[start_idx:end_idx].strip()

    return mermaid_code

# Function to render the sql code
def process_llm_response_for_sql(response: str) -> str:
    # Extract the Mermaid code block from the response
    start_idx = response.find("```sql") + len("```sql")
    end_idx = response.find("```", start_idx)
    sql_code = response[start_idx:end_idx].strip()

    return sql_code


def mermaid(code: str) -> None:
    # Escaping backslashes for special characters in the code
    code_escaped = code.replace("\\", "\\\\").replace("`", "\\`")
    
    components.html(
        f"""
        <div id="mermaid-container" style="width: 100%; height: 100%; overflow: auto;">
            <pre class="mermaid">
                {code_escaped}
            </pre>
        </div>

        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
        </script>
        """,
        height=800  # You can adjust the height as needed
    )       

# Function to create the ERD diagram for the selected schema and tables 
@st.experimental_fragment      
@st.cache_data 
def create_erd_diagram(catalog,schema,tables_list):

    table_schema = {}

    # Iterating through each selected tables and get the list of columns for each table.
    for table in tables_list:

        conn = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                        http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                        access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
            
        query = f"DESCRIBE TABLE `{catalog}`.{schema}.{table}"
        df = pd.read_sql(sql=query,con=conn)
        cols = df['col_name'].tolist()
        col_types = df['data_type'].tolist()
        cols_dict = [f"{col} : {col_type}" for col,col_type in zip(cols,col_types)]
        table_schema[table] = cols_dict

    # Generating the mermaid code for the ERD diagram
    ### Defining the prompt template
    template_string = """ 
    You are an expert in creating ERD diagrams (Entity Relationship Diagrams) for databases. 
    You have been given the task to create an ERD diagram for the selected tables in the database. 
    The ERD diagram should contain the tables and the columns present in the tables. 
    You need to generate the Mermaid code for the complete ERD diagram.
    Make sure the ERD diagram is clear and easy to understand with proper relationships details.

    The selected tables in the database are given below (delimited by ##) in the dictionary format: Keys being the table names and values being the list of columns and their datatype in the table.

    ##
    {table_schema}
    ##

    Before generating the mermaid code, validate it and make sure it is correct and clear.     
    Give me the final mermaid code for the ERD diagram after proper analysis.
    """

    prompt_template = PromptTemplate.from_template(template_string)

    ### Defining the LLM chain
    llm_chain = LLMChain(
        llm=ChatOpenAI(model="gpt-4o-mini",temperature=0),
        prompt=prompt_template
    )

    response =  llm_chain.invoke({"table_schema":table_schema})
    output = response['text']    
    return output

# Function to create Quick Analysis questions based on the given schema and tables
@st.experimental_fragment
@st.cache_data
def quick_analysis(user_name,mermaid_code):
    ### Getting the user_query_history details to list the top 5 queries for the user
    df_user = load_user_query_history(user_name)

    # check if df_user is empty, if yes then create user_history = [] else create user_history = df_user['question'].tolist()
    if df_user.empty:
        user_history = []
    else:
        user_history = df_user['question'].tolist()


    ### Defining the output schema from the LLM        
    output_schema = ResponseSchema(name="quick_analysis_questions",description="Generated Quick Analysis questions for the given tables list")
    output_parser = StructuredOutputParser.from_response_schemas([output_schema])
    format_instructions = output_parser.get_format_instructions()

    ### Defining the prompt template
    template_string = """
    Using the provided Mermaid code for the ERD diagram (delimited by ##), generate the top 5 "quick analysis" questions based on the relationships between the tables which can be answered by creating an SQL code. 
    These questions should be practical and insightful, targeting the kind of business inquiries a product manager or analyst would typically investigate daily.
    If the user_history is empty, then create the questions based on the given Mermaid code. If the user_history is not empty, then create the questions based on the user_history and the given Mermaid code.

    Mermaid code:
    ##
    {mermaid_code}
    ##

    User History:
    ##
    {user_history}
    ##

    The output should be in a nestod JSON format with the following structure:
    {fomat_instructions}
     """
    
    prompt_template = PromptTemplate.from_template(template_string)
    
    ### Defining the LLM chain
    llm_chain = LLMChain(
        llm=ChatOpenAI(model="gpt-4o-mini",temperature=0),
        prompt=prompt_template,
        output_parser=output_parser
    )

    response =  llm_chain.invoke({"mermaid_code":mermaid_code, "user_history":user_history,"fomat_instructions":format_instructions})
    # output = response['text']  

    return response

# Function to create SQL code for the selected question and return the data from the database
@st.experimental_fragment
@st.cache_data
def create_sql(question,mermaid_code,catalog,schema):

    ### Defining the prompt template
    template_string = """ 
    You are provided with a text question and a Mermaid code that represents the Entity-Relationship Diagram (ERD) of selected tables from a database. 
    Your task is to generate a working SQL query using the fields and relationships between the tables as depicted in the Mermaid code. 
    The SQL query should be able to run in Databricks.

    INPUT:
    QUESTION:
    ##
    {question}
    ##

    MERMAID CODE:
    ##
    {mermaid_code}
    ##

    CATALOG:
    ## {catalog} ##

    SCHEMA:
    ## {schema} ##

    INSTRUCTIONS:
    ##
    Understand the ERD:
    Analyze the Mermaid code to understand the tables, their fields, and the relationships between them.
    Ensure you correctly identify primary and foreign keys based on the ERD.
    
    Match Field Names:
    Identify and match the field names from the question to the actual field names in the ERD. If the field names in the question are not exact, use your understanding of the ERD to find the closest match.
    
    Match Field Types:
    Identify and match the field datatypes from the question to the actual field datatypes in the ERD. All the SQL logic and operation should reflect keeping in the mind the field types.

    Generate SQL Code:
    Using the information from the ERD, write a working SQL query that addresses the text question.
    Ensure the SQL query uses correct table and field names.    
    The query should be optimized for execution in Databricks.
    
    SQL Formatting:
    Format the SQL query for readability, using proper indentation and line breaks.

    IMPORTANT: MAKE SURE THE OUTPUT IS JUST THE SQL CODE AND NOTHING ELSE. Ensure the appropriate CATALOG is used in the query and SCHEMA is specified when reading the tables.
    There should not be any limit statements in the generated SQL code.
    ##


    OUTPUT:
    """
    prompt_template = PromptTemplate.from_template(template_string)

    ### Defining the LLM chain
    llm_chain = LLMChain(
        llm=ChatOpenAI(model="gpt-4o-mini",temperature=0),
        prompt=prompt_template
    )

    response =  llm_chain.invoke({"question":question,"mermaid_code":mermaid_code,"catalog":catalog,"schema":schema})
    output = response['text']

    return output


# Function to load data from the database given the SQL query
@st.experimental_fragment
@st.cache_data
def load_data_from_query(query):
    # Getting the sample details of the selected table
    conn = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))

    query = query.replace(";","")
    query = query + f" LIMIT 1000;"
    df = pd.read_sql(sql=query,con=conn)
    return df         