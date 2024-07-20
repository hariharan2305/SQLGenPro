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
def process_llm_response(response: str) -> str:
    # Extract the Mermaid code block from the response
    start_idx = response.find("```mermaid") + len("```mermaid")
    end_idx = response.find("```", start_idx)
    mermaid_code = response[start_idx:end_idx].strip()

    return mermaid_code
        
def mermaid(code: str) -> None:
    components.html(
        f"""
        <div id="mermaid-container" style="width: 100%; height: 100%; overflow: auto;">
            <pre class="mermaid">
                {code}
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
def create_erd_diagram(catalog,schema,tables_list):

    table_schema = {}

    # Iterating through each selected tables and get the list of columns for each table.
    for table in tables_list:

        conn = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                        http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                        access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
            
        query = f"SELECT * FROM `{catalog}`.{schema}.{table} LIMIT 5"
        df = pd.read_sql(sql=query,con=conn)
        cols = df.columns.tolist()
        table_schema[table] = cols

    # Generating the mermaid code for the ERD diagram
    ### Defining the output schema from the LLM        
    output_schema = ResponseSchema(name="erd_mermaid_code",description="Generated Mermaid code for the ERD diagram")
    output_parser = StructuredOutputParser.from_response_schemas([output_schema])
    format_instructions = output_parser.get_format_instructions()

    ### Defining the prompt template
    template_string = """ 
    You are an expert in creating ERD diagrams (Entity Relationship Diagrams) for databases. 
    You have been given the task to create an ERD diagram for the selected tables in the database. 
    The ERD diagram should contain the tables and the columns present in the tables. 
    You need to generate the Mermaid code for the complete ERD diagram.
    Make sure the ERD diagram is clear and easy to understand with proper relationships details.

    The selected tables in the database are given below (delimited by ##) in the dictionary format: Keys being the table names and values being the list of columns in the table.

    ##
    {table_schema}
    ##

    Before generating the mermaid code, validate it and make sure it is correct and clear.     
    Give me the final mermaid code for the ERD diagram after proper analysis.
    """

#     template_string = """ 
#     Given the table_schema (table and their fields) within the double hash ## , can you create an ERD diagram using mermaid to show the entity relationship? 
# Also make sure to create the diagram in a dimension and fact table schema to make it clear and easy to grasp. After proper analysis just give me final mermaid code.

# ##
# {table_schema}
# ##

#     """

    prompt_template = PromptTemplate.from_template(template_string)

    ### Defining the LLM chain
    # llm_chain = LLMChain(
    #     llm=ChatOpenAI(model="gpt-3.5-turbo",temperature=0),
    #     prompt=prompt_template,
    #     output_parser=output_parser
    # )
    llm_chain = LLMChain(
        llm=ChatOpenAI(model="gpt-4o-mini",temperature=0),
        prompt=prompt_template
    )

    ### Generating the Mermaid code for the ERD diagram
    # response =  llm_chain.invoke({"table_schema":table_schema, "format_instructions":format_instructions})
    # output = response['text']['erd_mermaid_code']

    response =  llm_chain.invoke({"table_schema":table_schema})
    output = response['text']    
    return output

            