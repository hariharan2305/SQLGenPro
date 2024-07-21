import os, sys 
import pandas as pd 
import numpy as np
import streamlit as st
import sqlparse
from collections import OrderedDict, Counter
from github import Github
from databricks import sql 
import streamlit_authenticator as stauth
import yaml 
from yaml.loader import SafeLoader
from dotenv import load_dotenv
load_dotenv()

# Brining the python scripts from the src folder
sys.path.append(os.path.abspath('src'))
from src.add_logo import add_logo
from src.utils import list_catalog_schema_tables, create_erd_diagram, mermaid, process_llm_response_for_mermaid, quick_analysis 
from src.utils import  create_sql, load_data_from_query, process_llm_response_for_sql

# Page Config
st.set_page_config(
    page_title="SQLGenPro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# The App 
st.markdown("<h1 style='text-align: center; color: orange;'> SQLGenPro &#128640; </h1>", unsafe_allow_html=True)

st.markdown("<h6 style='text-align: center; color: white;'> Productivity Improvement tool for Product Managers, Business stakeholders and even intermediate-coders when it comes to working with data stored in a traditional SQL database. </h6>", unsafe_allow_html=True)

# Adding the logo
add_logo("artifacts/project_pro_logo_white.png")

# Adding the authentication
with open('authenticator.yml') as f:
    config = yaml.load(f, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, user_name = authenticator.login()

if authentication_status:
    authenticator.logout('Logout','main')
    st.write(f"Welcome *{name}*!")

    # Selecting the Catalog, Schema and Table in the Target Database
    st.sidebar.image('artifacts/Databricks_Logo_2.png')
    result_tables = list_catalog_schema_tables()
    df_databricks = pd.DataFrame(result_tables).iloc[:,:4]
    df_databricks.columns=["catalog","schema","table","table_type"]

    # getting catalog to schema mapping for dynamically selecting only relevant schema for a given catalog
    catalog_schema_mapping_df = df_databricks.groupby(["catalog"]).agg({'schema': lambda x: list(np.unique(x))}).reset_index()

    # getting schema to table mapping for dynamically selecting only relevant tables for a given catalog and schema
    schema_table_mapping_df = df_databricks.groupby(["schema"]).agg({'table': lambda x: list(np.unique(x))}).reset_index()

    # Selecting the catalog
    catalog = st.sidebar.selectbox("Select the catalog", options=df_databricks['catalog'].unique().tolist())

    # Selecting the schema 
    schema_candidate_list = catalog_schema_mapping_df[catalog_schema_mapping_df["catalog"]==catalog]["schema"].values[0]
    schema_candidate_list = [val for val in schema_candidate_list if val != "dev_tools"]
    schema = st.sidebar.selectbox("Select the schema", options=schema_candidate_list)

    # Selecting the Tables
    table_candidate_list = schema_table_mapping_df[schema_table_mapping_df["schema"]==schema]["table"].values[0]
    table_list = st.sidebar.multiselect("Select the table", options= ["All"]+table_candidate_list)

    if "All" in table_list:
        table_list = table_candidate_list
    
    if st.sidebar.checkbox(":orange[Proceed]"):        
        with st.expander(":red[View the ERD Diagram]"):
            response = create_erd_diagram(catalog,schema,table_list)
            if st.button("Regenerate"):
                # Creating the ERD Diagram
                create_erd_diagram.clear()
                mermaid_code = process_llm_response_for_mermaid(response)
                mermaid(mermaid_code)
            else:
                mermaid_code = process_llm_response_for_mermaid(response)
                mermaid(mermaid_code)

        
        # Quick Analysis
        st.markdown("<h2 style='text-align: left; color: orange;'> Quick Analysis </h2>", unsafe_allow_html=True)
        quick_analysis_questions = quick_analysis(user_name,mermaid_code)
        if st.button("Need new ideas?"):
            quick_analysis.clear()
            quick_analysis_questions = quick_analysis(user_name,mermaid_code)
            questions = quick_analysis_questions['text']['quick_analysis_questions']
            selected_question = st.selectbox("Select the question", options=questions)
            if st.checkbox("Analyze"):
                st.write(f"##### {selected_question}")
                # st.text(mermaid_code)
                response_sql = create_sql(selected_question,mermaid_code,catalog,schema)
                response_sql = process_llm_response_for_sql(response_sql)
                st.code(response_sql)
                if st.button("Query Sample Data"):
                    df_query = load_data_from_query(response_sql)
                    st.write(df_query)
                    

        else:
            questions = quick_analysis_questions['text']['quick_analysis_questions']
            selected_question = st.selectbox("Select the question", options=questions)
            if st.checkbox("Analyze"):
                st.write(f"##### {selected_question}")
                # st.text(mermaid_code)
                response_sql = create_sql(selected_question,mermaid_code,catalog,schema)
                response_sql = process_llm_response_for_sql(response_sql)
                st.code(response_sql)
                if st.button("Query Sample Data"):
                    df_query = load_data_from_query(response_sql)
                    st.write(df_query)
            





   
    
