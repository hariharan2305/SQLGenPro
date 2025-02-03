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
from src.utils import  create_sql, load_data_from_query, process_llm_response_for_sql, create_advanced_sql, get_enriched_database_schema
from src.utils import validate_and_correct_sql, add_to_user_history

# Page Config
st.set_page_config(
    page_title="SQLGenPro",
    page_icon="📊",
    layout="centered",
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
                response = create_erd_diagram(catalog,schema,table_list)
                mermaid_code = process_llm_response_for_mermaid(response)
                mermaid(mermaid_code)
            else:
                mermaid_code = process_llm_response_for_mermaid(response)
                mermaid(mermaid_code)
            
            # Getting the table schema
            table_schema = get_enriched_database_schema(catalog,schema,table_list)

        
        # Quick Analysis
        st.markdown("<h2 style='text-align: left; color: red;'> Quick Analysis </h2>", unsafe_allow_html=True)
        with st.expander(":red[View the Section]"):
            quick_analysis_questions = quick_analysis(user_name,mermaid_code)
            if st.button("Need new ideas?"):
                quick_analysis.clear()
                quick_analysis_questions = quick_analysis(user_name,table_schema)
                questions = quick_analysis_questions['text']['quick_analysis_questions']
                selected_question = st.selectbox("Select the question", options=questions)
                if st.checkbox("Analyze"):
                    st.write(f"##### {selected_question}")
                    # st.text(mermaid_code)
                    response_sql_qa = create_sql(selected_question,table_schema)
                    response_sql_qa = process_llm_response_for_sql(response_sql_qa)
                    
                    # Self-correction loop
                    flag, response_sql_qa = validate_and_correct_sql(response_sql_qa,table_schema)
                    while flag != 'Correct':
                        flag, response_sql_qa = validate_and_correct_sql(response_sql_qa,table_schema)

                    st.code(response_sql_qa)
                    col1,col2 = st.columns(2)
                    if col1.button("Query Sample Data"):
                        df_query = load_data_from_query(response_sql_qa)                        
                        col1.write(df_query)
                    
                    # Saving the Favourites    
                    # Adding session_state for favourite button
                    if 'fav_ind_qa' not in st.session_state:
                        st.session_state.fav_ind_qa = False

                    fav_ind_qa = col2.button("Save the query",key="ddd34-13")
                    if fav_ind_qa:
                        st.session_state.fav_ind_qa = True
                        add_to_user_history(name,selected_question,response_sql_qa,favourite_ind=True)
                        col2.write("Added to favourites!") 
                        

            else:
                questions = quick_analysis_questions['text']['quick_analysis_questions']
                selected_question = st.selectbox("Select the question", options=questions)
                if st.checkbox("Analyze"):
                    st.write(f"##### {selected_question}")
                    # st.text(mermaid_code)
                    response_sql_qa = create_sql(selected_question,table_schema)
                    response_sql_qa = process_llm_response_for_sql(response_sql_qa)

                    # Self-correction loop
                    flag, response_sql_qa = validate_and_correct_sql(response_sql_qa,table_schema)
                    while flag != 'Correct':
                        flag, response_sql_qa = validate_and_correct_sql(response_sql_qa,table_schema)

                    st.code(response_sql_qa)
                    col1,col2 = st.columns(2)
                    if col1.button("Query Sample Data"):
                        df_query = load_data_from_query(response_sql_qa)
                        col1.write(df_query)

                    # Saving the Favourites    
                    # Adding session_state for favourite button
                    if 'fav_ind_qa_2' not in st.session_state:
                        st.session_state.fav_ind_qa_2 = False

                    fav_ind_qa_2 = col2.button("Save the query",key="dd34-13")
                    if fav_ind_qa_2:
                        st.session_state.fav_ind_qa_2 = True
                        add_to_user_history(name,selected_question,response_sql_qa,favourite_ind=True)
                        col2.write("Added to favourites!")  


        # Deep-Dive Analysis
        st.markdown("<h2 style='text-align: left; color: red;'> Deep-Dive Analysis </h2>", unsafe_allow_html=True)

        with st.expander(":red[View the Section]"):
            dd_question = st.text_area("Enter your question here..",key="dd-10",)

            generate_sql_1 = st.checkbox("Generate SQL",key="dd-11")
            if generate_sql_1:
                response_sql_1 = create_sql(dd_question,table_schema)
                response_sql_1 = process_llm_response_for_sql(response_sql_1)

                # Self-correction loop
                flag, response_sql_1 = validate_and_correct_sql(dd_question,response_sql_1,table_schema)
                while flag != 'Correct':
                    flag, response_sql_1 = validate_and_correct_sql(dd_question,response_sql_1,table_schema)

                st.code(response_sql_1)

                col1, col2 = st.columns(2)                

                query_sample_data_1 = col1.checkbox("Query Sample Data",key="dd-12")
                if query_sample_data_1:
                    df_query_1 = load_data_from_query(response_sql_1)
                    col1.write(df_query_1)

                # Saving the Favourites    
                # Adding session_state for favourite button
                if 'fav_ind_1' not in st.session_state:
                    st.session_state.fav_ind_1 = False

                fav_ind_1 = col2.button("Save the query",key="dd-13")
                if fav_ind_1:
                    st.session_state.fav_ind_1 = True
                    add_to_user_history(name,dd_question,response_sql_1,favourite_ind=True)
                    col2.write("Added to favourites!")


                build_1 = col1.checkbox("Build on top of this result?",key="dd-21")
                if build_1:
                    dd_question_2 = st.text_area("Enter your question here..",key="dd-23",)

                    generate_sql_2 = st.checkbox("Generate SQL",key="dd-24")
                    if generate_sql_2:
                        response_sql_2 = create_advanced_sql(dd_question_2,response_sql_1,table_schema)
                        response_sql_2 = process_llm_response_for_sql(response_sql_2)

                        # Self-correction loop
                        flag, response_sql_2 = validate_and_correct_sql(dd_question_2,response_sql_2,table_schema)
                        while flag != 'Correct':
                            flag, response_sql_2 = validate_and_correct_sql(dd_question_2,response_sql_2,table_schema)

                        st.code(response_sql_2)

                        col1, col2 = st.columns(2)
                        query_sample_data_2 = col1.checkbox("Query Sample Data",key="dd-25")
                        if query_sample_data_2:
                            df_query_2 = load_data_from_query(response_sql_2)
                            col1.write(df_query_2)
                                                
                        
                        # Saving the Favourites    
                        # Adding session_state for favourite button
                        if 'fav_ind_2' not in st.session_state:
                            st.session_state.fav_ind_2 = False

                        fav_ind_2 = col2.button("Save the query",key="dd3-13")
                        if fav_ind_2:
                            st.session_state.fav_ind_2 = True
                            add_to_user_history(name,dd_question_2,response_sql_2,favourite_ind=True)
                            col2.write("Added to favourites!")  
                            


