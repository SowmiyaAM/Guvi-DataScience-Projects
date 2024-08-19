
# Streamlit Code

import numpy as np
import pickle
import json
import streamlit as st
from streamlit_option_menu import option_menu

# Load the model from the file
with open('E:/AMS Docs/Courses/Guvi/Data Science/Projects/05_ML_Projects_Final/SVM/Subscription_Classification/subscription_classifier.pkl', 'rb') as file:
    loaded_svc = pickle.load(file)

marital_list = ['married', 'single', 'divorced', 'unknown']
contact_list = ['telephone', 'cellular']
poutcome_list = ['nonexistent', 'failure', 'success']
job_list = ['housemaid', 'services', 'admin', 'blue-collar', 'technician','retired', 'management', 'unemployed', 'self-employed', 'unknown','entrepreneur', 'student']
with open("E:/AMS Docs/Courses/Guvi/Data Science/Projects/05_ML_Projects_Final/SVM/Subscription_Classification/job_map.txt", "r") as file:
    loaded_job_freq = json.load(file)
education_list = ['middle school', 'high school', 'professional course', 'unknown', 'university degree', 'illiterate']
with open("E:/AMS Docs/Courses/Guvi/Data Science/Projects/05_ML_Projects_Final/SVM/Subscription_Classification/edu_map.txt", "r") as file:
    loaded_edu_freq = json.load(file)
dic_list = ['no', 'unknown', 'yes']
dic_map = {'yes':1,'no':0,'unknown':-1}
month_list = ['mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
month_map = {'mar':3, 'apr':4,'may':5, 'jun':6, 'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
day_list = ['mon', 'tue', 'wed', 'thu', 'fri']
day_map = {'mon':2, 'tue':3, 'wed':4, 'thu':5, 'fri':6}


st.set_page_config(layout="wide")
st.title(":red[PREDICTION OF TERM DEPOSIT BY A CLIENT]")
col1, col2 = st.columns(2)
with col1:
    marital = st.selectbox("Marital",marital_list)
    marital_map = ['marital_divorced', 'marital_married', 'marital_single', 'marital_unknown']
    # Set all variables to 0
    for name in marital_map:
        globals()[name] = 0
    if marital == 'Divorced':
        marital_divorced = 1
    if marital == 'Married':
        marital_married = 1
    if marital == 'Single':
        marital_single = 1
    if marital == 'Unknown':
        marital_unknown = 1
    contact = st.selectbox("Contact",contact_list)
    contact_map = ['contact_cellular', 'contact_telephone']
    # Set all variables to 0
    for name in contact_map:
        globals()[name] = 0
    if contact == 'Cellular':
        contact_cellular = 1
    if contact == 'Telephone':
        contact_telephone = 1
    poutcome = st.selectbox("Poutcome",poutcome_list)
    poutcome_map = ['poutcome_failure', 'poutcome_nonexistent', 'poutcome_success']
    # Set all variables to 0
    for name in poutcome_map:
        globals()[name] = 0
    if poutcome == 'Failure':
        poutcome_failure = 1
    if poutcome == 'Non Existent':
        poutcome_nonexistent = 1
    if poutcome == 'Success':
        poutcome_success = 1
    age = st.text_input("Age (Range : 15 to 100)",value=0)
    job = st.selectbox("Job",job_list)
    job_enc = loaded_job_freq.get(job)
    education = st.selectbox("Education",education_list)
    edu_enc = loaded_edu_freq.get(education)
    default = st.selectbox("Default",dic_list)
    default_enc = dic_map.get(default)
    housing = st.selectbox("Housing",dic_list)
    housing_enc = dic_map.get(housing)
    loan = st.selectbox("Loan",dic_list)
    loan_enc = dic_map.get(loan)
    month = st.selectbox("Month",month_list)
    month_enc = month_map.get(month)
with col2 :
    day = st.selectbox("Day of Week",day_list)
    day_enc = day_map.get(day)
    duration = st.text_input("Duration (Range : 0 to 650)",value=0)
    campaign = st.text_input("Campaign (Range : 0 to 60)",value=0)
    pdays = st.text_input("No. of Days passed after last contact (Range : 0 to 30)",value=0)
    previous = st.text_input("No. of Contacts before this campaign (Range : 0 to 7)",value=0)
    emp_var_rate = st.text_input("Employment Variation Rate (Range : -3.5 to 1.5)",value=0)
    cons_price_idx = st.text_input("Consumer Price Index (Range : 90 to 95)",value=0)
    cons_conf_idx = st.text_input("Consumer Confidence Index (Range : -50 to -25)",value=0)
    euribor3m = st.text_input("Daily Indicator (Range : 0 to 6)",value=0)
    nr_employed = st.text_input("No. of Individuals currently employed (Range : 4900 to 5250)",value=0)

if st.button("Predict",use_container_width = True):
    user_input = np.array([[marital_divorced, marital_married, marital_single, marital_unknown, contact_cellular, contact_telephone, poutcome_failure, poutcome_nonexistent,
                            poutcome_success, age, job_enc, edu_enc, default_enc, housing_enc, loan_enc, month_enc, day_enc, duration, campaign, pdays, previous, emp_var_rate, 
                            cons_price_idx, cons_conf_idx, euribor3m, nr_employed]])
    pred_user_output = loaded_svc.predict(user_input)
    if pred_user_output == 0:
        st.write("Client will not subscribe for a Term Deposit")
    if pred_user_output == 1:
        st.write("Client will subscribe for a Term Deposit")

    

