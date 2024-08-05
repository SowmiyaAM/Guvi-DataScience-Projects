# Import Libraries
import pandas as pd
import numpy as np
import pickle
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime

# Import Data
copper_model_data = pd.read_csv("/workspaces/dev/Industrial_Copper_Modelling/copper_final.csv")
copper_classify_data = copper_model_data[(copper_model_data["status"]=="Won") | (copper_model_data["status"]=="Lost")]
copper_regress_data = copper_model_data.copy()

# Load the Classification model from the Pickel file
with open('classification.pkl', 'rb') as file:
    rfc_model = pickle.load(file)

# Load the Regression model from the Pickel file
with open('regression.pkl', 'rb') as file:
    xgbr_model = pickle.load(file)

# Required functions defined
def item_type_input(item_type):
    item_type_IPL = 0.0
    item_type_Others = 0.0
    item_type_PL = 0.0
    item_type_S = 0.0
    item_type_SLAWR = 0.0
    item_type_W = 0.0
    item_type_WI = 0.0
    if item_type == "IPL":
        item_type_IPL = 1.0
    elif item_type == "Others":
        item_type_Others = 1.0
    elif item_type == "PL":
        item_type_PL = 1.0
    elif item_type == "S":
        item_type_S = 1.0
    elif item_type == "SLAWR":
        item_type_SLAWR = 1.0
    elif item_type == "W":
        item_type_W = 1.0
    elif item_type == "WI":
        item_type_WI = 1.0
    return item_type_IPL, item_type_Others, item_type_PL, item_type_S, item_type_SLAWR, item_type_W, item_type_WI

def item_date_input(item_date):
    item_date_day = item_date.day
    item_date_month = item_date.month
    item_date_year = item_date.year
    return item_date_day, item_date_month, item_date_year

def delivery_date_input(delivery_date):
    delivery_date_day = delivery_date.day
    delivery_date_month = delivery_date.month
    delivery_date_year = delivery_date.year
    return delivery_date_day, delivery_date_month, delivery_date_year

# Streamlit Code
st.set_page_config(layout="wide")
st.title(":red[INDUSTRIAL COPPER MODELING]")

tab1, tab2 = st.tabs(["Classification","Regression"])
with tab1:
    col1, col2 = st.columns(2)
    pred_user_output1 = [""]
    with col1:
        item_type_list = ['IPL', 'Others', 'PL', 'S', 'SLAWR', 'W', 'WI']
        item_type = st.selectbox("Item Type",item_type_list)
        i = item_type_input(item_type)                                                      # Value of encoded 7 columns calculated
        quantity_tons = st.text_input("Quantity Tons (Range : 0 to 150)",value=0)
        # Try to convert if it's not empty
        if quantity_tons:
            try:
                quantity_tons_log = np.log(int(quantity_tons)+12)                           # Log transformed value is considered as input
            except ValueError:
                st.error("Invalid input: Please enter a valid number")
        country = st.selectbox("Country",copper_classify_data["country"].unique().tolist())
        application = st.selectbox("Application",copper_classify_data["application"].unique().tolist())
        thickness = st.text_input("Thickness (Range : 0 to 7)",value=0)                                              
        # Try to convert if it's not empty
        if thickness:
            try:
                thickness_log = np.log(int(thickness)+1)                                    # Log transformed value is considered as input
            except ValueError:
                st.error("Invalid input: Please enter a valid number")
        width = st.text_input("Width (Range : 20 to 2000)",value=0)
        width = float(width)
    with col2:
        material_ref = st.selectbox("Material Reference",copper_classify_data["material_ref"].unique().tolist())
        product_ref = st.selectbox("Product Reference",copper_classify_data["product_ref"].unique().tolist())
        selling_price = st.text_input("Selling Price (Range : 200 to 1400)",value=0)
        # Try to convert if it's not empty
        if selling_price:
            try:
                selling_price_log = np.log(int(selling_price))                              # Log transformed value is considered as input
            except ValueError:
                st.error("Invalid input: Please enter a valid number")
        today = datetime.today().date()
        item_date = st.date_input("Item Date",value=today)
        id = item_date_input(item_date)                                                     # Values of splitted 3 columns calculated
        delivery_date = st.date_input("Delivery Date",value=today)
        dd = delivery_date_input(delivery_date)                                             # Values of splitted 3 columns calculated
        # Calculate date_differ
        item_date = pd.to_datetime(item_date)
        delivery_date = pd.to_datetime(delivery_date)
        date_differ = (delivery_date - item_date).days
    if st.button("Classify Status",use_container_width = True):
        user_input1 = np.array([[i[0], i[1], i[2], i[3], i[4], i[5], i[6],
                                  quantity_tons_log, country, application, thickness_log, width, material_ref, product_ref, selling_price_log, 
                                  date_differ, id[0], id[1], id[2], dd[0], dd[1], dd[2]]])
        pred_user_output1 = rfc_model.predict(user_input1)
    st.header(pred_user_output1[0])

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        item_type_list = ['IPL', 'Others', 'PL', 'S', 'SLAWR', 'W', 'WI']
        item_type = st.selectbox("Item Type ",item_type_list)
        i = item_type_input(item_type)                                                      # Value of encoded 7 columns calculated
        quantity_tons = st.text_input("Quantity Tons (Range : 0 to 150) ",value=0)
        # Try to convert if it's not empty
        if quantity_tons:
            try:
                quantity_tons_log = np.log(int(quantity_tons)+12)                           # Log transformed value is considered as input
            except ValueError:
                st.error("Invalid input: Please enter a valid number")
        country = st.selectbox("Country ",copper_regress_data["country"].unique().tolist())
        status = st.selectbox("Status ", copper_regress_data["status"].unique().tolist())
        status_map = {'Won':8,'Draft':1,"To be approved":4,"Lost":0,"Not lost for AM":6, "Wonderful":7,"Revised":2,"Offered":5,"Offerable":3}
        mapped_status = status_map.get(status)
        application = st.selectbox("Application ",copper_regress_data["application"].unique().tolist())
        thickness = st.text_input("Thickness (Range : 0 to 7) ",value=0)                                              
        # Try to convert if it's not empty
        if thickness:
            try:
                thickness_log = np.log(int(thickness)+1)                                    # Log transformed value is considered as input
            except ValueError:
                st.error("Invalid input: Please enter a valid number")

    with col2:
        width = st.text_input("Width (Range : 20 to 2000) ",value=0)
        width = float(width)
        material_ref = st.selectbox("Material Reference ",copper_classify_data["material_ref"].unique().tolist())
        product_ref = st.selectbox("Product Reference ",copper_classify_data["product_ref"].unique().tolist())
        today = datetime.today().date()
        item_date = st.date_input("Item Date ",value=today)
        id = item_date_input(item_date)                                                     # Values of splitted 3 columns calculated
        delivery_date = st.date_input("Delivery Date ",value=today)
        dd = delivery_date_input(delivery_date)                                             # Values of splitted 3 columns calculated
        # Calculate date_differ
        item_date = pd.to_datetime(item_date)
        delivery_date = pd.to_datetime(delivery_date)
        date_differ = (delivery_date - item_date).days
    if st.button("Predict Selling Price",use_container_width = True):
        user_input2 = np.array([[i[0], i[1], i[2], i[3], i[4], i[5], i[6],
                                  quantity_tons_log, country, mapped_status, application, thickness_log, width, material_ref, product_ref, 
                                  date_differ, id[0], id[1], id[2], dd[0], dd[1], dd[2]]])
        pred_user_output2 = xgbr_model.predict(user_input2)
        original_selling_price = np.exp(pred_user_output2)
        st.write("Predicted Selling Price :",original_selling_price[0])






