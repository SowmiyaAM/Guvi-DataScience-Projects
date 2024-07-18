
# Import Packages

from pandas.core.arraylike import maybe_dispatch_ufunc_to_dunder_op
import streamlit as st
from streamlit_option_menu import option_menu
import easyocr
from PIL import Image
import pandas as pd
import numpy as np
import re
import io
import sqlite3

# IMAGE TO TEXT
def image_to_text(path):

  input_img = Image.open(path)

  # Converting Image into Array format
  img_arr = np.array(input_img)

  reader = easyocr.Reader(['en'])
  text = reader.readtext(img_arr, detail=0)
  return text, input_img

# TEXT TO PYTHON DICTIONARY
def ext_text (texts):
  ext_dict = {"NAME":[], "DESIGNATION":[], "COMPANY":[], "CONTACT":[], "EMAIL":[], "WEBSITE":[], "ADDRESS":[], "PINCODE":[]}

  ext_dict["NAME"]=texts[0]
  ext_dict["DESIGNATION"]=texts[1]

  for i in range(2,len(texts)):
    if texts[i].startswith("+") or ('-' in texts[i] and texts[i].replace("-","").isdigit()):
      ext_dict["CONTACT"].append(texts[i])
    elif "@" in texts[i] and ".com" in texts[i]:
      ext_dict["EMAIL"].append(texts[i])
    elif "WWW" in texts[i] or "www" in texts[i] or "Www" in texts[i] or "wWw" in texts[i] or "wwW" in texts[i]:
      small = texts[i].lower()
      ext_dict["WEBSITE"].append(small)
    elif "Tamil Nadu" in texts[i] or "TamilNadu" in texts[i] or texts[i].isdigit():
      ext_dict["PINCODE"].append(texts[i])
    elif re.match(r'^[A_Za-z]',texts[i]):
      ext_dict["COMPANY"].append(texts[i])
    else:
      remove_colon = re.sub(r'[,;]','',texts[i])
      ext_dict["ADDRESS"].append(remove_colon)
  for key, value in ext_dict.items():
    if len(value)>0:
      ext_dict[key] = ["".join(value)]
    else:
      ext_dict[key] = ["NA"]
  return ext_dict

# STREAMLIT CODE
st.set_page_config(layout="wide")
st.title("EXTRACTING BUSINESS CARD DATA WITH OCR")

with st.sidebar:
  select = option_menu("Main Menu",["HOME","UPLOAD AND MODIFY","DELETE"])

if select == "HOME":
  st.write("")
  st.write("Technologies Used : Python, easyOCR, SQLite, Streamlit, Pandas")
  st.write("Biz Card - Python application used to extract information from Business Cards")
  st.write("")
  st.image("/content/OCR_Pic.png",width=600)

elif select == "UPLOAD AND MODIFY":
  tab1, tab2, tab3 = st.tabs(["Upload","Preview","Modify"])
  with tab1:
    img = st.file_uploader("Upload the Image",type=["png","jpg","jpeg"])

    if img is not None:
      st.image(img, width=300)
      text, input_img = image_to_text(img)
      text_dict = ext_text(text)
      if text_dict:
          st.success("Text Extracted Successfully")
      text_df = pd.DataFrame(text_dict)

      # Converting Image to Bytes
      image_bytes = io.BytesIO()
      input_img.save(image_bytes,format="PNG")
      img_data = image_bytes.getvalue()

      # Creating Dictionary
      data = {"IMAGE_BYTES" : [img_data]}
      img_byte_df = pd.DataFrame(data)
      concat_df = pd.concat([text_df,img_byte_df],axis=1)

      st.dataframe(concat_df)

      save_button = st.button("Save",use_container_width = True)

      if save_button:
        mydb = sqlite3.connect("bizcardx.db")
        mycursor = mydb.cursor()
        # Table Creation
        create_table_query = '''CREATE TABLE IF NOT EXISTS bizcard_info(name varchar(255),
                                                                        designation varchar(255),
                                                                        company varchar(255),
                                                                        contact varchar(255),
                                                                        email varchar(255),
                                                                        website text,
                                                                        address text,
                                                                        pincode varchar(255),
                                                                        image text)'''

        mycursor.execute(create_table_query)
        mydb.commit()

        # Insert Query

        insert_query = '''INSERT INTO bizcard_info(name, designation, company, contact, email, website, address, pincode, image)
                          values (?,?,?,?,?,?,?,?,?)'''
        datas = concat_df.values.tolist()[0]
        mycursor.execute(insert_query,datas)
        mydb.commit()
        st.success("Saved Successfully")

  with tab2:
    mydb = sqlite3.connect("bizcardx.db")
    mycursor = mydb.cursor()
    # Select Query
    select_query = '''SELECT * FROM bizcard_info'''
    mycursor.execute(select_query)
    table = mycursor.fetchall()
    table_df = pd.DataFrame(table, columns=["NAME", "DESIGNATION", "COMPANY", "CONTACT", "EMAIL", "WEBSITE", "ADDRESS", "PINCODE", "IMAGE_BYTES"])
    st.dataframe(table_df)

  with tab3:  
    mydb = sqlite3.connect("bizcardx.db")
    mycursor = mydb.cursor()
    # Select Query
    select_query = '''SELECT * FROM bizcard_info'''
    mycursor.execute(select_query)
    table = mycursor.fetchall()
    table_df = pd.DataFrame(table, columns=["NAME", "DESIGNATION", "COMPANY", "CONTACT", "EMAIL", "WEBSITE", "ADDRESS", "PINCODE", "IMAGE_BYTES"])
   
    selected_name = st.selectbox("Select the name",table_df["NAME"])
    selected_df = table_df[table_df["NAME"]==selected_name]

    updated_df = selected_df.copy()

    col1, col2 = st.columns(2)
    with col1:
      m_name = st.text_input("Name",selected_df["NAME"].unique()[0])
      m_designation = st.text_input("Designation",selected_df["DESIGNATION"].unique()[0])
      m_company = st.text_input("Company",selected_df["COMPANY"].unique()[0])
      m_contact = st.text_input("Contact",selected_df["CONTACT"].unique()[0])
      m_email = st.text_input("Email",selected_df["EMAIL"].unique()[0])

    with col2:
      
      m_website = st.text_input("Website",selected_df["WEBSITE"].unique()[0])
      m_address = st.text_input("Address",selected_df["ADDRESS"].unique()[0])
      m_pincodes = st.text_input("Pincode",selected_df["PINCODE"].unique()[0])
      m_image_bytes = st.text_input("Image_Bytes",selected_df["IMAGE_BYTES"].unique()[0]) 

    modify_button = st.button("Modify",use_container_width = True)
    if modify_button:
      updated_df["NAME"] = m_name
      updated_df["DESIGNATION"] = m_designation
      updated_df["COMPANY"] = m_company
      updated_df["CONTACT"] = m_contact
      updated_df["EMAIL"] = m_email
      updated_df["WEBSITE"] = m_website
      updated_df["ADDRESS"] = m_address
      updated_df["PINCODE"] = m_pincodes
      updated_df["IMAGE_BYTES"] = m_image_bytes
      st.write("Modified Data")
      st.dataframe(updated_df)

    update_button = st.button("Update",use_container_width = True)
    if update_button:
      mydb = sqlite3.connect("bizcardx.db")
      mycursor = mydb.cursor()
      mycursor.execute(f"DELETE FROM bizcard_info WHERE NAME='{selected_name}'")
      mydb.commit()

      # Insert Query

      insert_query = '''INSERT INTO bizcard_info(name, designation, company, contact, email, website, address, pincode, image)
                        values (?,?,?,?,?,?,?,?,?)'''
      datas = updated_df.values.tolist()[0]
      mycursor.execute(insert_query,datas)
      mydb.commit()
      st.success("Updated Successfully")
    
elif select == "DELETE":
  mydb = sqlite3.connect("bizcardx.db")
  mycursor = mydb.cursor()
  
  col1, col2 = st.columns(2)
  # Select Query
  with col1:
    select_name_query = '''SELECT NAME FROM bizcard_info'''
    mycursor.execute(select_name_query)
    name_table = mycursor.fetchall()
    mydb.commit()
    names = []
    for i in name_table:
      names.append(i[0])
    name_select = st.selectbox("Select the Name",names)

  with col2:
    select_designation_query = f"SELECT DESIGNATION FROM bizcard_info WHERE NAME = '{name_select}'"
    mycursor.execute(select_designation_query)
    desig_table = mycursor.fetchall()
    mydb.commit()
    designations = []
    for j in desig_table:
      designations.append(j[0])
    designation_select = st.selectbox("Select the Designation",designations)

  # Delete Code
  if name_select and designation_select:
    st.write("Verify the below details to delete")
    col1, col2 = st.columns(2)

    with col1:
      st.write(f"Selected Name : {name_select}")
      st.write(f"Selected Designation : {designation_select}")
      delete_button = st.button("Delete",use_container_width=True)
      if delete_button:
        mycursor.execute(f"DELETE FROM bizcard_info WHERE NAME='{name_select}' AND DESIGNATION='{designation_select}'")
        mydb.commit()
        st.warning("Deleted")
