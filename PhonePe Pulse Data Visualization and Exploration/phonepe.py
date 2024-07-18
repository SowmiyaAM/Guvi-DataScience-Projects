import pandas as pd
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu  
import plotly.express as px 
import requests
import json

# MySQL Connection

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database='phonepe_data'
)

mycursor = mydb.cursor(buffered=True)

# DATA FETCH FROM MySQL 

# Agg_trans - data fetch and store

mycursor.execute("SELECT * FROM agg_trans")
fetch1 = mycursor.fetchall()
Agg_trans = pd.DataFrame(fetch1, columns=["States", "Years", "Quarters", "Transaction Type", "Transaction Count", "Transaction Amount"])

# Agg_users - data fetch and store

mycursor.execute("SELECT * FROM agg_users")
fetch2 = mycursor.fetchall()
Agg_users = pd.DataFrame(fetch2, columns=["States", "Years", "Quarters", "Brands", "Users", "Percentage"])

# Agg_ins - data fetch and store

mycursor.execute("SELECT * FROM agg_ins")
fetch3 = mycursor.fetchall()
Agg_ins = pd.DataFrame(fetch3, columns=["States", "Years", "Quarters", "Insurance Name", "Transaction Count", "Transaction Amount"])

# Map_trans - data fetch and store

mycursor.execute("SELECT * FROM Map_trans")
fetch4 = mycursor.fetchall()
Map_trans = pd.DataFrame(fetch4, columns=["States", "Years", "Quarters", "Districts", "Transaction Count", "Transaction Amount"])

# Map_users - data fetch and store

mycursor.execute("SELECT * FROM Map_users")
fetch5 = mycursor.fetchall()
Map_users = pd.DataFrame(fetch5, columns=["States", "Years", "Quarters", "Districts", "Users", "App Opens"])

# Map_ins - data fetch and store

mycursor.execute("SELECT * FROM Map_ins")
fetch6 = mycursor.fetchall()
Map_ins = pd.DataFrame(fetch6, columns=["States", "Years", "Quarters", "Districts", "Transaction Count", "Transaction Amount"])

# Top_trans - data fetch and store

mycursor.execute("SELECT * FROM Top_trans")
fetch7 = mycursor.fetchall()
Top_trans = pd.DataFrame(fetch7, columns=["States", "Years", "Quarters", "Pincodes", "Transaction Count", "Transaction Amount"])

# Top_users - data fetch and store

mycursor.execute("SELECT * FROM Top_users")
fetch8 = mycursor.fetchall()
Top_users = pd.DataFrame(fetch8, columns=["States", "Years", "Quarters", "Pincodes", "Users"])

# Top_ins - data fetch and store

mycursor.execute("SELECT * FROM Top_ins")
fetch9 = mycursor.fetchall()
Top_ins = pd.DataFrame(fetch9, columns=["States", "Years", "Quarters", "Pincodes", "Transaction Count", "Transaction Amount"])

# FUNCTIONS

# Filter -- Helps to filter all the required dataframes based on the year and quarter selected

def filter(df,year, quarter):
    filtered_info = df[(df["Years"]==year) & (df["Quarters"]==quarter)]
    filtered_info.reset_index(drop=True, inplace=True)
    return filtered_info

def filter_y(df,year):
    filtered_info = df[(df["Years"]==year)]
    filtered_info.reset_index(drop=True, inplace=True)
    return filtered_info

# trans_toplist 

def Trans_state_toplist(year,quarter):
    filtered_trans = filter(Agg_trans,year,quarter)
    trans_state = filtered_trans[["States","Transaction Count","Transaction Amount"]].groupby("States")[["Transaction Count","Transaction Amount"]].sum()
    trans_state.reset_index(inplace=True)
    trans_state_toplist = trans_state[["States","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    trans_state_toplist.reset_index(drop=True, inplace=True)
    return trans_state_toplist

def Trans_district_toplist(year,quarter):
    filtered_trans = filter(Map_trans,year,quarter)
    trans_district = filtered_trans[["Districts","Transaction Count","Transaction Amount"]].groupby("Districts")[["Transaction Count","Transaction Amount"]].sum()
    trans_district.reset_index(inplace=True)
    trans_district_toplist = trans_district[["Districts","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    trans_district_toplist.reset_index(drop=True, inplace=True)
    trans_district_toplist["Districts"] = trans_district_toplist["Districts"].str.title()
    trans_district_toplist["Districts"] = trans_district_toplist["Districts"].str.strip("District")
    return trans_district_toplist

def Trans_pincode_toplist(year,quarter):
    filtered_trans = filter(Top_trans,year,quarter)
    trans_pincodes = filtered_trans[["Pincodes","Transaction Count","Transaction Amount"]].groupby("Pincodes")[["Transaction Count","Transaction Amount"]].sum()
    trans_pincodes.reset_index(inplace=True)
    trans_pincode_toplist = trans_pincodes[["Pincodes","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    trans_pincode_toplist.reset_index(drop=True, inplace=True)
    trans_pincode_toplist["Pincodes"] = trans_pincode_toplist["Pincodes"].astype(int)
    return trans_pincode_toplist

# users_toplist

def Users_state_toplist(year,quarter):
    filtered_trans = filter(Map_users,year,quarter)
    users_state = filtered_trans[["States","Users"]].groupby("States")[["Users"]].sum()
    users_state.reset_index(inplace=True)
    users_state_toplist = users_state[["States","Users"]].sort_values(by="Users",ascending=False).head(10)
    users_state_toplist.reset_index(drop=True, inplace=True)
    return users_state_toplist

def Users_district_toplist(year,quarter):
    filtered_trans = filter(Map_users,year,quarter)
    users_district = filtered_trans[["Districts","Users"]].groupby("Districts")[["Users"]].sum()
    users_district.reset_index(inplace=True)
    users_district_toplist = users_district[["Districts","Users"]].sort_values(by="Users",ascending=False).head(10)
    users_district_toplist.reset_index(drop=True, inplace=True)
    users_district_toplist["Districts"] = users_district_toplist["Districts"].str.title()
    users_district_toplist["Districts"] = users_district_toplist["Districts"].str.strip("District")
    return users_district_toplist

def Users_pincode_toplist(year,quarter):
    filtered_trans = filter(Top_users,year,quarter)
    users_pincodes = filtered_trans[["Pincodes","Users"]].groupby("Pincodes")[["Users"]].sum()
    users_pincodes.reset_index(inplace=True)
    users_pincode_toplist = users_pincodes[["Pincodes","Users"]].sort_values(by="Users",ascending=False).head(10)
    users_pincode_toplist.reset_index(drop=True, inplace=True)
    users_pincode_toplist["Pincodes"] = users_pincode_toplist["Pincodes"].astype(int)
    return users_pincode_toplist

# ins_toplist

def Ins_state_toplist(year,quarter):
    filtered_trans = filter(Agg_ins,year,quarter)
    ins_state = filtered_trans[["States","Transaction Count","Transaction Amount"]].groupby("States")[["Transaction Count","Transaction Amount"]].sum()
    ins_state.reset_index(inplace=True)
    ins_state_toplist = ins_state[["States","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    ins_state_toplist.reset_index(drop=True, inplace=True)
    return ins_state_toplist

def Ins_district_toplist(year,quarter):
    filtered_trans = filter(Map_ins,year,quarter)
    ins_district = filtered_trans[["Districts","Transaction Count","Transaction Amount"]].groupby("Districts")[["Transaction Count","Transaction Amount"]].sum()
    ins_district.reset_index(inplace=True)
    ins_district_toplist = ins_district[["Districts","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    ins_district_toplist.reset_index(drop=True, inplace=True)
    ins_district_toplist["Districts"] = ins_district_toplist["Districts"].str.title()
    ins_district_toplist["Districts"] = ins_district_toplist["Districts"].str.strip("District")
    return ins_district_toplist

def Ins_pincode_toplist(year,quarter):
    filtered_trans = filter(Top_ins,year,quarter)
    ins_pincodes = filtered_trans[["Pincodes","Transaction Count","Transaction Amount"]].groupby("Pincodes")[["Transaction Count","Transaction Amount"]].sum()
    ins_pincodes.reset_index(inplace=True)
    ins_pincode_toplist = ins_pincodes[["Pincodes","Transaction Count"]].sort_values(by="Transaction Count",ascending=False).head(10)
    ins_pincode_toplist.reset_index(drop=True, inplace=True)
    ins_pincode_toplist["Pincodes"] = ins_pincode_toplist["Pincodes"].astype(int)
    return ins_pincode_toplist

# MAP DATA POINTS 

url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
response = requests.get(url)                # Json containing latitude and longitude data points of each state in India
map_data = json.loads(response.content)     # converts json string to python dictionary
# len(map_data["features"]) -- 36 
# map_data["features"][0]["geometry"]["coordinates"] -- contains the latitude and longitude data points
# map_data["features"][0]["properties"]["ST_NM"] -- contains the name of the state
states_name = []
for i in map_data["features"]:
    states_name.append(i["properties"]["ST_NM"])
states_name.sort()

# trans_plot

def trans_plot(year,quarter):
    filtered_trans = filter(Agg_trans,year,quarter)
    trans_state = filtered_trans[["States","Transaction Count","Transaction Amount"]].groupby("States")[["Transaction Count","Transaction Amount"]].sum()
    trans_state.reset_index(inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_trans_amt = px.choropleth(trans_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="Transaction Amount",
                                    color_continuous_scale="Rainbow", range_color=(trans_state["Transaction Amount"].min(),trans_state["Transaction Amount"].max()),
                                    title= f"TRANSACTION AMOUNT - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_trans_amt.update_geos(visible=False)
        st.plotly_chart(fig_trans_amt)
        fig_trans_count = px.choropleth(trans_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="Transaction Count",
                                    color_continuous_scale="Rainbow", range_color=(trans_state["Transaction Count"].min(),trans_state["Transaction Count"].max()),
                                    title= f"TRANSACTION COUNT - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_trans_count.update_geos(visible=False)
        st.plotly_chart(fig_trans_count)
    with col2:
        st.markdown(" ")
        st.header("TRANSACTIONS")
        total_trans_count = int(filtered_trans["Transaction Count"].sum())
        total_trans_amt = int(filtered_trans["Transaction Amount"].sum())
        avg_trans_amt = int(total_trans_amt/total_trans_count)
        st.write("All PhonePe Transactions :",f"{total_trans_count:,}")
        st.write("Total Payment Value :", f"Rs. {total_trans_amt:,}")
        st.write("Avg. Transaction Value :", f"Rs. {avg_trans_amt:,}")

        st.header("CATEGORIES")
        trans_info_type = filtered_trans[["Transaction Type","Transaction Count","Transaction Amount"]].groupby("Transaction Type")[["Transaction Count","Transaction Amount"]].sum()
        trans_info_type.reset_index(inplace=True)
        trans_info_type["Transaction Count"] = trans_info_type["Transaction Count"].apply(lambda x: f"{x:,}")
        trans_info_type["Transaction Amount"] = trans_info_type["Transaction Amount"].apply(lambda x: f"Rs. {x:,}")
        st.write(trans_info_type)

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            st.header("TOP 10 STATES")
            st.write(Trans_state_toplist(year,quarter))
        with tab2:
            st.header("TOP 10 DISTRICTS")
            st.write(Trans_district_toplist(year,quarter))
        with tab3:
            st.header("TOP 10 PINCODES")
            st.write(Trans_pincode_toplist(year,quarter))

# users_plot_reguser_function

def reg_users_till(year, quarter):
    reg_users = 0
    found = False
    for y in Map_users["Years"].unique().tolist():
        for q in Map_users["Quarters"].unique().tolist():
            reg_users += int(Map_users[(Map_users["Years"]==y) & (Map_users["Quarters"]==q)]["Users"].sum())
            if y==year and q==quarter:
                found = True
                break
        if found==True:
            break
    return reg_users

# users_plot

def users_plot(year,quarter):
    filtered_users = filter(Map_users,year,quarter)
    users_state = filtered_users[["States","Users","App Opens"]].groupby("States")[["Users","App Opens"]].sum()
    users_state.reset_index(inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_users_count = px.choropleth(users_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="Users",
                                    color_continuous_scale="Rainbow", range_color=(users_state["Users"].min(),users_state["Users"].max()),
                                    title= f"USERS - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_users_count.update_geos(visible=False)
        st.plotly_chart(fig_users_count)
        fig_app_opens = px.choropleth(users_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="App Opens",
                                    color_continuous_scale="Rainbow", range_color=(users_state["App Opens"].min(),users_state["App Opens"].max()),
                                    title= f"APP OPENS - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_app_opens.update_geos(visible=False)
        st.plotly_chart(fig_app_opens)
    with col2:
        st.markdown(" ")
        st.header("USERS")
        reg_users = reg_users_till(year,quarter)
        app_opens = int(filtered_users["App Opens"].sum())
        st.write(f"Registered PhonePe Users till Q{quarter} {year} :",f"{reg_users:,}")
        st.write(f"PhonePe App Opens in Q{quarter} {year} :",f"{app_opens:,}")

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            st.header("TOP 10 STATES")
            st.write(Users_state_toplist(year,quarter))
        with tab2:
            st.header("TOP 10 DISTRICTS")
            st.write(Users_district_toplist(year,quarter))
        with tab3:
            st.header("TOP 10 PINCODES")
            st.write(Users_pincode_toplist(year,quarter))

# ins_plot

def ins_plot(year,quarter):
    filtered_ins = filter(Agg_ins,year,quarter)
    ins_state = filtered_ins[["States","Transaction Count","Transaction Amount"]].groupby("States")[["Transaction Count","Transaction Amount"]].sum()
    ins_state.reset_index(inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_ins_amt = px.choropleth(ins_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="Transaction Amount",
                                    color_continuous_scale="Rainbow", range_color=(ins_state["Transaction Amount"].min(),ins_state["Transaction Amount"].max()),
                                    title= f"POLICIES PURCHASED - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_ins_amt.update_geos(visible=False)
        st.plotly_chart(fig_ins_amt)

        fig_ins_count = px.choropleth(ins_state, geojson=map_data, locations= "States", featureidkey="properties.ST_NM", color="Transaction Count",
                                    color_continuous_scale="Rainbow", range_color=(ins_state["Transaction Count"].min(),ins_state["Transaction Count"].max()),
                                    title= f"PREMIUM AMOUNT - {year} - QUARTER : {quarter}",fitbounds="locations", height=600, width=600)
        fig_ins_count.update_geos(visible=False)
        st.plotly_chart(fig_ins_count)
    with col2:
        st.markdown(" ")
        st.header("INSURANCE")
        total_policies_purchased = int(filtered_ins["Transaction Count"].sum())
        total_premium_value = int(filtered_ins["Transaction Amount"].sum())
        avg_premium_value = int(total_premium_value/total_policies_purchased)
        st.write(f"All India Insurance Policies Purchased (Nos.):",f"{total_policies_purchased:,}")
        st.write("Total Premium Value :",f"Rs. {total_premium_value:,}")
        st.write("Average Premium Value :",f"Rs. {avg_premium_value:,}")

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            st.header("TOP 10 STATES")
            st.write(Ins_state_toplist(year,quarter))
        with tab2:
            st.header("TOP 10 DISTRICTS")
            st.write(Ins_district_toplist(year,quarter))
        with tab3:
            st.header("TOP 10 PINCODES")
            st.write(Ins_pincode_toplist(year,quarter))

# Trans_Charts

def trans_charts(year,quarter,state):

    st.title(f"{state} - Q{quarter} of {year}")
    filtered_agg_trans = filter(Agg_trans,year,quarter)
    state_filtered_agg = filtered_agg_trans[filtered_agg_trans["States"]==state]

    col1, col2 = st.columns(2)
    # # hole -- 0.5 -- represents that exactly at 50% of the radius, the chart gets displayed
    with col1:
        fig_pie_amt = px.pie(data_frame=state_filtered_agg, names="Transaction Type", values="Transaction Amount", width=600, 
                            title="Transaction Amount Distribution by Type",hole=0.5)
        st.plotly_chart(fig_pie_amt)
    with col2:    
        fig_pie_count = px.pie(data_frame=state_filtered_agg, names="Transaction Type", values="Transaction Count", width=600, 
                            title="Transaction Count Distribution by Type",hole=0.5)
        st.plotly_chart(fig_pie_count)

    filtered_map_trans = filter(Map_trans,year,quarter)
    state_filtered_map = filtered_map_trans[filtered_map_trans["States"]==state]
    state_filtered_map["Districts"] = state_filtered_map["Districts"].str.title()
    state_filtered_map["Districts"] = state_filtered_map["Districts"].str.strip("District")

    col1, col2 = st.columns(2)
    with col1:
        fig_bar_amt = px.bar(state_filtered_map, x="Transaction Amount",y="Districts",title=f"{state} - Districts vs Transaction Amount",
                            width=800, color_discrete_sequence=['darkblue'])
        st.plotly_chart(fig_bar_amt)
    with col2:
        fig_bar_count = px.bar(state_filtered_map, x="Transaction Count",y="Districts",title=f"{state} - Districts vs Transaction Count",
                            width=800, color_discrete_sequence=['darkblue'])
        st.plotly_chart(fig_bar_count)

    filtered_top_trans = filter(Top_trans,year,quarter)
    state_filtered_top = filtered_top_trans[filtered_top_trans["States"]==state]
    state_filtered_top["Pincodes"] = state_filtered_top["Pincodes"].astype(int)
    fig_bar_ac = px.bar(state_filtered_top,x="States",y="Transaction Count",title=f"{state} - Transactions over different Pincodes",hover_data=["Pincodes","Transaction Amount"],color="Transaction Amount",color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_ac)

    Agg_trans_state = Agg_trans[Agg_trans["States"]==state]
    Agg_trans_syg = Agg_trans_state.groupby(["Years","Quarters"])[["Transaction Amount","Transaction Count"]].sum()
    Agg_trans_syg.reset_index(inplace=True)
    st.header(f"{state} - Transactions over the Years")
    fig_line = px.line(Agg_trans_syg,x="Years",y=["Transaction Amount","Transaction Count"],hover_data="Quarters",markers=True)
    st.plotly_chart(fig_line)

    st.header("Transactions over Different States")
    fig_pincode_bar = px.bar(filtered_top_trans,x="States",y="Transaction Count",hover_data=["Pincodes","Transaction Amount"],color="Transaction Amount")
    st.plotly_chart(fig_pincode_bar)

# Users_Charts    

def users_charts(year,quarter,state):
    st.title(f"{state} in Q{quarter} of {year}")
    filtered_agg_users = filter(Agg_users,year,quarter)
    state_filtered_agg = filtered_agg_users[filtered_agg_users["States"]==state]
    fig_bar_brand_user = px.bar(state_filtered_agg, x="Brands",y="Users",title=f"Brands vs Users",width=800)
    st.plotly_chart(fig_bar_brand_user)

    filtered_map_users = filter(Map_users,year,quarter)
    state_filtered_map = filtered_map_users[filtered_map_users["States"]==state]
    state_filtered_map["Districts"] = state_filtered_map["Districts"].str.title()
    state_filtered_map["Districts"] = state_filtered_map["Districts"].str.strip("District")
    fig_bar_user_appopens = px.bar(state_filtered_map,x=["Users","App Opens"],y="Districts",title=f"Registered Users and App Opens for different Districts of {state}",width=800,barmode="group")
    st.plotly_chart(fig_bar_user_appopens)

    filtered_top_users = filter(Top_users,year,quarter)
    state_filtered_top = filtered_top_users[filtered_top_users["States"]==state]
    state_filtered_top["Pincodes"] = state_filtered_top["Pincodes"].astype(int)
    fig_bar_user = px.bar(state_filtered_top,x="States",y="Users",title=f"{state} - Users over different Pincodes",hover_data=["Pincodes"],color="Users",color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_user)

    st.header(f"{state} - Users and App Opens over the Years")
    Map_users_state = Map_users[Map_users["States"]==state]
    Map_users_syg = Map_users_state.groupby(["Years","Quarters"])[["Users","App Opens"]].sum()
    Map_users_syg.reset_index(inplace=True)
    col1, col2 = st.columns(2)
    with col1:
        fig_line_appopens = px.line(Map_users_syg,x="Years",y="App Opens",hover_data="Quarters",markers=True)
        fig_line_appopens.update_xaxes(tickmode='linear')
        st.plotly_chart(fig_line_appopens)
    with col2:
        fig_line_users = px.line(Map_users_syg,x="Years",y="Users",hover_data="Quarters",markers=True)
        fig_line_users.update_xaxes(tickmode='linear')
        st.plotly_chart(fig_line_users)

# Ins_Charts

def ins_charts(year,quarter,state):
    st.title(f"{state} - Q{quarter} of {year}")
    filtered_map_ins = filter(Map_ins,year,quarter)
    state_filtered_ins = filtered_map_ins[filtered_map_ins["States"]==state]

    state_filtered_ins["Districts"] = state_filtered_ins["Districts"].str.title()
    state_filtered_ins["Districts"] = state_filtered_ins["Districts"].str.strip("District")

    col1, col2 = st.columns(2)
    with col1:
        fig_bar_amt = px.bar(state_filtered_ins, x="Transaction Amount",y="Districts",title="Districts vs Premium Values",
                            width=800, color_discrete_sequence=['darkblue'])
        st.plotly_chart(fig_bar_amt)
    with col2:
        fig_bar_count = px.bar(state_filtered_ins, x="Transaction Count",y="Districts",title="Districts vs Policies Purchased",
                            width=800, color_discrete_sequence=['darkblue'])
        st.plotly_chart(fig_bar_count)

    filtered_top_ins = filter(Top_ins,year,quarter)
    state_filtered_top = filtered_top_ins[filtered_top_ins["States"]==state]
    state_filtered_top["Pincodes"] = state_filtered_top["Pincodes"].astype(int)
    fig_bar_ac = px.bar(state_filtered_top,x="States",y="Transaction Count",title=f"{state} - Policies over different Pincodes",hover_data=["Pincodes","Transaction Amount"],color="Transaction Amount",color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_ac)

    Map_ins_state = Map_ins[Map_ins["States"]==state]
    Map_ins_syg = Map_ins_state.groupby(["Years","Quarters"])[["Transaction Amount","Transaction Count"]].sum()
    Map_ins_syg.reset_index(inplace=True)
    st.header(f"{state} - Policies over the Years")
    fig_line = px.line(Map_ins_syg,x="Years",y=["Transaction Amount","Transaction Count"],hover_data="Quarters",markers=True)
    st.plotly_chart(fig_line)


# STREAMLIT CODE

st.set_page_config(layout="wide")
st.title(":violet[PHONEPE DATA VISUALIZATION AND EXPLORATION]")

with st.sidebar:
    select = option_menu("Main Menu", ["HOME", "EXPLORE DATA", "TOP CHARTS"])

if select=="HOME":
    st.subheader("INDIA'S BEST TRANSACTION APP")
    st.write("To offer every Indian equal opportunity to accelerate their progress by unlocking the flow of money and access to services")
    col1, col2, col3  = st.columns(3)
    with col1:
        st.markdown("")
        st.markdown("")
        st.image(r"E:\AMS Docs\Courses\Guvi\Data Science\Projects\02_PhonePe Project\Pic.jpg",width=500)
    with col3:
        st.title("FEATURES")
        st.write("Business Solutions")
        st.write("Insurance")
        st.write("Investments")
        st.write("General")
        st.write("Legal")
        st.download_button("DOWNLOAD THE APP NOW","https://www.phonepe.com/app-download/")
    
elif select=="EXPLORE DATA":
    tab1, tab2, tab3 = st.tabs(["Transactions", "Users", "Insurance Policies"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            year = st.selectbox("Select Year",Agg_trans["Years"].unique().tolist())
            
        with col2:
            quarter_list = Agg_trans[Agg_trans["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter",quarter_list)
        
        trans_plot(year, quarter)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            year = st.selectbox("Select Year ",Agg_users["Years"].unique().tolist())
            
        with col2:
            quarter_list = Agg_users[Agg_users["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter ",quarter_list)
        
        users_plot(year, quarter)

    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            year = st.selectbox("Select Year  ",Agg_ins["Years"].unique().tolist())
            
        with col2:
            quarter_list = Agg_ins[Agg_ins["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter  ",quarter_list)
        
        ins_plot(year,quarter)

elif select=="TOP CHARTS":
    tab1, tab2, tab3 = st.tabs(["Transactions", "Users", "Insurance Policies"])
    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.selectbox("Select Year   ",Agg_trans["Years"].unique().tolist())
        with col2:
            quarter_list = Agg_trans[Agg_trans["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter   ",quarter_list)
        with col3:
            state_list = Agg_trans[(Agg_trans["Years"]==year) & (Agg_trans["Quarters"]==quarter)]["States"].unique().tolist()
            state = st.selectbox("Select State",state_list)
        trans_charts(year,quarter,state)

    with tab2:
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.selectbox("Select Year    ",Map_users["Years"].unique().tolist())
        with col2:
            quarter_list = Map_users[Map_users["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter    ",quarter_list)
        with col3:
            state_list = Map_users[(Map_users["Years"]==year) & (Map_users["Quarters"]==quarter)]["States"].unique().tolist()
            state = st.selectbox("Select State ",state_list)
        users_charts(year, quarter, state)

    with tab3:
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.selectbox("Select Year     ",Agg_ins["Years"].unique().tolist())
        with col2:
            quarter_list = Agg_ins[Agg_ins["Years"]==year]["Quarters"].unique().tolist()
            quarter = st.selectbox("Select Quarter     ",quarter_list)
        with col3:
            state_list = Agg_ins[(Agg_ins["Years"]==year) & (Agg_ins["Quarters"]==quarter)]["States"].unique().tolist()
            state = st.selectbox("Select State  ",state_list)
        ins_charts(year,quarter,state)