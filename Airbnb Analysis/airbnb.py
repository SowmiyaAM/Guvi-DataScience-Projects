import certifi
import pymongo
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
import plotly.express as px
import nbformat
import seaborn as sns
import streamlit as st
from streamlit_option_menu import option_menu
import webbrowser

# Connection to Mongo DB 
# Database - "Airbnb_DB"
client=pymongo.MongoClient("mongodb+srv://sowmiyaamgs:1234@cluster0.hwjrmfq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",tlsCAFile=certifi.where())
db=client["Airbnb_DB"]

# Collection - "Airbnb_Info" : Fetching all documents and storing as a list - then converting into DF
record=db["Airbnb_Info"]
data = []
docs = list(record.find())
for doc in docs:
    data.append(doc)
df = pd.DataFrame(data)

# DATA CLEANING AND PREPROCESSING

df_host = pd.json_normalize(df["host"])
df_address = pd.json_normalize(df["address"])
df_availability = pd.json_normalize(df["availability"])
df_review_scores = pd.json_normalize(df["review_scores"])
df.drop(["host","address","availability","review_scores"],axis=1,inplace=True)
concat_df = pd.concat([df, df_host,df_address,df_availability,df_review_scores],axis=1)

# is_reviewed --> new column added - 0 : not reviewed & 1 : reviewed
concat_df["is_reviewed"] = np.where(concat_df["number_of_reviews"] == 0, 0,1)
# Using conditional statements directly in apply -- Categorizing "review_scores_rating" into 5 categories => new column added
concat_df['rating_category'] = concat_df['review_scores_rating'].apply(lambda x: 'Poor' if x>0 and x<=20 
                                                                        else 'Needs Improvement' if x>20 and x<=40
                                                                        else 'Meets Expectations' if x>40 and x<=60 
                                                                        else 'Exceeds Expectations' if x>60 and x<=80 
                                                                        else 'Outstanding')
# Defining a function to categorize "Availabiity" into 5 categories => new column added
def availability(row):
    if row["availability_365"]==0:
        return "Booked for a year"
    elif row["availability_90"]==0:
        return "Booked for 3 months"
    elif row["availability_60"]==0:
        return "Booked for 2 months"
    elif row["availability_30"]==0:
        return "Booked for a month"
    else:
        return "Bookings Available"
concat_df["Availability"] = concat_df.apply(lambda row: availability(row),axis=1)
# Filling Nan values as 0
concat_df["bedrooms"] = concat_df["bedrooms"].fillna(0)
concat_df["beds"] = concat_df["beds"].fillna(0)
concat_df["bathrooms"] = concat_df["bathrooms"].fillna(0)
concat_df["host_response_rate"] = concat_df["host_response_rate"].fillna(0)
concat_df["review_scores_accuracy"] = concat_df["review_scores_accuracy"].fillna(0)
concat_df["review_scores_cleanliness"] = concat_df["review_scores_cleanliness"].fillna(0)
concat_df["review_scores_checkin"] = concat_df["review_scores_checkin"].fillna(0)
concat_df["review_scores_communication"] = concat_df["review_scores_communication"].fillna(0)
concat_df["review_scores_location"] = concat_df["review_scores_location"].fillna(0)
concat_df["review_scores_value"] = concat_df["review_scores_value"].fillna(0)
concat_df["review_scores_rating"] = concat_df["review_scores_rating"].fillna(0)
concat_df["reviews_per_month"] = concat_df["reviews_per_month"].fillna(0)
# Converting data types of other required columns
concat_df["minimum_nights"] = concat_df["minimum_nights"].astype(int)
concat_df["maximum_nights"] = concat_df["maximum_nights"].astype(int)
concat_df["last_scraped"] = pd.to_datetime(concat_df["last_scraped"])
concat_df["calendar_last_scraped"] = pd.to_datetime(concat_df["calendar_last_scraped"])
concat_df["first_review"] = pd.to_datetime(concat_df["first_review"])
concat_df["last_review"] = pd.to_datetime(concat_df["last_review"])
concat_df["bedrooms"] = concat_df["bedrooms"].astype(int)
concat_df["beds"] = concat_df["beds"].astype(int)
concat_df["bathrooms"] = concat_df["bathrooms"].astype(int)
concat_df["host_response_rate"] = concat_df["host_response_rate"].astype(int)
concat_df["host_listings_count"] = concat_df["host_listings_count"].astype(int)
concat_df["host_total_listings_count"] = concat_df["host_total_listings_count"].astype(int)
concat_df["availability_30"]=concat_df["availability_30"].astype(int)
concat_df["availability_60"]=concat_df["availability_60"].astype(int)
concat_df["availability_90"]=concat_df["availability_90"].astype(int)
concat_df["availability_365"]=concat_df["availability_365"].astype(int)
concat_df["review_scores_accuracy"]=concat_df["review_scores_accuracy"].astype(int)
concat_df["review_scores_cleanliness"]=concat_df["review_scores_cleanliness"].astype(int)
concat_df["review_scores_checkin"]=concat_df["review_scores_checkin"].astype(int)
concat_df["review_scores_communication"]=concat_df["review_scores_communication"].astype(int)
concat_df["review_scores_location"]=concat_df["review_scores_location"].astype(int)
concat_df["review_scores_value"]=concat_df["review_scores_value"].astype(int)
concat_df["review_scores_rating"]=concat_df["review_scores_rating"].astype(int)
concat_df["reviews_per_month"]=concat_df["reviews_per_month"].astype(int)

# FUNCTION DEFINITIONS

def view_listed_properties(host_id):
    df = concat_df[concat_df["host_id"]==host_id]
    new_df = df[["_id","name","country","Availability","number_of_reviews","rating_category","first_review","last_review"]].reset_index(drop=True)
    st.write(new_df)

def overview():
    grouped_country_df = concat_df.groupby("country").agg(Number_of_properties_listed=('_id','count')).reset_index()
    mappings = {"Australia" : "AUS",
                "Brazil" : "BRA",
                "Canada" : "CAN",
                "China" : "CHN",
                "Hong Kong" : "HKG",
                "Portugal" : "PRT",
                "Spain" : "ESP",
                "Turkey" : "TUR",
                "United States" : "USA"}
    grouped_country_df["iso_code"] = grouped_country_df["country"].map(mappings)
    color_map = {'Australia': 'blue',
                'Brazil': 'green',
                'Canada': 'red',
                'China': 'purple',
                'Hong Kong': 'orange',
                'Portugal': 'cyan',
                'Spain': 'magenta',
                'Turkey': 'yellow',
                'United States': 'lime'}
    fig_map = px.scatter_geo(grouped_country_df, locations="iso_code", title="Count of listed Properties by Country",
                        size="Number_of_properties_listed", projection='natural earth',color='country',color_discrete_map=color_map)
    fig_map.update_geos(showcoastlines=True, coastlinecolor="Black", showland=True, landcolor="White",
                    showocean=True, oceancolor="LightBlue", showcountries=True, countrycolor="Gray")
    st.plotly_chart(fig_map)

    # Country Wise
    fig_box_country = px.box(concat_df, x='country', y='price', points=False, title='Country vs Price')

    fig_box_country.update_layout(xaxis_title='Country', yaxis_title='Price', title_x=0.5, boxmode='group')
    fig_box_country.update_yaxes(range=[10,1000])
    st.plotly_chart(fig_box_country)

    # Market Wise
    fig_box_market = px.box(concat_df, x='market', y='price', points=False, title='Market Vs Price')

    fig_box_market.update_layout(xaxis_title='Market', yaxis_title='Price', title_x=0.5, boxmode='group')
    fig_box_market.update_yaxes(range=[10,1000])
    st.plotly_chart(fig_box_market)

def filter_country(country):
        filtered_df = concat_df[(concat_df["country"]==country)]
        filtered_df.reset_index(drop=True, inplace=True)
        return filtered_df
def filter_reviewed(df):
        filtered_df = df[(df["is_reviewed"]==1)]
        filtered_df.reset_index(drop=True, inplace=True)
        return filtered_df

# Plots for reviewed and rated
def top_rated(df):
    concat_df_reviewed = filter_reviewed(df)
    custom_order = ['Poor', 'Needs Improvement', 'Meets Expectations', 'Exceeds Expectations','Outstanding']
    # Average Rating of different Bed Types
    grouped_bed_df = concat_df_reviewed.groupby(["bed_type","rating_category"]).agg(Average_Rating=('review_scores_rating','mean')).reset_index()
    grouped_bed_df["Average_Rating"] = round(grouped_bed_df["Average_Rating"],2)
    filtered_custom_order = grouped_bed_df["rating_category"].unique().tolist()
    ordered_list = sorted(filtered_custom_order, key=lambda x: custom_order.index(x))
    grouped_bed_df_sorted = grouped_bed_df.set_index('rating_category').loc[ordered_list].reset_index()
    fig_bar_bed_type = px.bar(grouped_bed_df_sorted, x="rating_category",y="Average_Rating",title="Average Rating of different Bed Types",color="bed_type",barmode="group",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_bed_type)
    # Average Rating of different Room Types
    grouped_room_df = concat_df_reviewed.groupby(["room_type","rating_category"]).agg(Average_Rating=('review_scores_rating','mean')).reset_index()
    grouped_room_df["Average_Rating"] = round(grouped_room_df["Average_Rating"],2)
    filtered_custom_order = grouped_bed_df["rating_category"].unique().tolist()
    ordered_list = sorted(filtered_custom_order, key=lambda x: custom_order.index(x))
    grouped_room_df_sorted = grouped_room_df.set_index('rating_category').loc[ordered_list].reset_index()
    fig_bar_room_type = px.bar(grouped_room_df_sorted, x="rating_category",y="Average_Rating",title="Average Rating of different Room Types",color="room_type",barmode="group",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_room_type)
    # Average Rating of Top 3 Property Types
    def select_top_n(df, n=2):
        return df.sort_values(by='Average_Rating', ascending=False).head(n)
    grouped_prop_df = concat_df_reviewed.groupby(["property_type","rating_category"]).agg(Average_Rating=('review_scores_rating','mean')).reset_index()
    grouped_prop_df["Average_Rating"] = round(grouped_prop_df["Average_Rating"],2)
    top_n_property_types = grouped_prop_df.groupby('rating_category', group_keys=False).apply(select_top_n, n=3).reset_index(drop=True)
    filtered_custom_order = grouped_bed_df["rating_category"].unique().tolist()
    ordered_list = sorted(filtered_custom_order, key=lambda x: custom_order.index(x))
    # Sort DataFrame based on custom_order
    top_n_property_types_sorted = top_n_property_types.set_index('rating_category').loc[ordered_list].reset_index()
    fig_bar_prop_type = px.bar(top_n_property_types_sorted, x="rating_category",y="Average_Rating",title="Average Rating of Top 3 Property Types",color="property_type",barmode="group",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_prop_type)

# Plots for high in demand 
def in_demand(df):
    # Preferred Bed Type
    grouped_bed_df = df.groupby(["bed_type","Availability"]).agg(Number_of_properties=('_id', 'count')).reset_index()
    custom_order = ['Booked for a year', 'Booked for 3 months', 'Booked for 2 months', 'Booked for a month','Bookings Available']
    grouped_bed_df_sorted = grouped_bed_df.set_index('Availability').loc[custom_order].reset_index()
    fig_bar_bed_type = px.bar(grouped_bed_df_sorted, x="bed_type",y="Number_of_properties",title="Preferred Bed Type",color="Availability",barmode="stack",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    fig_bar_bed_type.update_layout(xaxis_title=None)
    st.plotly_chart(fig_bar_bed_type)
    # Preferred Room Type
    grouped_room_df = df.groupby(["room_type","Availability"]).agg(Number_of_properties=('_id', 'count')).reset_index()
    custom_order = ['Booked for a year', 'Booked for 3 months', 'Booked for 2 months', 'Booked for a month','Bookings Available']
    grouped_room_df_sorted = grouped_room_df.set_index('Availability').loc[custom_order].reset_index()
    fig_sunburst_room_type = px.sunburst(grouped_room_df_sorted, path=['room_type','Availability'], values='Number_of_properties',
                    title='Preferred Room Type',color='Availability',color_discrete_sequence=px.colors.qualitative.Plotly)
    fig_sunburst_room_type.update_layout(xaxis_title=None)
    st.plotly_chart(fig_sunburst_room_type)
    # Preferred Property Type
    grouped_prop_df = df.groupby(["property_type","Availability"]).agg(Number_of_properties=('_id', 'count')).reset_index()
    custom_order = ['Booked for a year', 'Booked for 3 months', 'Booked for 2 months', 'Booked for a month','Bookings Available']
    grouped_prop_df_sorted = grouped_prop_df.set_index('Availability').loc[custom_order].reset_index()
    fig_bar_prop_type = px.bar(grouped_prop_df_sorted, x="property_type",y="Number_of_properties",title="Preferred Property Type",color="Availability",barmode="stack",
                            width=1100, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    fig_bar_prop_type.update_layout(xaxis_title=None)
    st.plotly_chart(fig_bar_prop_type)
    # Preferred Cancellation Policy
    grouped_cancelp_df = df.groupby(["cancellation_policy","Availability"]).agg(Number_of_properties=('_id', 'count')).reset_index()
    custom_order = ['Booked for a year', 'Booked for 3 months', 'Booked for 2 months', 'Booked for a month','Bookings Available']
    grouped_cancelp_df_sorted = grouped_cancelp_df.set_index('Availability').loc[custom_order].reset_index()
    fig_bar_cancelp_type = px.bar(grouped_cancelp_df_sorted, x="cancellation_policy",y="Number_of_properties",title="Preferred Cancellation Policy",color="Availability",barmode="stack",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    fig_bar_cancelp_type.update_layout(xaxis_title=None)
    st.plotly_chart(fig_bar_cancelp_type)

# Other Features Preferred 

def get_other_preferred_features():
    top_properties_df = concat_df[(concat_df["Availability"]=="Booked for a year") & (concat_df["rating_category"]=="Outstanding")].reset_index(drop=True)
    
    aminities = []
    top_aminities = []
    for i in range(len(top_properties_df)):
        for j in top_properties_df["amenities"][i]:
            aminities.append(j)
    # Count occurrences of each item in the list
    item_counts = Counter(aminities)

    # Get the top 10 most common items
    top_15_items = item_counts.most_common(15)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 15 - Most Preferred Aminities")
        # Print the top 10 items and their counts
        for item, count in top_15_items:
            st.write(f"     - {item}")
    with col2:
        st.subheader("Preferred limits set by Hosts for User Registration")
        st.write("Limit of Minimum Nights preferred by Customers : ", int(top_properties_df['minimum_nights'].mean()))
        st.write("Limit of Maximum Nights preferred by Customers : ", int(top_properties_df['maximum_nights'].mean()))
        st.subheader("Preferred Count for Other Features")
        fig_box_features = px.box(top_properties_df, y=['bedrooms','beds','bathrooms','accommodates','guests_included'],points=False)
        fig_box_features.update_yaxes(range=[0,7]) 
        st.plotly_chart(fig_box_features)

# Pricing Dynamics

def prop_price(property_type):
    filtered_df = concat_df[concat_df["property_type"]==property_type]
    grouped_df = filtered_df[["country","price"]].groupby("country")["price"].mean().reset_index()
    fig_bar_prop_type = px.bar(grouped_df, x="country",y="price",title=f"Average price by Country for {property_type}",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_prop_type)

def room_price(room_type,number_of_bedrooms):
    filtered_df = concat_df[(concat_df["room_type"]==room_type) & (concat_df["bedrooms"]==number_of_bedrooms)]
    grouped_df = filtered_df[["country","price"]].groupby("country")["price"].mean().reset_index()
    fig_bar_room_type = px.bar(grouped_df, x="country",y="price",title=f"Average price by Country for {number_of_bedrooms} {room_type}",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_room_type)

def bed_price(bed_type,number_of_beds):
    filtered_df = concat_df[(concat_df["bed_type"]==bed_type) & (concat_df["beds"]==number_of_beds)]
    grouped_df = filtered_df[["country","price"]].groupby("country")["price"].mean().reset_index()
    fig_bar_bed_type = px.bar(grouped_df, x="country",y="price",title=f"Average price by Country for {number_of_beds} {bed_type}",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_bed_type)

def acc_price(number_of_accommodates,number_of_guests):
    filtered_df = concat_df[(concat_df["accommodates"]==number_of_accommodates) & (concat_df["guests_included"]==number_of_guests)]
    grouped_df = filtered_df[["country","price"]].groupby("country")["price"].mean().reset_index()
    fig_bar_acc_type = px.bar(grouped_df, x="country",y="price",title=f"Price by Country for {number_of_accommodates} accommodates and {number_of_guests} guests",
                            width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl_r)
    st.plotly_chart(fig_bar_acc_type)

# STREAMLIT CODE

st.set_page_config(layout="wide")
st.title(":red[AIRBNB ANALYSIS]")

with st.sidebar:
    select = option_menu("Main Menu", ["HOME", "VIEW PROPERTIES", "INSIGHTS","DASHBOARD"])

if select=="HOME":
    st.subheader("Belong Anywhere")
    col1, col2 = st.columns(2)
    with col1:
        st.image(r"E:\AMS Docs\Courses\Guvi\Data Science\Projects\04_Airbnb Project\airbnb_logo.png",width=300)
    with col2:
        st.write("")
        st.write("")
        st.write("")
        st.write("From earning extra income to meeting new people, get inspired to start hosting")
        st.write("Turn your extra space into extra income where ever you are")
        st.write("Discover how hosting works and how to run your own business on Airbnb")
        st.write("Check Insights to manage multiple listings and expand your Business")
        st.write("Explore ==>>")

elif select=="VIEW PROPERTIES":
    host_id = st.text_input("Enter Host ID")
    if st.button("View"):
        st.title("Listed Properties")
        view_listed_properties(host_id)

elif select=="INSIGHTS":
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Overview","Top Rated","In Demand","Other Preferred Features","Pricing Dynamics"])
    with tab1:
        overview()

    with tab2:
        tab1, tab2 = st.tabs(["Overall","Country Wise"])
        with tab1:
            top_rated(concat_df)
        with tab2:
            country1 = st.selectbox("Select Country",concat_df["country"].unique().tolist())
            st.title(f"Top Rated Features in {country1}")
            country_filtered_df = filter_country(country1)
            top_rated(country_filtered_df)
    
    with tab3:
        tab1, tab2 = st.tabs(["Overall","Country Wise"])
        with tab1:
            in_demand(concat_df)
        with tab2:
            country2 = st.selectbox("Select Country ",concat_df["country"].unique().tolist())
            st.title(f"Preferred Features in {country2}")
            country_filtered_df = filter_country(country2)
            in_demand(country_filtered_df)

    with tab4:
        get_other_preferred_features()

    with tab5:
        select = st.selectbox("Select Variable",["Property","Room","Bed","Accommodation"])
        if select=="Property":
            property_type = st.selectbox("Property Type",concat_df["property_type"].unique().tolist())
            prop_price(property_type)
        elif select=="Room":
            col1, col2 = st.columns(2)
            with col1:
                room_type = st.selectbox("Room Type",concat_df["room_type"].unique().tolist())
            with col2:                         
                number_of_bedrooms = st.selectbox("Count of Rooms",sorted(concat_df["bedrooms"].unique().tolist()))
            room_price(room_type,number_of_bedrooms)
        elif select=="Bed":
            col1, col2 = st.columns(2)
            with col1:
                bed_type = st.selectbox("Bed Type",concat_df["bed_type"].unique().tolist())
            with col2:                         
                number_of_beds = st.selectbox("Count of Beds",sorted(concat_df["beds"].unique().tolist()))
            bed_price(bed_type,number_of_beds)
        elif select=="Accommodation":
            col1, col2 = st.columns(2)
            with col1:
                number_of_accommodates = st.selectbox("Accommodates",sorted(concat_df["accommodates"].unique().tolist()))
            with col2:                         
                number_of_guests = st.selectbox("Guests Included",sorted(concat_df["guests_included"].unique().tolist()))
            acc_price(number_of_accommodates,number_of_guests)

elif select=="DASHBOARD":
    st.write("Get a glimpse of how price, availability and ratings vary across different property types, room types and bed types in different countries")
    def main():
        # Define a function to open the URL in a new tab
        def open_url_in_new_tab(url):
            webbrowser.open_new_tab(url)

        # Create a button with a label and a callback function
        if st.button('Tableau Dashboard'):
            url = 'https://public.tableau.com/views/Airbnb_Dashboard_17212423837350/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link'
            open_url_in_new_tab(url)

    if __name__ == '__main__':
        main()




