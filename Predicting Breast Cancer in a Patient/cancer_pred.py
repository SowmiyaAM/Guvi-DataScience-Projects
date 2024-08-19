import numpy as np
import pickle
import streamlit as st
from streamlit_option_menu import option_menu

# Load the model from the file
with open('E:/AMS Docs/Courses/Guvi/Data Science/Projects/05_ML_Projects_Final/Ensemble_RF/Breast Cancer Prediction/cancer_classifier.pkl', 'rb') as file:
    loaded_rfc = pickle.load(file)

# Streamlit Code
st.set_page_config(layout="wide")
st.title(":red[PREDICTING BREAST CANCER IN A PATIENT]")

col1, col2, col3 = st.columns(3)
with col1:
    radius_mean = st.text_input("Radius Mean (Range : 6 to 30)",value=0)
    texture_mean = st.text_input("Texture Mean (Range : 9 to 40)",value=0)
    perimeter_mean = st.text_input("Perimeter Mean (Range : 43 to 189)",value=0)
    area_mean = st.text_input("Area Mean (Range : 143 to 2500)",value=0)
    smoothness_mean = st.text_input("Smoothness Mean (Range : 0.05 to 0.16)",value=0)
    compactness_mean = st.text_input("Compactness Mean (Range : 0.019 to 0.345)",value=0)
    concavity_mean = st.text_input("Concavity Mean (Range : 0 to 0.5)",value=0)
    concave_points_mean = st.text_input("Concave Points Mean (Range : 0 to 0.25)",value=0)
    symmetry_mean = st.text_input("Symmetry Mean (Range : 0.1 to 0.35)",value=0)
    fractal_dimension_mean = st.text_input("Fractal Dimension Mean (Range : 0.04 to 0.09)",value=0)
with col2:
    radius_se = st.text_input("Radius Se (Range : 0.1 to 3.0)",value=0)
    texture_se = st.text_input("Texture Se (Range : 0.3 to 5.0)",value=0)
    perimeter_se = st.text_input("Perimeter Se (Range : 0.7 to 22)",value=0)
    area_se = st.text_input("Area Se (Range : 6 to 543)",value=0)
    smoothness_se = st.text_input("Smoothness Se (Range : 0.001 to 0.03)",value=0)
    compactness_se = st.text_input("Compactness Se (Range : 0.002 to 0.14)",value=0)
    concavity_se = st.text_input("Concavity Se (Range : 0 to 0.4)",value=0)
    concave_points_se = st.text_input("Concave Points Se (Range : 0 to 0.055)",value=0)
    symmetry_se = st.text_input("Symmetry Se (Range : 0.007 to 0.079)",value=0)
    fractal_dimension_se = st.text_input("Fractal Dimension Se (Range : 0 to 0.03)",value=0)
with col3:
    radius_worst = st.text_input("Radius Worst (Range : 7 to 37)",value=0)
    texture_worst = st.text_input("Texture Worst (Range : 12 to 50)",value=0)
    perimeter_worst = st.text_input("Perimeter Worst (Range : 50 to 250)",value=0)
    area_worst = st.text_input("Area Worst (Range : 185 to 4255)",value=0)
    smoothness_worst = st.text_input("Smoothness Worst (Range : 0.07 to 0.22)",value=0)
    compactness_worst = st.text_input("Compactness Worst (Range : 0.02 to 1.05)",value=0)
    concavity_worst = st.text_input("Concavity Worst (Range : 0 to 1.3)",value=0)
    concave_points_worst = st.text_input("Concave Points Worst (Range : 0 to 0.3)",value=0)
    symmetry_worst = st.text_input("Symmetry Worst (Range : 0.1 to 0.7)",value=0)
    fractal_dimension_worst = st.text_input("Fractal Dimension Worst (Range : 0.05 to 0.2)",value=0)

if st.button("Predict",use_container_width = True):
    user_input = np.array([[radius_mean, texture_mean, perimeter_mean, area_mean,smoothness_mean, compactness_mean, concavity_mean, concave_points_mean, 
                            symmetry_mean, fractal_dimension_mean, radius_se, texture_se, perimeter_se, area_se, smoothness_se, compactness_se, concavity_se, 
                            concave_points_se, symmetry_se, fractal_dimension_se, radius_worst, texture_worst, perimeter_worst, area_worst, smoothness_worst,compactness_worst,
                            concavity_worst, concave_points_worst, symmetry_worst, fractal_dimension_worst]])
    pred_user_output = loaded_rfc.predict(user_input)
    if pred_user_output == "M":
        st.write("Cancer result is Positive")
    if pred_user_output == "B":
        st.write("Cancer result is Negative")


