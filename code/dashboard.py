import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os
import requests

load_dotenv()

auth_code = os.getenv('auth_code')

def get_data(endpoint, params=''):
    endpoint = endpoint+'.csv'
    full_auth_code = 'Bearer {}'.format(auth_code)

    if params:
        url = 'https://api.us.airfold.co/v1/pipes/{}?inp_date={}'
        url = url.format(endpoint, str(params))
    else:
        url = 'https://api.us.airfold.co/v1/pipes/{}'
        url = url.format(endpoint)
    
    response = requests.get(url, 
                            headers={
                                'Authorization': full_auth_code
                            })

    if response.status_code == 200:
        # data = response.json()
        data = response.text
        return data
    else:
        return [response.status_code, response.text]

# Set page config
st.set_page_config(page_title="Airfold Analytics Dashboard", layout="wide")

st.title("📊 Airfold API Analytics Dashboard")

# --- Load DataFrames (assume you've already loaded them using pd.read_csv)
# df_accessibility = pd.read_csv("accessibility_metrics.csv")
# df_peak_hours = pd.read_csv("peak_hours.csv")
# df_pricing = pd.read_csv("pricing_analysis.csv")
# df_provider = pd.read_csv("provider_comparison.csv")
# df_zone_flow = pd.read_csv("zone_flows.csv")

# Replace above with actual df assignment
df_accessibility = ...
df_peak_hours = ...
df_pricing = ...
df_provider = ...
df_zone_flow = ...

# --- Section 1: Accessibility Service Performance
st.header("♿ Accessibility Service Analysis")
with st.expander("Show Accessibility Plots", expanded=True):
    fig1 = px.line(df_accessibility, x="date", y="wav_fulfillment_rate", color="pickup_borough", title="WAV Fulfillment Rate Over Time")
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = px.bar(df_accessibility, x="pickup_borough", y="avg_wav_wait_time", color="pickup_borough", title="Average WAV Wait Time by Borough")
    st.plotly_chart(fig2, use_container_width=True)

# --- Section 2: Peak Hour Analysis
st.header("⏰ Peak Hour Trip Analysis")
with st.expander("Show Peak Hour Charts", expanded=True):
    fig3 = px.bar(df_peak_hours, x="hour_of_day", y="trip_count", color="pickup_borough", barmode="group", title="Trips by Hour of Day")
    st.plotly_chart(fig3, use_container_width=True)

    fig4 = px.box(df_peak_hours, x="pickup_borough", y="avg_fare", title="Avg Fare Distribution by Borough")
    st.plotly_chart(fig4, use_container_width=True)

# --- Section 3: Pricing Analysis
st.header("💵 Pricing Metrics & Anomalies")
with st.expander("Show Pricing Insights", expanded=True):
    fig5 = px.scatter(df_pricing, x="avg_fare_per_mile", y="avg_tip_percentage", size="trip_count",
                      color="pickup_borough", hover_data=["dropoff_borough"],
                      title="Fare per Mile vs Tip % (Bubble = #Trips)")
    st.plotly_chart(fig5, use_container_width=True)

    fig6 = px.bar(df_pricing.groupby("pickup_borough").median().reset_index(), x="pickup_borough",
                  y="median_fare_per_mile", title="Median Fare per Mile by Pickup Borough")
    st.plotly_chart(fig6, use_container_width=True)

# --- Section 4: Provider Performance Comparison
st.header("🚖 Provider Performance Comparison")
with st.expander("Show Provider Metrics", expanded=True):
    fig7 = px.bar(df_provider, x="provider_name", y="avg_fare", title="Average Fare per Provider")
    st.plotly_chart(fig7, use_container_width=True)

    fig8 = px.bar(df_provider, x="provider_name", y="driver_pay_ratio", title="Driver Pay Ratio per Provider")
    st.plotly_chart(fig8, use_container_width=True)

    fig9 = px.scatter(df_provider, x="avg_trip_miles", y="avg_speed_mph", size="total_trips", color="provider_name",
                      title="Trip Miles vs Speed per Provider")
    st.plotly_chart(fig9, use_container_width=True)

# --- Section 5: Zone Flow Analysis
st.header("🌐 Zone-to-Zone Flow Insights")
with st.expander("Show Zone Flow Analysis", expanded=True):
    df_zone_flow['route'] = df_zone_flow['pickup_zone_name'] + " → " + df_zone_flow['dropoff_zone_name']
    top_routes = df_zone_flow.sort_values(by="trip_count", ascending=False).head(15)

    fig10 = px.bar(top_routes, x="route", y="trip_count", title="Top 15 Trip Corridors")
    st.plotly_chart(fig10, use_container_width=True)

    fig11 = px.scatter(top_routes, x="avg_miles", y="fare_per_mile", size="trip_count", color="pickup_borough",
                       hover_data=["pickup_zone_name", "dropoff_zone_name"],
                       title="Fare per Mile vs Avg Miles for Top Corridors")
    st.plotly_chart(fig11, use_container_width=True)
