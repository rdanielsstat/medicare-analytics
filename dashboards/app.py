import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pathlib import Path
import altair as alt

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(Path('.') / '.env')

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)

# -----------------------------
# Connect to Postgres
# -----------------------------
engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# -----------------------------
# Query total enrollment
# -----------------------------
query = """
SELECT year,
       month,
       tot_benes
  FROM medicare_monthly_enrollment
 WHERE bene_geo_lvl = 'National'
   AND month != 'Year'
"""

@st.cache_data
def load_data():
    df = pd.read_sql(query, engine)
    df['report_date'] = pd.to_datetime(df['year'] + '-' + df['month'] + '-01', format='%Y-%B-%d')
    df = df.sort_values('report_date').reset_index(drop=True)
    df['monthly_new_benes'] = df['tot_benes'].diff().fillna(0)
    df = df[['report_date', 'tot_benes', 'monthly_new_benes']]  # drop year/month string cols
    return df

df = load_data()

# print(df[['report_date', 'tot_benes']].to_string())

# -----------------------------
# Streamlit UI
# -----------------------------
# altair version (simpler but less hover formatting control)
st.title("U.S. Medicare Enrollment")
option = st.radio("View:", ('Cumulative Total', 'Monthly New Beneficiaries'))

if option == 'Cumulative Total':
    plot_df = df[['report_date', 'tot_benes']].copy()
    plot_df['y'] = (plot_df['tot_benes'] / 1_000_000).round(1)
    plot_df['label'] = plot_df['y'].astype(str) + 'M'

    chart = alt.Chart(plot_df, title='Total Medicare Beneficiaries').mark_line().encode(
        x=alt.X('report_date:T', title='Date', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
        y=alt.Y('y:Q', title='Beneficiaries (M)'),
        tooltip=[
            alt.Tooltip('report_date:T', title='Date', format='%b %Y'),
            alt.Tooltip('label:N', title='Total Beneficiaries'),
        ]
    ).properties(width=800, height=400)

else:
    plot_df = df[['report_date', 'monthly_new_benes']].copy()
    plot_df['y'] = (plot_df['monthly_new_benes'] / 1_000).round(1)
    plot_df['label'] = plot_df['y'].astype(str) + 'K'

    chart = alt.Chart(plot_df, title='New Medicare Beneficiaries — Monthly Change').mark_bar().encode(
        x=alt.X('report_date:T', title='Date', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
        y=alt.Y('y:Q', title='New Beneficiaries (K)'),
        tooltip=[
            alt.Tooltip('report_date:T', title='Date', format='%b %Y'),
            alt.Tooltip('label:N', title='New Beneficiaries'),
        ]
    ).properties(width=800, height=400)

st.altair_chart(chart, width='stretch')

## plotly version for better hover formatting
# st.title("U.S. Medicare Enrollment")

# option = st.radio("View:", ('Cumulative Total', 'Monthly New Beneficiaries'))

# if option == 'Cumulative Total':
#     fig = px.line(df, x='report_date', y=df['tot_benes'] / 1_000_000,
#                   labels={'report_date': 'Date', 'y': 'Beneficiaries'},
#                   title='Total Medicare Beneficiaries')
#     fig.update_traces(
#         hovertemplate='Date: %{x|%b %Y}<br>Total Beneficiaries: %{y:.1f}M<extra></extra>'
#     )
#     fig.update_yaxes(ticksuffix='M')
# else:
#     fig = px.bar(df, x='report_date', y=df['monthly_new_benes'] / 1_000,
#                  labels={'report_date': 'Date', 'y': 'New Beneficiaries'},
#                  title='New Medicare Beneficiaries — Monthly Change')
#     fig.update_traces(
#         hovertemplate='Date: %{x|%b %Y}<br>New Beneficiaries: %{y:.1f}K<extra></extra>'
#     )
#     fig.update_yaxes(ticksuffix='K')

# st.plotly_chart(fig, width='stretch')