import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os
from pathlib import Path

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(Path('.') / '.env')

# -----------------------------
# Detect environment and connect
# -----------------------------
REDSHIFT_ENDPOINT = os.getenv("REDSHIFT_ENDPOINT") or st.secrets.get("REDSHIFT_ENDPOINT", None)

@st.cache_resource
def get_engine():
    if REDSHIFT_ENDPOINT:
        import redshift_connector
        conn = redshift_connector.connect(
            host=REDSHIFT_ENDPOINT,
            database=os.getenv("REDSHIFT_DATABASE") or st.secrets["REDSHIFT_DATABASE"],
            user=os.getenv("REDSHIFT_ADMIN_USERNAME") or st.secrets["REDSHIFT_ADMIN_USERNAME"],
            password=os.getenv("REDSHIFT_ADMIN_PASSWORD") or st.secrets["REDSHIFT_ADMIN_PASSWORD"],
            port=5439,
        )
        return conn
    else:
        from sqlalchemy import create_engine
        return create_engine(
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@localhost:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB')}"
        )


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)

# -----------------------------
# Query
# -----------------------------
query = "SELECT * FROM dbt_medicare.mart_enrollment_national"

@st.cache_data
def load_data():
    conn_or_engine = get_engine()
    if REDSHIFT_ENDPOINT:
        cursor = conn_or_engine.cursor()
        cursor.execute(query)
        df = cursor.fetch_dataframe()
    else:
        df = pd.read_sql(query, conn_or_engine)
    df = df.sort_values('report_date').reset_index(drop=True)
    df['monthly_new_benes'] = df['total_beneficiaries'].diff().fillna(0)
    return df

df = load_data()

# -----------------------------
# Streamlit UI
# -----------------------------
# altair version (simpler but less hover formatting control)
# import altair as alt
# st.title("U.S. Medicare Enrollment")
# option = st.radio("View:", ('Cumulative Total', 'Monthly New Beneficiaries'))

# if option == 'Cumulative Total':
#     plot_df = df[['report_date', 'total_beneficiaries']].copy()
#     plot_df['y'] = (plot_df['total_beneficiaries'] / 1_000_000).round(1)
#     plot_df['label'] = plot_df['y'].astype(str) + 'M'

#     chart = alt.Chart(plot_df, title='Total Medicare Beneficiaries').mark_line().encode(
#         x=alt.X('report_date:T', title='Date', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
#         y=alt.Y('y:Q', title='Beneficiaries (M)'),
#         tooltip=[
#             alt.Tooltip('report_date:T', title='Date', format='%b %Y'),
#             alt.Tooltip('label:N', title='Total Beneficiaries'),
#         ]
#     ).properties(width=800, height=400)

# else:
#     plot_df = df[['report_date', 'monthly_new_benes']].copy()
#     plot_df['y'] = (plot_df['monthly_new_benes'] / 1_000).round(1)
#     plot_df['label'] = plot_df['y'].astype(str) + 'K'

#     chart = alt.Chart(plot_df, title='New Medicare Beneficiaries — Monthly Change').mark_bar(width=3).encode(
#         x=alt.X('report_date:T', title='Date', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
#         y=alt.Y('y:Q', title='New Beneficiaries (K)'),
#         tooltip=[
#             alt.Tooltip('report_date:T', title='Date', format='%b %Y'),
#             alt.Tooltip('label:N', title='New Beneficiaries'),
#         ]
#     ).properties(width=800, height=400)

# st.altair_chart(chart, width='stretch')

## plotly version for better hover formatting
st.title("U.S. Medicare Enrollment")
option = st.radio("View:", ('Cumulative Total', 'Monthly New Beneficiaries'))

if option == 'Cumulative Total':
    fig = px.line(df, x='report_date', y=df['total_beneficiaries'] / 1_000_000,
                  labels={'report_date': 'Date', 'y': 'Beneficiaries'},
                  title='Total Medicare Beneficiaries')
    fig.update_traces(
        hovertemplate='Date: %{x|%b %Y}<br>Total Beneficiaries: %{y:.1f}M<extra></extra>'
    )
    fig.update_yaxes(ticksuffix='M')
else:
    fig = px.bar(df, x='report_date', y=df['monthly_new_benes'] / 1_000,
                 labels={'report_date': 'Date', 'y': 'New Beneficiaries'},
                 title='New Medicare Beneficiaries — Monthly Change')
    fig.update_traces(
        hovertemplate='Date: %{x|%b %Y}<br>New Beneficiaries: %{y:.1f}K<extra></extra>'
    )
    fig.update_yaxes(ticksuffix='K')

st.plotly_chart(fig, width='stretch')