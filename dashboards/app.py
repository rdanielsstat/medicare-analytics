import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
import io
import os
from dotenv import load_dotenv
from pathlib import Path

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(Path('.') / '.env')

# -----------------------------
# Config
# -----------------------------
S3_BUCKET = os.getenv("S3_BUCKET") or st.secrets.get("S3_BUCKET", None)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or st.secrets.get("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or st.secrets.get("AWS_SECRET_ACCESS_KEY", None)

# -----------------------------
# Data loading
# -----------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def load_csv_from_s3(key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

def load_csv_from_postgres(query: str) -> pd.DataFrame:
    from sqlalchemy import create_engine
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@localhost:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB')}"
    )
    return pd.read_sql(query, engine)

@st.cache_data
def load_national() -> pd.DataFrame:
    if S3_BUCKET:
        df = load_csv_from_s3("exports/mart_enrollment_national/enrollment_national.csv")
    else:
        df = load_csv_from_postgres("SELECT * FROM dbt_medicare.mart_enrollment_national")
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date').reset_index(drop=True)
    df['monthly_new_benes'] = df['total_beneficiaries'].diff().fillna(0)
    return df

@st.cache_data
def load_by_state() -> pd.DataFrame:
    if S3_BUCKET:
        df = load_csv_from_s3("exports/mart_enrollment_by_state/enrollment_by_state.csv")
    else:
        df = load_csv_from_postgres("SELECT * FROM dbt_medicare.mart_enrollment_by_state")
    df['year'] = df['year'].astype(int)
    df['total_beneficiaries'] = pd.to_numeric(df['total_beneficiaries'], errors='coerce')
    return df

national_df = load_national()
state_df = load_by_state()

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

tab1, tab2 = st.tabs(["National Trends", "Enrollment by State"])

with tab1:
    option = st.radio("View:", ('Cumulative Total', 'Monthly New Beneficiaries'))
    if option == 'Cumulative Total':
        fig = px.line(national_df, x='report_date', y=national_df['total_beneficiaries'] / 1_000_000,
                      labels={'report_date': 'Date', 'y': 'Beneficiaries'},
                      title='Total Medicare Beneficiaries Over Time')
        fig.update_traces(
            hovertemplate='Date: %{x|%b %Y}<br>Total Beneficiaries: %{y:.1f}M<extra></extra>'
        )
        fig.update_yaxes(ticksuffix='M')
    else:
        fig = px.bar(national_df, x='report_date', y=national_df['monthly_new_benes'] / 1_000,
                     labels={'report_date': 'Date', 'y': 'New Beneficiaries'},
                     title='New Medicare Beneficiaries — Monthly Change')
        fig.update_traces(
            hovertemplate='Date: %{x|%b %Y}<br>New Beneficiaries: %{y:.1f}K<extra></extra>'
        )
        fig.update_yaxes(ticksuffix='K')
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    years = sorted(state_df['year'].unique())
    selected_year = st.slider("Select Year", min_value=int(min(years)), max_value=int(max(years)), value=int(max(years)))
    filtered = state_df[state_df['year'] == selected_year]

    fig_map = px.choropleth(
        filtered,
        locations='state',
        locationmode='USA-states',
        color='total_beneficiaries',
        scope='usa',
        hover_name='state_name',
        hover_data={'state': False, 'total_beneficiaries': ':,.0f'},
        color_continuous_scale='Blues',
        title=f'Medicare Enrollment by State — {selected_year}',
        labels={'total_beneficiaries': 'Total Beneficiaries'}
    )
    fig_map.update_layout(geo=dict(showlakes=True, lakecolor='rgb(255,255,255)'))
    st.plotly_chart(fig_map, use_container_width=True)