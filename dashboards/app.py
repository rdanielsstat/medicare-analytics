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
S3_BUCKET = os.getenv("S3_BUCKET") or st.secrets.get("S3_BUCKET", None)
S3_KEY = "exports/mart_enrollment_national/enrollment_national.csv"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or st.secrets.get("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or st.secrets.get("AWS_SECRET_ACCESS_KEY", None)

@st.cache_data
def load_data():
    if S3_BUCKET:
        import boto3
        import io
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    else:
        from sqlalchemy import create_engine
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@localhost:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB')}"
        )
        df = pd.read_sql("SELECT * FROM dbt_medicare.mart_enrollment_national", engine)

    df['report_date'] = pd.to_datetime(df['report_date'])
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