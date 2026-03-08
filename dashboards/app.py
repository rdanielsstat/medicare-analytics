import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
import io
import os
from dotenv import load_dotenv
from pathlib import Path

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Medicare Analytics",
    page_icon=None,
    layout="wide",
)

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(Path('.') / '.env')

S3_BUCKET = os.getenv("S3_BUCKET") or st.secrets.get("S3_BUCKET", None)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or st.secrets.get("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or st.secrets.get("AWS_SECRET_ACCESS_KEY", None)


# -----------------------------
# Data loading
# -----------------------------
def load_csv_from_s3(key: str) -> pd.DataFrame:
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
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
    df['total_beneficiaries'] = pd.to_numeric(df['total_beneficiaries'], errors='coerce')
    df['medicare_advantage_benes'] = pd.to_numeric(df['medicare_advantage_benes'], errors='coerce')
    df['original_medicare_benes'] = pd.to_numeric(df['original_medicare_benes'], errors='coerce')
    df['ma_penetration_rate'] = pd.to_numeric(df['ma_penetration_rate'], errors='coerce')
    df['ffs_penetration_rate'] = pd.to_numeric(df['ffs_penetration_rate'], errors='coerce')
    df = df.sort_values('report_date').reset_index(drop=True)
    return df


@st.cache_data
def load_by_state() -> pd.DataFrame:
    if S3_BUCKET:
        df = load_csv_from_s3("exports/mart_enrollment_by_state/enrollment_by_state.csv")
    else:
        df = load_csv_from_postgres("SELECT * FROM dbt_medicare.mart_enrollment_by_state")
    df['year'] = df['year'].astype(int)
    df['total_beneficiaries'] = pd.to_numeric(df['total_beneficiaries'], errors='coerce')
    df['medicare_advantage_benes'] = pd.to_numeric(df['medicare_advantage_benes'], errors='coerce')
    df['original_medicare_benes'] = pd.to_numeric(df['original_medicare_benes'], errors='coerce')
    df['ma_penetration_rate'] = pd.to_numeric(df['ma_penetration_rate'], errors='coerce')
    df['ffs_penetration_rate'] = pd.to_numeric(df['ffs_penetration_rate'], errors='coerce')
    return df


national_df = load_national()
state_df = load_by_state()

# -----------------------------
# Derived values for KPI cards
# -----------------------------
latest = national_df.iloc[-1]
previous = national_df.iloc[-2]

latest_total = latest['total_beneficiaries']
latest_ma = latest['medicare_advantage_benes']
latest_ffs = latest['original_medicare_benes']
latest_ma_pct = latest['ma_penetration_rate']
latest_ffs_pct = latest['ffs_penetration_rate']
latest_date = latest['report_date'].strftime('%B %Y')

mom_total = latest_total - previous['total_beneficiaries']
mom_ma = latest_ma - previous['medicare_advantage_benes']
mom_ffs = latest_ffs - previous['original_medicare_benes']

# -----------------------------
# Header
# -----------------------------
st.title("U.S. Medicare Enrollment")
st.caption(f"Data through {latest_date} | Source: Centers for Medicare & Medicaid Services")

st.divider()

# -----------------------------
# Row 1: Donut + KPI cards
# -----------------------------
col_donut, col_kpi = st.columns([1, 2])

with col_donut:
    donut = go.Figure(go.Pie(
        values=[latest_ma, latest_ffs],
        labels=['Medicare Advantage', 'Fee-for-Service'],
        hole=0.65,
        marker_colors=['#1f77b4', '#aec7e8'],
        textinfo='none',
        hovertemplate='%{label}<br>%{value:,.0f} beneficiaries<br>%{percent}<extra></extra>',
    ))
    donut.update_layout(
        annotations=[dict(
            text=f"<b>{latest_total / 1_000_000:.1f}M</b><br>Beneficiaries",
            x=0.5, y=0.5, font_size=18, showarrow=False, align='center'
        )],
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5),
        margin=dict(t=20, b=40, l=20, r=20),
        height=300,
    )
    st.plotly_chart(donut, use_container_width=True)

with col_kpi:
    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric(
            label="Total Beneficiaries",
            value=f"{latest_total / 1_000_000:.2f}M",
            delta=f"{mom_total / 1_000:+.0f}K vs prior month"
        )
    with k2:
        st.metric(
            label="Medicare Advantage",
            value=f"{latest_ma / 1_000_000:.2f}M ({latest_ma_pct:.1f}%)",
            delta=f"{mom_ma / 1_000:+.0f}K vs prior month"
        )
    with k3:
        st.metric(
            label="Fee-for-Service",
            value=f"{latest_ffs / 1_000_000:.2f}M ({latest_ffs_pct:.1f}%)",
            delta=f"{mom_ffs / 1_000:+.0f}K vs prior month",
            delta_color="inverse"
        )

st.caption("Note: Fee-for-Service trends are shown inverse to Medicare Advantage. Declining FFS enrollment reflects the ongoing policy shift toward managed care.")

st.divider()

# -----------------------------
# Row 2: Map + toggle table
# -----------------------------
years = sorted(state_df['year'].unique())
selected_year = st.slider(
    "Select Year",
    min_value=int(min(years)),
    max_value=int(max(years)),
    value=int(max(years))
)
filtered = state_df[state_df['year'] == selected_year].copy()

view = st.radio("View as:", ["Map", "Table"], horizontal=True)

if view == "Map":
    fig_map = px.choropleth(
        filtered,
        locations='state',
        locationmode='USA-states',
        color='ma_penetration_rate',
        scope='usa',
        hover_name='state_name',
        hover_data={
            'state': False,
            'ma_penetration_rate': ':.1f',
            'ffs_penetration_rate': ':.1f',
            'total_beneficiaries': ':,.0f',
            'medicare_advantage_benes': ':,.0f',
            'original_medicare_benes': ':,.0f',
        },
        color_continuous_scale='Blues',
        range_color=[0, 100],
        title=f'Medicare Advantage Penetration Rate by State — {selected_year}',
        labels={
            'ma_penetration_rate': 'MA Penetration %',
            'ffs_penetration_rate': 'FFS Penetration %',
            'total_beneficiaries': 'Total Beneficiaries',
            'medicare_advantage_benes': 'MA Beneficiaries',
            'original_medicare_benes': 'FFS Beneficiaries',
        }
    )
    fig_map.update_layout(
        coloraxis_colorbar=dict(title='MA %', ticksuffix='%'),
        geo=dict(showlakes=False, lakecolor='rgb(255,255,255)'),
        margin=dict(t=50, b=0, l=0, r=0),
        height=500,
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    table_df = filtered[[
        'state_name', 'state', 'total_beneficiaries',
        'original_medicare_benes', 'medicare_advantage_benes',
        'ffs_penetration_rate', 'ma_penetration_rate'
    ]].copy()
    table_df.columns = [
        'State', 'Abbr', 'Total', 'FFS', 'MA', 'FFS %', 'MA %'
    ]
    table_df = table_df.sort_values('Total', ascending=False).reset_index(drop=True)
    table_df['Total'] = table_df['Total'].apply(lambda x: f"{x:,.0f}")
    table_df['FFS'] = table_df['FFS'].apply(lambda x: f"{x:,.0f}")
    table_df['MA'] = table_df['MA'].apply(lambda x: f"{x:,.0f}")
    table_df['FFS %'] = table_df['FFS %'].apply(lambda x: f"{x:.1f}%")
    table_df['MA %'] = table_df['MA %'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(table_df, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Row 3: Trend charts
# -----------------------------
col_line, col_area = st.columns(2)

with col_line:
    annual_df = national_df[national_df['month'] == 'January'].copy()

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=annual_df['report_date'],
        y=annual_df['total_beneficiaries'] / 1_000_000,
        name='Total',
        line=dict(color='#2c2c2c', width=2),
        hovertemplate='%{x|%Y}<br>Total: %{y:.2f}M<extra></extra>',
    ))
    fig_line.add_trace(go.Scatter(
        x=annual_df['report_date'],
        y=annual_df['medicare_advantage_benes'] / 1_000_000,
        name='Medicare Advantage',
        line=dict(color='#1f77b4', width=2),
        hovertemplate='%{x|%Y}<br>MA: %{y:.2f}M<extra></extra>',
    ))
    fig_line.add_trace(go.Scatter(
        x=annual_df['report_date'],
        y=annual_df['original_medicare_benes'] / 1_000_000,
        name='Fee-for-Service',
        line=dict(color='#aec7e8', width=2),
        hovertemplate='%{x|%Y}<br>FFS: %{y:.2f}M<extra></extra>',
    ))
    fig_line.update_layout(
        title='Enrollment Count Yearly Trend',
        xaxis_title=None,
        yaxis_title='Beneficiaries (Millions)',
        yaxis_ticksuffix='M',
        legend=dict(orientation='h', yanchor='bottom', y=-0.3, xanchor='center', x=0.5),
        margin=dict(t=50, b=60, l=60, r=20),
        height=400,
    )
    st.plotly_chart(fig_line, use_container_width=True)

with col_area:
    fig_area = go.Figure()
    fig_area.add_trace(go.Scatter(
        x=annual_df['report_date'],
        y=annual_df['ffs_penetration_rate'],
        name='Fee-for-Service',
        fill='tozeroy',
        stackgroup='one',
        line=dict(color='#aec7e8'),
        hovertemplate='%{x|%Y}<br>FFS: %{y:.1f}%<extra></extra>',
    ))
    fig_area.add_trace(go.Scatter(
        x=annual_df['report_date'],
        y=annual_df['ma_penetration_rate'],
        name='Medicare Advantage',
        fill='tonexty',
        stackgroup='one',
        line=dict(color='#1f77b4'),
        hovertemplate='%{x|%Y}<br>MA: %{y:.1f}%<extra></extra>',
    ))
    fig_area.update_layout(
        title='Share of Total Enrollment Yearly Trend',
        xaxis_title=None,
        yaxis_title='Share of Total (%)',
        yaxis_ticksuffix='%',
        yaxis_range=[0, 100],
        legend=dict(orientation='h', yanchor='bottom', y=-0.3, xanchor='center', x=0.5),
        margin=dict(t=50, b=60, l=60, r=20),
        height=400,
    )
    st.plotly_chart(fig_area, use_container_width=True)
    