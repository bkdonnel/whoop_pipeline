import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector

# Page config
st.set_page_config(
    page_title="Whoop Metrics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    private_key_obj = serialization.load_pem_private_key(
        st.secrets["snowflake"]["private_key"].encode(),
        password=None,
        backend=default_backend()
    )

    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        account=st.secrets["snowflake"]["account"],
        private_key=private_key_obj,
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )

@st.cache_data(ttl=300)  # Cache for 5 minutes
def query_snowflake(query):
    conn = get_snowflake_connection()
    try:
        conn.cursor().execute(f"ALTER WAREHOUSE {st.secrets['snowflake']['warehouse']} RESUME IF SUSPENDED")
    except:
        pass
    df = pd.read_sql(query, conn)
    return df

# Sidebar
st.sidebar.title("Whoop Metrics Dashboard")
st.sidebar.markdown("---")

# Date range selector
date_range = st.sidebar.selectbox(
    "Date Range",
    ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 365 Days", "All Time"],
    index=1
)

date_mapping = {
    "Last 7 Days": 7,
    "Last 30 Days": 30,
    "Last 90 Days": 90,
    "Last 365 Days": 365,
    "All Time": None
}
days_back = date_mapping[date_range]

if days_back:
    date_filter = f"WHERE d.date_day >= DATEADD(day, -{days_back}, CURRENT_DATE)"
else:
    date_filter = ""

st.sidebar.markdown("---")
st.sidebar.markdown("### Metrics Available")
st.sidebar.markdown("""
- Recovery Momentum
- Strain Recovery Efficiency
- Training Readiness Gap
- Recovery Volatility
- Autonomic Balance
- Seasonal Adaptation
""")

# Main title
st.title("Whoop Performance Metrics Dashboard")
st.markdown(f"**Data Range:** {date_range}")
st.markdown("---")

# Tab layout
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Recovery Momentum",
    "Strain-Recovery Efficiency",
    "Training Readiness",
    "Recovery Volatility",
    "Autonomic Balance",
    "Seasonal Adaptation"
])

# ==================== TAB 1: Recovery Momentum ====================
with tab1:
    st.header("Recovery Momentum Analysis")
    st.markdown("Track how your recovery score is trending over time")

    query = f"""
    SELECT
        d.date_day,
        recovery_score,
        day_1_change,
        day_3_change,
        day_7_change,
        day_14_change,
        recovery_momentum_score,
        momentum_category
    FROM DATAEXPERT_STUDENT.BRYAN.recovery_momentum rm
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON rm.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_momentum = query_snowflake(query)

    if not df_momentum.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_momentum = df_momentum['RECOVERY_MOMENTUM_SCORE'].mean()
            st.metric("Avg Momentum Score", f"{avg_momentum:.1f}")

        with col2:
            current_category = df_momentum.iloc[-1]['MOMENTUM_CATEGORY']
            st.metric("Current Trend", current_category)

        with col3:
            avg_recovery = df_momentum['RECOVERY_SCORE'].mean()
            st.metric("Avg Recovery", f"{avg_recovery:.0f}%")

        with col4:
            latest_change = df_momentum.iloc[-1]['DAY_1_CHANGE']
            st.metric("Yesterday's Change", f"{latest_change:+.0f}", delta=f"{latest_change:+.0f}")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_momentum['DATE_DAY'],
            y=df_momentum['RECOVERY_MOMENTUM_SCORE'],
            mode='lines+markers',
            name='Momentum Score',
            line=dict(color='#1f77b4', width=2),
            fill='tozeroy'
        ))
        fig.update_layout(
            title="Recovery Momentum Score Over Time",
            xaxis_title="Date",
            yaxis_title="Momentum Score",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(
            go.Scatter(x=df_momentum['DATE_DAY'], y=df_momentum['RECOVERY_SCORE'],
                      name='Recovery Score', line=dict(color='green', width=2)),
            secondary_y=False
        )
        fig2.add_trace(
            go.Bar(x=df_momentum['DATE_DAY'], y=df_momentum['DAY_1_CHANGE'],
                   name='Daily Change', marker_color='lightblue'),
            secondary_y=True
        )
        fig2.update_layout(
            title="Recovery Score with Daily Changes",
            hovermode='x unified',
            height=400
        )
        fig2.update_yaxes(title_text="Recovery Score (%)", secondary_y=False)
        fig2.update_yaxes(title_text="Daily Change", secondary_y=True)
        st.plotly_chart(fig2, use_container_width=True)

        category_counts = df_momentum['MOMENTUM_CATEGORY'].value_counts()
        fig3 = px.pie(
            values=category_counts.values,
            names=category_counts.index,
            title="Distribution of Momentum Categories",
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# ==================== TAB 2: Strain-Recovery Efficiency ====================
with tab2:
    st.header("Strain-Recovery Efficiency")
    st.markdown("How efficiently does your body recover from strain?")

    query = f"""
    SELECT
        d.date_day,
        day_strain,
        recovery_score,
        next_day_recovery,
        baseline_recovery,
        recovery_efficiency_ratio,
        efficiency_30day_avg,
        efficiency_category
    FROM DATAEXPERT_STUDENT.BRYAN.strain_recovery_efficiency sre
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON sre.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_efficiency = query_snowflake(query)

    if not df_efficiency.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            avg_efficiency = df_efficiency['RECOVERY_EFFICIENCY_RATIO'].mean()
            st.metric("Avg Efficiency Ratio", f"{avg_efficiency:.2f}")

        with col2:
            current_category = df_efficiency.iloc[-1]['EFFICIENCY_CATEGORY']
            st.metric("Current Efficiency", current_category)

        with col3:
            avg_30d = df_efficiency['EFFICIENCY_30DAY_AVG'].iloc[-1]
            st.metric("30-Day Rolling Avg", f"{avg_30d:.2f}" if avg_30d is not None else "N/A")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_efficiency['DATE_DAY'],
            y=df_efficiency['RECOVERY_EFFICIENCY_RATIO'],
            mode='markers',
            name='Daily Efficiency',
            marker=dict(size=8, opacity=0.6)
        ))
        fig.add_trace(go.Scatter(
            x=df_efficiency['DATE_DAY'],
            y=df_efficiency['EFFICIENCY_30DAY_AVG'],
            mode='lines',
            name='30-Day Average',
            line=dict(color='red', width=2)
        ))
        fig.update_layout(
            title="Recovery Efficiency Ratio Over Time",
            xaxis_title="Date",
            yaxis_title="Efficiency Ratio",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        df_efficiency['size_col'] = df_efficiency['RECOVERY_EFFICIENCY_RATIO'].abs().fillna(1)
        fig2 = px.scatter(
            df_efficiency,
            x='DAY_STRAIN',
            y='NEXT_DAY_RECOVERY',
            color='EFFICIENCY_CATEGORY',
            size='size_col',
            title="Strain vs Next Day Recovery",
            labels={'DAY_STRAIN': 'Daily Strain', 'NEXT_DAY_RECOVERY': 'Next Day Recovery (%)'},
            hover_data={'RECOVERY_EFFICIENCY_RATIO': ':.2f', 'size_col': False}
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# ==================== TAB 3: Training Readiness ====================
with tab3:
    st.header("Training Readiness Gap")
    st.markdown("Alignment between your body's readiness and training recommendations")

    query = f"""
    SELECT
        d.date_day,
        recovery_score,
        body_readiness,
        optimal_strain_target,
        training_recommendation_score,
        readiness_motivation_gap,
        gap_category,
        overtraining_risk_flag
    FROM DATAEXPERT_STUDENT.BRYAN.training_readiness_gap trg
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON trg.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_readiness = query_snowflake(query)

    if not df_readiness.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            current_gap = df_readiness.iloc[-1]['READINESS_MOTIVATION_GAP']
            st.metric("Current Gap", f"{current_gap:+.0f}" if current_gap is not None else "N/A")

        with col2:
            gap_category = df_readiness.iloc[-1]['GAP_CATEGORY']
            st.metric("Alignment Status", gap_category)

        with col3:
            avg_recovery = df_readiness['RECOVERY_SCORE'].mean()
            st.metric("Avg Recovery", f"{avg_recovery:.0f}%")

        with col4:
            risk_days = df_readiness['OVERTRAINING_RISK_FLAG'].sum()
            st.metric("Risk Days", int(risk_days), delta=None if risk_days == 0 else "Warning")

        fig = go.Figure()
        colors = df_readiness['READINESS_MOTIVATION_GAP'].apply(
            lambda x: 'red' if x is not None and x < -20 else 'orange' if x is not None and x < 0 else 'green'
        )
        fig.add_trace(go.Bar(
            x=df_readiness['DATE_DAY'],
            y=df_readiness['READINESS_MOTIVATION_GAP'],
            name='Readiness Gap',
            marker_color=colors
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(
            title="Training Readiness Gap Over Time",
            xaxis_title="Date",
            yaxis_title="Gap Score",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_readiness['DATE_DAY'],
            y=df_readiness['BODY_READINESS'],
            mode='lines',
            name='Body Readiness',
            line=dict(color='blue', width=2)
        ))
        fig2.add_trace(go.Scatter(
            x=df_readiness['DATE_DAY'],
            y=df_readiness['TRAINING_RECOMMENDATION_SCORE'],
            mode='lines',
            name='Training Recommendation',
            line=dict(color='orange', width=2)
        ))
        fig2.update_layout(
            title="Body Readiness vs Training Recommendation",
            xaxis_title="Date",
            yaxis_title="Score",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# ==================== TAB 4: Recovery Volatility ====================
with tab4:
    st.header("Recovery Volatility")
    st.markdown("How consistent is your recovery from day to day?")

    query = f"""
    SELECT
        d.date_day,
        rv.recovery_score,
        rv.volatility_7d,
        rv.volatility_30d,
        rv.volatility_coefficient,
        rv.volatility_trend,
        rv.volatility_category
    FROM DATAEXPERT_STUDENT.BRYAN.recovery_volatility rv
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON rv.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_volatility = query_snowflake(query)

    if not df_volatility.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            current_std = df_volatility['VOLATILITY_7D'].iloc[-1]
            st.metric("7-Day Volatility", f"{current_std:.1f}" if current_std is not None else "N/A")

        with col2:
            volatility_30d = df_volatility['VOLATILITY_30D'].iloc[-1]
            st.metric("30-Day Volatility", f"{volatility_30d:.1f}" if volatility_30d is not None else "N/A")

        with col3:
            category = df_volatility.iloc[-1]['VOLATILITY_CATEGORY']
            st.metric("Volatility Category", category)

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=df_volatility['DATE_DAY'], y=df_volatility['RECOVERY_SCORE'],
                      name='Recovery Score', line=dict(color='blue')),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=df_volatility['DATE_DAY'], y=df_volatility['VOLATILITY_7D'],
                      name='7-Day Volatility', line=dict(color='red', dash='dash')),
            secondary_y=True
        )
        fig.update_layout(
            title="Recovery Score and Volatility",
            hovermode='x unified',
            height=400
        )
        fig.update_yaxes(title_text="Recovery Score", secondary_y=False)
        fig.update_yaxes(title_text="Volatility", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_volatility['DATE_DAY'],
            y=df_volatility['VOLATILITY_TREND'],
            mode='lines+markers',
            name='Volatility Trend',
            line=dict(color='orange', width=2)
        ))
        fig2.update_layout(
            title="Volatility Trend Over Time",
            xaxis_title="Date",
            yaxis_title="Trend Score",
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# ==================== TAB 5: Autonomic Balance ====================
with tab5:
    st.header("Autonomic Nervous System Balance")
    st.markdown("Balance between sympathetic (stress) and parasympathetic (recovery) systems")

    query = f"""
    SELECT
        d.date_day,
        ab.hrv_rmssd,
        ab.resting_heart_rate,
        ab.hrv_rhr_ratio,
        ab.hrv_rhr_ratio_7d_avg,
        ab.autonomic_balance_score,
        ab.balance_category
    FROM DATAEXPERT_STUDENT.BRYAN.autonomic_balance ab
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON ab.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_ans = query_snowflake(query)

    if not df_ans.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            current_hrv = df_ans.iloc[-1]['HRV_RMSSD']
            st.metric("Current HRV", f"{current_hrv:.1f} ms" if current_hrv is not None else "N/A")

        with col2:
            current_rhr = df_ans.iloc[-1]['RESTING_HEART_RATE']
            st.metric("Resting HR", f"{current_rhr:.0f} bpm" if current_rhr is not None else "N/A")

        with col3:
            balance_score = df_ans.iloc[-1]['AUTONOMIC_BALANCE_SCORE']
            st.metric("Balance Score", f"{balance_score:.0f}" if balance_score is not None else "N/A")

        with col4:
            category = df_ans.iloc[-1]['BALANCE_CATEGORY']
            st.metric("Balance Category", category)

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=df_ans['DATE_DAY'], y=df_ans['HRV_RMSSD'],
                      name='HRV', line=dict(color='green', width=2)),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=df_ans['DATE_DAY'], y=df_ans['RESTING_HEART_RATE'],
                      name='Resting HR', line=dict(color='red', width=2)),
            secondary_y=True
        )
        fig.update_layout(
            title="HRV and Resting Heart Rate Trends",
            hovermode='x unified',
            height=400
        )
        fig.update_yaxes(title_text="HRV (ms)", secondary_y=False)
        fig.update_yaxes(title_text="Resting HR (bpm)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_ans['DATE_DAY'],
            y=df_ans['AUTONOMIC_BALANCE_SCORE'],
            mode='lines+markers',
            name='Autonomic Balance Score',
            line=dict(color='purple', width=2),
            fill='tozeroy'
        ))
        fig2.update_layout(
            title="Autonomic Balance Score Over Time",
            xaxis_title="Date",
            yaxis_title="Score",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# ==================== TAB 6: Seasonal Adaptation ====================
with tab6:
    st.header("Seasonal Adaptation")
    st.markdown("How environmental factors affect your recovery and performance")

    query = f"""
    SELECT
        d.date_day,
        d.month_name,
        sa.season,
        sa.recovery_score,
        sa.seasonal_recovery_baseline,
        sa.recovery_deviation,
        sa.seasonal_adaptation_index,
        sa.adaptation_category
    FROM DATAEXPERT_STUDENT.BRYAN.seasonal_adaptation sa
    JOIN DATAEXPERT_STUDENT.BRYAN.dim_date d ON sa.date_sk = d.date_sk
    {date_filter}
    ORDER BY d.date_day
    """

    df_seasonal = query_snowflake(query)

    if not df_seasonal.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            current_season = df_seasonal.iloc[-1]['SEASON']
            st.metric("Current Season", current_season)

        with col2:
            seasonal_baseline = df_seasonal.iloc[-1]['SEASONAL_RECOVERY_BASELINE']
            st.metric("Seasonal Baseline", f"{seasonal_baseline:.0f}%" if seasonal_baseline is not None else "N/A")

        with col3:
            category = df_seasonal.iloc[-1]['ADAPTATION_CATEGORY']
            st.metric("Adaptation", category)

        seasonal_summary = df_seasonal.groupby('SEASON').agg({
            'RECOVERY_SCORE': 'mean',
            'SEASONAL_ADAPTATION_INDEX': 'mean'
        }).reset_index()

        fig = px.bar(
            seasonal_summary,
            x='SEASON',
            y='RECOVERY_SCORE',
            title="Average Recovery by Season",
            labels={'RECOVERY_SCORE': 'Avg Recovery Score', 'SEASON': 'Season'},
            color='SEASON',
            color_discrete_map={
                'Winter': '#3498db',
                'Spring': '#2ecc71',
                'Summer': '#f39c12',
                'Fall': '#e67e22'
            }
        )
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_seasonal['DATE_DAY'],
            y=df_seasonal['SEASONAL_ADAPTATION_INDEX'],
            mode='lines+markers',
            name='Adaptation Index',
            line=dict(color='orange', width=2)
        ))
        fig2.update_layout(
            title="Seasonal Adaptation Index Over Time",
            xaxis_title="Date",
            yaxis_title="Adaptation Index",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)

        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=df_seasonal['DATE_DAY'],
            y=df_seasonal['RECOVERY_DEVIATION'],
            name='Deviation from Seasonal Baseline',
            marker_color=df_seasonal['RECOVERY_DEVIATION'].apply(
                lambda x: 'green' if x is not None and x > 0 else 'red'
            )
        ))
        fig3.update_layout(
            title="Recovery Deviation from Seasonal Baseline",
            xaxis_title="Date",
            yaxis_title="Deviation (%)",
            height=400
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

# Footer
st.markdown("---")
st.markdown("**Whoop Metrics Dashboard** | Data refreshed every 5 minutes | Built with Streamlit")
