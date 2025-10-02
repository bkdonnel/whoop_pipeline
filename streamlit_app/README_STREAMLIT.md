# Whoop Metrics Streamlit Dashboard

A comprehensive interactive dashboard for visualizing all Whoop performance metrics from your Snowflake data warehouse.

## Features

The dashboard includes 7 interactive tabs covering all metrics:

1. **📈 Recovery Momentum** - Track recovery score trends and momentum
2. **⚡ Strain-Recovery Efficiency** - Analyze recovery efficiency per unit of strain
3. **🎯 Strain Sustainability** - Monitor training within optimal strain ranges
4. **🏃 Training Readiness Gap** - Alignment between body readiness and training recommendations
5. **📊 Recovery Volatility** - Consistency of recovery day-to-day
6. **❤️ Autonomic Balance** - HRV and resting heart rate trends
7. **🌡️ Seasonal Adaptation** - Environmental impacts on performance

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements-streamlit.txt
```

### 2. Configure Snowflake Connection

Copy the `.env.streamlit` file to `.env` and fill in your Snowflake credentials:

```bash
cp .env.streamlit .env
```

Edit `.env`:
```
SNOWFLAKE_ACCOUNT=your_account_here
SNOWFLAKE_USER=your_user_here
SNOWFLAKE_PASSWORD=your_password_here
SNOWFLAKE_WAREHOUSE=your_warehouse_here
SNOWFLAKE_ROLE=your_role_here
```

### 3. Run the Dashboard

```bash
streamlit run streamlit_dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Features

### Date Range Filtering
- Last 7 Days
- Last 30 Days (default)
- Last 90 Days
- Last 365 Days
- All Time

### Visualizations

Each metric tab includes:
- **Key Performance Indicators (KPIs)** - Top-level metrics
- **Time Series Charts** - Track trends over time
- **Distribution Charts** - Understand patterns
- **Comparative Analysis** - Multi-metric comparisons

### Data Refresh

- Data is cached for 5 minutes for performance
- Automatic refresh on date range changes
- Direct queries to Snowflake `whoop.marts.*` tables

## Metrics Overview

### Recovery Momentum
- Momentum score trends
- Day-over-day changes (1, 3, 7, 14 days)
- Category distribution (Accelerating, Improving, Stable, Declining, Deteriorating)

### Strain-Recovery Efficiency
- Recovery efficiency ratio
- 30-day rolling average
- Strain vs recovery scatter plots
- Efficiency categories

### Strain Sustainability
- Sustainability score (0-100)
- Days in optimal strain range
- Strain deviation from optimal
- Category breakdown (Highly Sustainable, Sustainable, Moderately Sustainable, Unsustainable)

### Training Readiness Gap
- Readiness-motivation gap score
- Body readiness vs training recommendations
- Overtraining risk flags
- Gap categories

### Recovery Volatility
- 7-day and 30-day standard deviation
- Day-over-day changes
- Coefficient of variation
- Consistency categories

### Autonomic Balance
- HRV (Heart Rate Variability) trends
- Resting heart rate trends
- Parasympathetic dominance score
- ANS balance categories

### Seasonal Adaptation
- Recovery by season
- Skin temperature trends
- Seasonal deviations
- Adaptation categories

## Troubleshooting

### Connection Issues

If you get Snowflake connection errors:

1. Verify credentials in `.env` file
2. Check that your Snowflake user has access to `WHOOP.MARTS` schema
3. Ensure the warehouse is running

### Missing Data

If charts show no data:

1. Verify metrics tables exist: `SELECT * FROM whoop.marts.recovery_momentum LIMIT 1`
2. Check that the DAG `whoop_pipeline_cosmos_simple` has run successfully
3. Adjust date range filter

### Performance Issues

If the dashboard is slow:

1. Reduce the date range (use "Last 7 Days" or "Last 30 Days")
2. Check Snowflake warehouse size
3. Increase cache TTL in the code (line 44: `@st.cache_data(ttl=300)`)

## Customization

### Adding New Metrics

To add a new metric from `whoop.marts.*`:

1. Add a new tab in line 142
2. Create a query against the new metrics table
3. Add visualizations using Plotly

### Changing Colors

Update the color schemes in Plotly chart configurations:
- Line colors: `line=dict(color='blue')`
- Bar colors: `marker_color='green'`
- Color maps: `color_discrete_map={...}`

### Adjusting Cache Duration

Change the cache TTL in line 44:
```python
@st.cache_data(ttl=600)  # Cache for 10 minutes instead of 5
```

## Tech Stack

- **Streamlit** - Dashboard framework
- **Plotly** - Interactive visualizations
- **Pandas** - Data manipulation
- **Snowflake Connector** - Database connection

## License

MIT
