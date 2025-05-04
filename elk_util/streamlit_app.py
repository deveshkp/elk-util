import streamlit as st
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd

# Initialize Elasticsearch client with SSL
es = Elasticsearch(
    ["https://your-elasticsearch-server:9200"],  # Replace with your server URL
    http_auth=('your_username', 'your_password'),  # Replace with your credentials
    scheme="https",
    port=9200,
    ssl_show_warn=False,
    verify_certs=True,
    ca_certs="/path/to/ca.crt"  # Path to your CA certificate
)

# Streamlit UI
st.title("Elasticsearch Index Growth")

# Time range selection
time_range = st.selectbox("Select Time Range", ["Last Month", "Last 6 Months", "Last Year"])
ranges = {
    "Last Month": 30,
    "Last 6 Months": 180,
    "Last Year": 365
}
days = ranges[time_range]

# Calculate date range
end_date = datetime.now()
start_date = end_date - timedelta(days=days)

# Fetch index data
indices = es.cat.indices(format="json")
index_stats = []

for index in indices:
    stats = es.indices.stats(index=index['index'])
    size_in_bytes = stats['indices'][index['index']]['total']['store']['size_in_bytes']
    index_stats.append({
        'index': index['index'],
        'size': size_in_bytes
    })

# Get top 10 largest indices
top_10 = sorted(index_stats, key=lambda x: x['size'], reverse=True)[:10]

# Convert to DataFrame for Streamlit
df = pd.DataFrame(top_10)
df['size'] = df['size'] / (1024 * 1024)  # Convert to MB

# Display DataFrame
st.write(f"Top 10 Indices from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
st.dataframe(df)

# Plotting
st.bar_chart(df.set_index('index'))
