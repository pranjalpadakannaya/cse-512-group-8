"""
Analytics Dashboard Page
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.workload.db_connection import CockroachDBConnection

st.set_page_config(page_title="Analytics", page_icon="📈", layout="wide")

st.title("📈 Analytics Dashboard")

# Initialize DB
if 'db' not in st.session_state:
    st.session_state.db = CockroachDBConnection()

# Revenue by Region
st.header("Revenue by Region")

query = """
SELECT r.R_NAME as region, 
       COUNT(DISTINCT o.O_ORDERKEY) as num_orders,
       ROUND(SUM(o.O_TOTALPRICE)::numeric, 2) as total_revenue
FROM tpch.REGION r
JOIN tpch.NATION n ON r.R_REGIONKEY = n.N_REGIONKEY
JOIN tpch.CUSTOMER c ON n.N_NATIONKEY = c.C_NATIONKEY
JOIN tpch.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
GROUP BY r.R_REGIONKEY, r.R_NAME
ORDER BY total_revenue DESC;
"""

try:
    result = st.session_state.db.execute_query(query)
    if result:
        df = pd.DataFrame(result, columns=['Region', 'Orders', 'Revenue'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(df, x='Region', y='Revenue',
                        title="Total Revenue by Region")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(df, values='Revenue', names='Region',
                        title="Revenue Distribution")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# Top Products
st.header("Top Products by Sales")

query = """
SELECT p.P_NAME as product,
       SUM(l.L_QUANTITY) as total_quantity,
       ROUND(SUM(l.L_EXTENDEDPRICE)::numeric, 2) as total_sales
FROM tpch.PART p
JOIN tpch.LINEITEM l ON p.P_PARTKEY = l.L_PARTKEY
GROUP BY p.P_PARTKEY, p.P_NAME
ORDER BY total_sales DESC
LIMIT 10;
"""

try:
    result = st.session_state.db.execute_query(query)
    if result:
        df = pd.DataFrame(result, columns=['Product', 'Quantity', 'Sales'])
        
        fig = px.bar(df, x='Product', y='Sales',
                    title="Top 10 Products by Sales Volume")
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error: {e}")