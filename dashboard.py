import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# Page Configuration
st.set_page_config(
    page_title="SmartLogix - Lakehouse Operations Dashboard",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark-themed styling
st.markdown("""
    <style>
    .main { background-color: #121212; color: #E0E0E0; }
    .stMetric { background-color: #1E1E1E; border-radius: 8px; padding: 15px; border: 1px solid #333333; }
    .stAlert { border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)

# Setup JDK and Hadoop environment variables for PySpark
script_dir = os.path.dirname(os.path.abspath(__file__))
local_jdk_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "jdk"))
if not os.environ.get("JAVA_HOME") and os.path.exists(local_jdk_path):
    subdirs = [os.path.join(local_jdk_path, d) for d in os.listdir(local_jdk_path) if os.path.isdir(os.path.join(local_jdk_path, d))]
    if subdirs:
        os.environ["JAVA_HOME"] = subdirs[0]
        os.environ["PATH"] = os.path.join(subdirs[0], "bin") + os.path.pathsep + os.environ.get("PATH", "")

local_hadoop_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "hadoop"))
if not os.environ.get("HADOOP_HOME") and os.path.exists(local_hadoop_path):
    os.environ["HADOOP_HOME"] = local_hadoop_path
    os.environ["PATH"] = os.path.join(local_hadoop_path, "bin") + os.path.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

# Initialize cached Spark session
@st.cache_resource
def get_spark_session():
    # Configure PySpark to use the active virtual environment's Python executable
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    spark = SparkSession.builder \
        .appName("SmartLogix-Lakehouse-Dashboard") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", os.path.join(script_dir, "warehouse")) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    return spark

# Title
st.title("🧊 SmartLogix Real-Time Data Lakehouse")
st.subheader("Operations Dashboard Powered by Apache Iceberg & PySpark")

# Auto-refresh every 5 seconds
st_autorefresh(interval=5000, key="lakehouse_refresh")

try:
    spark = get_spark_session()
    
    # Query database and table presence
    db_exists = spark.sql("SHOW DATABASES LIKE 'db'").count() > 0
    table_exists = False
    if db_exists:
        table_exists = spark.sql("SHOW TABLES IN local.db LIKE 'shipments'").count() > 0
        
    if not table_exists:
        st.warning("⚠️ Apache Iceberg table `local.db.shipments` not found! Please run the ingestion job (`python spark_lakehouse.py`) to generate data.")
    else:
        # Load data from Iceberg table into Pandas
        df = spark.sql("SELECT * FROM local.db.shipments").toPandas()
        
        if df.empty:
            st.info("ℹ️ Iceberg table `local.db.shipments` is currently empty. Run the Kafka producer and Spark ingestion script to load data.")
        else:
            # Sort by timestamp
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values(by="timestamp", ascending=False)
                
            # --- KPIs Metric Row ---
            total_shipments = len(df)
            total_revenue = df["revenue"].sum() if "revenue" in df.columns else 0.0
            avg_weight = df["weight"].mean() if "weight" in df.columns else 0.0
            unique_vehicles = df["vehicle_id"].nunique() if "vehicle_id" in df.columns else 0
            
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("📦 Total Shipments", f"{total_shipments:,}")
            kpi2.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
            kpi3.metric("⚖️ Avg Weight", f"{avg_weight:.2f} kg")
            kpi4.metric("🚛 Active Vehicles", f"{unique_vehicles}")
            
            st.markdown("---")
            
            # --- Visualizations Row ---
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Destination Revenue Breakdown")
                if "destination" in df.columns and "revenue" in df.columns:
                    dest_df = df.groupby("destination")["revenue"].sum().reset_index()
                    fig_dest = px.bar(
                        dest_df, 
                        x="destination", 
                        y="revenue", 
                        labels={"destination": "Destination City", "revenue": "Revenue ($)"},
                        color="revenue", 
                        template="plotly_dark",
                        color_continuous_scale=px.colors.sequential.Bluyl
                    )
                    st.plotly_chart(fig_dest, use_container_width=True)
                    
            with col2:
                st.subheader("Priority Distribution")
                if "priority" in df.columns:
                    pri_df = df["priority"].value_counts().reset_index()
                    fig_pri = px.pie(
                        pri_df, 
                        values="count", 
                        names="priority", 
                        hole=0.4,
                        template="plotly_dark",
                        color_discrete_sequence=px.colors.qualitative.Pastel
                    )
                    st.plotly_chart(fig_pri, use_container_width=True)
            
            # --- Status Section ---
            st.subheader("Shipment Status Breakdown")
            if "status" in df.columns:
                status_df = df["status"].value_counts().reset_index()
                fig_status = px.bar(
                    status_df, 
                    y="status", 
                    x="count", 
                    orientation="h",
                    labels={"status": "Status", "count": "Count"},
                    color="status", 
                    template="plotly_dark"
                )
                st.plotly_chart(fig_status, use_container_width=True)
            
            # --- Recent Shipments Table ---
            st.subheader("Recent Lakehouse Shipments")
            st.dataframe(df.head(20), use_container_width=True)

except Exception as e:
    st.error(f"❌ Error connecting to Iceberg Catalog: {e}")
