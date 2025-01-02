import happybase
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set page configuration to wide layout
fig, ax = plt.subplots()

# Customize the plot
ax.set_title("E-commerce App Transactional Dashboard", fontsize=10)
# Adjust margins: top should be greater than bottom
plt.subplots_adjust(top=0.95, bottom=0.1)  # Adjust the top and bottom margins

st.markdown(
    """
    <style>
    body {
        background-color: #eedcd3; /* Cream-yellow background */
        color: #2E4B28; /* Dark green text */
        margin-top: 0px !important; /* Set top margin to 0 to remove unnecessary space */
    }
    .css-18e3th9 {  /* For Streamlit's main container */
        background-color: #eedcd3 !important;
        margin-top: 0px !important; /* Set top margin to 0 */
    }
    .css-1v3fvcr { /* For Streamlit's header */
        margin-top: 0px !important; /* Remove header margin */
    }
    .css-1f5iv3s { /* For Streamlit's main content */
        margin-top: 0px !important; /* Adjust content margin */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Connect to HBase (update with your HBase server details)
connection = happybase.Connection('localhost', port=9090)
connection.open()

# Function to fetch data from HBase and preprocess it
def fetch_data_from_hbase(table_name, column_mapping):
    table = connection.table(table_name)
    rows = table.scan()
    data = []
    for _, row in rows:
        mapped_row = {}
        for col_key, col_name in column_mapping.items():
            mapped_row[col_name] = row[col_key].decode() if col_key in row else None
        data.append(mapped_row)
    return pd.DataFrame(data)

# Fetch data from HBase (product, customer, transaction, click stream)
product_mapping = {
    b'product_info:productDisplayName': 'Product Name',
    b'product_details:gender': 'Gender',
    b'product_details:masterCategory': 'Master Category',
    b'product_details:subCategory': 'Sub Category',
    b'product_details:articleType': 'Article Type',
    b'product_details:baseColour': 'Base Colour',
    b'metadata:season': 'Season',
    b'metadata:year': 'Year',
    b'metadata:usage': 'Usage',
}
product_df = fetch_data_from_hbase('product', product_mapping)

customer_mapping = {
    # Personal Info
    b'personal_info:first_name': 'First Name',
    b'personal_info:last_name': 'Last Name',
    b'personal_info:username': 'Username',
    b'personal_info:email': 'Email',
    b'personal_info:gender': 'Gender',
    b'personal_info:birthdate': 'Birthdate',
    
    # Device Info
    b'device_info:device_type': 'Device Type',
    b'device_info:device_id': 'Device ID',
    b'device_info:device_version': 'Device Version',
    
    # Location Info
    b'location_info:home_location_lat': 'Home Location Latitude',
    b'location_info:home_location_long': 'Home Location Longitude',
    b'location_info:home_location': 'Home Location',
    b'location_info:home_country': 'Home Country',
    
    # Join Info
    b'join_info:first_join_date': 'First Join Date',
}

customer_df = fetch_data_from_hbase('customer', customer_mapping)

click_stream_mapping = {
    # Event Details
    b'event_details:event_name': 'Event Name',
    b'event_details:event_time': 'Event Time',
    b'event_details:event_id': 'Event ID',
    
    # Traffic Info
    b'traffic_info:traffic_source': 'Traffic Source',
    
    # Metadata (if any)
    b'metadata:event_metadata': 'Event Metadata',
}

click_stream_df = fetch_data_from_hbase('click_stream', click_stream_mapping)

transaction_mapping = {
    # Transaction Details
    b'transaction_details:created_at': 'Transaction Date',
    b'transaction_details:customer_id': 'Customer ID',
    b'transaction_details:session_id': 'Session ID',
    b'transaction_details:product_metadata': 'Product Metadata',
    
    # Payment Info
    b'payment_info:payment_method': 'Payment Method',
    b'payment_info:payment_status': 'Payment Status',
    b'payment_info:promo_amount': 'Promo Amount',
    b'payment_info:promo_code': 'Promo Code',
    
    # Shipment Info
    b'shipment_info:shipment_fee': 'Shipment Fee',
    b'shipment_info:shipment_date_limit': 'Shipment Date Limit',
    b'shipment_info:shipment_location_lat': 'Shipment Location Latitude',
    b'shipment_info:shipment_location_long': 'Shipment Location Longitude',
    
    # Transaction Summary
    b'transaction_summary:total_amount': 'Total Amount',
}

transaction_df = fetch_data_from_hbase('transactions', transaction_mapping)


# Data Preprocessing
click_stream_df['Event Time'] = pd.to_datetime(click_stream_df['Event Time'], errors='coerce')
click_stream_df['Hour'] = click_stream_df['Event Time'].dt.hour
customer_df['First Join Date'] = pd.to_datetime(customer_df['First Join Date'], errors='coerce')
transaction_df['Transaction Date'] = pd.to_datetime(transaction_df['Transaction Date'], errors='coerce')

# Convert Birthdate column to datetime format (handle errors and missing values)
customer_df['Birthdate'] = pd.to_datetime(customer_df['Birthdate'], errors='coerce')

# Handle missing gender data by filling with 'Unknown' or a default value
customer_df['Gender'] = customer_df['Gender'].fillna('Unknown')

# Fill missing values with "Unknown"
product_df = product_df.fillna("Unknown")
customer_df = customer_df.fillna("Unknown")
click_stream_df = click_stream_df.fillna("Unknown")
transaction_df = transaction_df.fillna("Unknown")

# Dashboard Layout
st.title("Enhanced Analytics Dashboard", anchor='center')

# Row 1: Overview Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Products", len(product_df))
col2.metric("Total Customers", len(customer_df))
col3.metric("Total Transactions", len(transaction_df))

# Row 2: Product Analysis (Gender, Category Distribution)
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    # Chart 1: Donut chart (Gender Distribution)
    st.write("*Gender Distribution*")
    gender_counts = customer_df['Gender'].value_counts().head(2)
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.pie(gender_counts.values, labels=gender_counts.index, autopct='%1.1f%%', 
           colors=['#E06543', '#95e795', '#91583a', '#7bcfed'], wedgeprops=dict(width=0.3))
    st.pyplot(fig)

with col2:
    # Chart 2: Pie chart (Master Category Distribution)
    st.write("*Master Category Distribution*")
    category_counts = product_df['Master Category'].value_counts().head(4)
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%', 
           colors=['#E06543', '#95e795', '#91583a', '#7bcfed'])
    st.pyplot(fig)

with col3:
    # Chart 3: Sub Category Distribution (Bar chart with custom colors for 2 lowest and 2 highest)
    st.write("*Sub Category Distribution*")
    subcategory_counts = product_df['Sub Category'].value_counts().head(5)
    custom_colors = ['#1d120c', '#583523', '#91583a', '#c58c9d', '#dcbaa7']
    fig, ax = plt.subplots(figsize=(4, 3))
    sns.barplot(x=subcategory_counts.index, y=subcategory_counts.values, ax=ax, palette=custom_colors)
    ax.set_xlabel('Sub Category')
    ax.set_ylabel('Count')
    st.pyplot(fig)

with col4:
    # Chart 4: Usage Distribution (Bar chart with custom colors for top 3)
    st.write("*Usage Distribution*")
    usage_counts = product_df['Usage'].value_counts().head(3)
    custom_colors = ['#cdf4cd', '#95e795', '#135313']
    fig, ax = plt.subplots(figsize=(4, 3))
    sns.barplot(x=usage_counts.index, y=usage_counts.values, ax=ax, palette=custom_colors)
    ax.set_xlabel('Usage')
    ax.set_ylabel('Count')
    st.pyplot(fig)

with col5:
    # Chart 5: Traffic Source Distribution from Click Stream (Pie chart)
    st.write("*Traffic Source Distribution from Click Stream*")
    traffic_source_counts = click_stream_df['Traffic Source'].value_counts().head(4)
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.pie(traffic_source_counts.values, labels=traffic_source_counts.index, autopct='%1.1f%%', 
           colors=['#E06543', '#95e795', '#91583a', '#7bcfed'])
    ax.set_title('Traffic Source Distribution from Click Stream')
    st.pyplot(fig)


# Row 4: Customer Transaction Analysis (Last 5 graphs in one row)
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    # Chart 6: Bar chart of Customer Age Distribution
    st.write("*Customer Age Distribution*")
    customer_df['Birthdate'] = pd.to_datetime(customer_df['Birthdate'], errors='coerce')
    customer_df['Age'] = 2024 - customer_df['Birthdate'].dt.year
    age_counts = customer_df['Age'].value_counts().head(5)
    fig, ax = plt.subplots(figsize=(4, 3))
    sns.barplot(x=age_counts.index, y=age_counts.values, ax=ax, palette=['#Dbf2fa', '#7bcfed', '#28b0e2', '#146f90', '#0a3748'])
    ax.set_xlabel('Age')
    ax.set_ylabel('Count')
    st.pyplot(fig)

with col2:
    # Chart 7: Pie chart of Event Name from Click Stream
    st.write("*Event Name Distribution*")
    event_name_counts = click_stream_df['Event Name'].value_counts().head(5)
    if event_name_counts.empty:
        st.write("No data available for event names.")
    else:
        fig, ax = plt.subplots(figsize=(3, 3))
        ax.pie(event_name_counts.values, labels=event_name_counts.index, autopct='%1.1f%%', 
               colors=['#E06543', '#95e795', '#91583a', '#7bcfed', '#cdf4cd'])
        st.pyplot(fig)

with col3:
    # Chart 9: Pie chart of Device Type
    st.write("*Device Type Distribution*")
    device_type_counts = customer_df['Device Type'].value_counts().head(4)
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.pie(device_type_counts.values, labels=device_type_counts.index, autopct='%1.1f%%', 
           colors=['#E06543', '#95e795', '#91583a', '#7bcfed'])
    st.pyplot(fig)

with col4:
    # Chart 10: Donut chart of Payment Method
    st.write("*Payment Method Distribution*")
    payment_method_counts = transaction_df['Payment Method'].value_counts().head(4)
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.pie(payment_method_counts.values, labels=payment_method_counts.index, autopct='%1.1f%%', 
           colors=['#E06543', '#95e795', '#91583a', '#7bcfed'], wedgeprops=dict(width=0.3))
    st.pyplot(fig)

with col5:
    # Chart 11: Trend line graph of transactions over the years
    st.write("*Transaction Trend Over the Years*")
    transaction_df['Transaction Date'] = pd.to_datetime(transaction_df['Transaction Date'], errors='coerce')
    transaction_df['Year'] = transaction_df['Transaction Date'].dt.year
    yearly_transactions = transaction_df['Year'].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(4, 3))
    sns.lineplot(x=yearly_transactions.index, y=yearly_transactions.values, ax=ax, marker='o', color='#E06543')
    ax.set_xlabel('Year')
    ax.set_ylabel('Number of Transactions')
    st.pyplot(fig)
