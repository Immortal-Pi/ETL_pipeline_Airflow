import streamlit as st 
import plotly.graph_objects as go
from datetime import datetime, timedelta,date
import psycopg2 
import dotenv
import pandas as pd 


dotenv.load_dotenv()

def get_data_from_postgres(start_date:datetime=datetime.today()-timedelta(days=90),end_date:datetime=datetime.today()):
    conn=psycopg2.connect(
                database="postgres",
                user="postgres",
                password="postgres",
                host="127.0.0.1",
                port=5432,
            )

    query=""" 
    select * from bitcoin_data where Last_Refreshed_Date>=%s and Last_Refreshed_Date<=%s order by Last_Refreshed_Date desc;
    """

    df=pd.read_sql(query,conn,params=(start_date,end_date))

    conn.close()
    return df 


st.title('Bitcoin')

start_date=st.date_input('Start Date',pd.to_datetime(datetime.today()-timedelta(days=90)))
end_date=st.date_input('End Date',pd.to_datetime(datetime.today()))
df=get_data_from_postgres(start_date,end_date)
if not df.empty:
    
    df['Last_Refreshed_Date']=pd.to_datetime(df['last_refreshed_date'])

    fig=go.Figure(data=[go.Candlestick(
        x=df['Last_Refreshed_Date'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price'],
        increasing_line_color='green',
        decreasing_line_color='red'
    )])

    fig.update_layout(title='Bitcoin Candlestick Chart',
                      xaxis_title='Date',
                      yaxis_title='Price (USD)',
                      xaxis_rangeslider_visible=False 
                      )
    
    st.plotly_chart(fig)
else:
    st.write('No data available for the selected date range')

