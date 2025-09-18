import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()

# --- DB connection setup ---
pg_user = os.getenv("POSTGRES_USER")
pg_pw = os.getenv("POSTGRES_PASSWORD")
pg_host = os.getenv("POSTGRES_HOST")
pg_db = os.getenv("POSTGRES_DB")
pg_port = os.getenv("POSTGRES_PORT")


engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}?sslmode=require")

# --- Query your local DB ---
@st.cache_data
def load_calls_by_quadrant():
    query = """
            SELECT quadrant_normalize AS "Quadrant",
                   COUNT(1) AS "# Calls",
                   ROUND(100.0 * COUNT(1) / SUM(COUNT(1)) OVER (), 1) AS "Call %%",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => ROUND(AVG(response_time_seconds))),
                           'MI:SS'
                   ) AS "Avg Resp Time",
                   COUNT(1) FILTER (WHERE response_time_seconds >= 420) AS "+7 Min Resp",
                   round(((count(1) filter ( where response_time_seconds >= 420 ))::numeric / count(1)) * 100, 1) as "+7 Min Resp %%"
            FROM sheet_data
            GROUP BY quadrant_normalize, quadrant_sort
            ORDER BY quadrant_sort \
            """
    df = pd.read_sql(query, engine)
    # --- Calculate totals row ---
    totals = {
        "Quadrant": "TOTAL",
        "# Calls": df["# Calls"].sum(),
        "Call %": "",
        "Avg Resp Time": "",
        "+7 Min Resp": df["+7 Min Resp"].sum(),
        "+7 Min Resp %": "",
    }
    # Append totals row
    df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

    return df

@st.cache_data
def load_calls_by_city():
    query = """
            select city as "City",
                   count(1) as "# Calls",
                   ROUND(100.0 * COUNT(1) / SUM(COUNT(1)) OVER (), 1) AS "Call %%",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => ROUND(AVG(response_time_seconds))),
                           'MI:SS'
                   ) AS "Avg Resp Time",
                   count(1) filter ( where response_time_seconds >= 420 ) as "+7 Min Resp",
                   round(((count(1) filter ( where response_time_seconds >= 420 ))::numeric / count(1)) * 100, 1) as "+7 Min Resp %%"
            from sheet_data
            group by city
            ORDER BY count(1) desc \
            """
    df = pd.read_sql(query, engine)
    # --- Calculate totals row ---
    totals = {
        "City": "TOTAL",
        "# Calls": df["# Calls"].sum(),
        "Call %": "",
        "Avg Resp Time": "",
        "+7 Min Resp": df["+7 Min Resp"].sum(),
        "+7 Min Resp %": "",
    }
    # Append totals row
    df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

    return df

@st.cache_data
def load_overlapping_calls():
    query = """
            select 49 as "Call Count",
                   '8.0' as "Call %% of Total"
             \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_springville_details():
    query = """
            select count(1) as "# Calls",
                   count(1) filter ( where call_enroute_time is not null and call_complete_time is not null and call_arrived_time is null ) as "# Canceled",
                   round(((count(1) filter ( where call_enroute_time is not null and call_complete_time is not null and call_arrived_time is null ))::numeric / count(1)) * 100, 2) as "%% Canceled",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => round(avg(extract(epoch from call_complete_time - call_dispatched_time)) filter (
                               where call_enroute_time is not null
                                   and call_complete_time is not null
                                   and call_arrived_time is null
                               ))),
                           'MI:SS'
                   ) AS "Avg Response Until Canceled"

            from sheet_data
            where city = 'SPRINGVILLE'
             \
            """
    return pd.read_sql(query, engine)

@st.cache_data
def load_call_data_points():
    query = """
            select 'Call Creation Until Dispatched' as "Data Point",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => round(avg(extract(epoch from call_dispatched_time - call_psap_time)) filter (
                               where call_psap_time is not null and call_dispatched_time is not null
                               ))),
                           'MI:SS'
                   ) AS "All Calls",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => round(avg(extract(epoch from call_dispatched_time - call_psap_time)) filter (
                               where call_psap_time is not null and call_dispatched_time is not null and city = 'MAPLETON'
                               ))),
                           'MI:SS'
                   ) AS "Mapleton Only"
            from sheet_data
            union
            select 'Turnout Time' as "Data Point",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => round(avg(extract(epoch from call_enroute_time - call_dispatched_time)) filter (
                               where call_enroute_time is not null and call_dispatched_time is not null
                               ))),
                           'MI:SS'
                   ) AS "All Calls",
                   TO_CHAR(
                           MAKE_INTERVAL(secs => round(avg(extract(epoch from call_enroute_time - call_dispatched_time)) filter (
                               where call_enroute_time is not null and call_dispatched_time is not null and city = 'MAPLETON'
                               ))),
                           'MI:SS'
                   ) AS "Mapleton Only"
            from sheet_data
             \
            """
    return pd.read_sql(query, engine)

# todo: put this back after not hard coded anymore
# select count(1) filter ( where overlap_previous is true ) as "Call Count",
# round(((count(1) filter ( where overlap_previous is true ))::numeric / count(1) * 100), 1) as "Call %% of Total"
# from sheet_data \

@st.cache_data
def load_mapleton_times():
    query = """
            select "Timeframe", "Call Count", "Call %%"
            from (with t1 as (select *
                              from sheet_data
                              where city = 'MAPLETON')
                  select 'Canceled Prior to Arrival'                                                                          as "Timeframe",
                      count(1) filter ( where response_time_seconds is null)                                       as "Call Count",
                      round((count(1) filter ( where response_time_seconds is null )::numeric / count(1)) * 100,
                      1)                                                                                    as "Call %%",
                      1                                                                                           as sort_order
                  from t1
                  UNION
                  select 'Less than 5 mins'                                                                          as "Timeframe",
                         count(1) filter ( where response_time_seconds < 300 )                                       as "Call Count",
                         round((count(1) filter ( where response_time_seconds < 300 )::numeric / count(1)) * 100,
                               1)                                                                                    as "Call %%",
                         2                                                                                          as sort_order
                  from t1
                  UNION
                  select '5 - 7 Minutes'                                                                          as "Timeframe",
                         count(1)
                         filter ( where response_time_seconds >= 300 and response_time_seconds < 420 )          as "Call Count",
                         round((count(1)
                                filter ( where response_time_seconds >= 300 and response_time_seconds < 420 )::numeric /
                                count(1)) * 100,
                               1)                                                                               as "Call %%",
                         3                                                                                      as sort_order
                  from t1
                  UNION
                  select '7 - 9 Minutes'                                                                          as "Timeframe",
                         count(1)
                         filter ( where response_time_seconds >= 420 and response_time_seconds < 540 )          as "Call Count",
                         round((count(1)
                                filter ( where response_time_seconds >= 420 and response_time_seconds < 540 )::numeric /
                                count(1)) * 100,
                               1)                                                                               as "Call %%",
                         4                                                                                      as sort_order
                  from t1
                  union
                  select '+ 9 Minutes'                                                                                 as "Timeframe",
                         count(1) filter ( where response_time_seconds >= 540 )                                       as "Call Count",
                         round((count(1) filter ( where response_time_seconds >= 540 )::numeric / count(1)) * 100,
                               1)                                                                                     as "Call %%",
                         5                                                                                            as sort_order
                  from t1) as t2 order by sort_order \
            """
    return pd.read_sql(query, engine)


# --- Page config ---
st.set_page_config(
    page_title="Mapleton Fire",
    page_icon="assets/mapleton.png",
    layout="wide"
)

st.title("Mapleton City Call Details")

# --- Two-column layout ---
col1, col2 = st.columns(2)

# --- First column ---
with col1:
    st.subheader("Calls by Quadrant")
    df1 = load_calls_by_quadrant()
    st.dataframe(df1, use_container_width=True, hide_index=True)

    # --- Bar chart for # Calls ---
    # Exclude TOTAL row if present
    bar_df = df1[df1["Quadrant"] != "TOTAL"]

    fig = px.bar(
        bar_df,
        x="# Calls",       # values along the x-axis
        y="Quadrant",      # labels along the y-axis
        orientation='h',   # horizontal bars
        text="# Calls",    # show values on bars
        color="Quadrant"   # optional: color by quadrant
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Pie chart for Call %% ---
    st.markdown("Call % by Quadrant")
    # Exclude TOTAL row for pie chart
    pie_df = df1[df1["Quadrant"] != "TOTAL"]
    fig = px.pie(pie_df, names="Quadrant", values="Call %",
                 title="Call % Distribution by Quadrant",
                 color="Quadrant")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Mapleton Details")
    df3 = load_mapleton_times()
    st.dataframe(df3, use_container_width=True, hide_index=True)

    # --- Pie chart for Mapleton Details ---
    st.markdown("Call Distribution for Mapleton by Timeframe")

    # Exclude any TOTAL row if present
    pie_df3 = df3[df3["Timeframe"] != "TOTAL"].copy()

    # Recalculate percentages based on Call Count
    total_calls = pie_df3["Call Count"].sum()
    pie_df3["Percent"] = (pie_df3["Call Count"] / total_calls * 100).round(1)

    fig3 = px.pie(
        pie_df3,
        names="Timeframe",
        values="Percent",
        title="Mapleton Call % by Response Time",
        color="Timeframe"
    )

    st.plotly_chart(fig3, use_container_width=True)

# --- Second column ---
with col2:
    st.subheader("Calls by City")
    df2 = load_calls_by_city()
    st.dataframe(df2, use_container_width=True, hide_index=True)

    # Exclude TOTAL row if present
    bar_df_city = df2[df2["City"] != "TOTAL"]

    fig2 = px.bar(
        bar_df_city,
        x="# Calls",       # values along the x-axis
        y="City",      # labels along the y-axis
        orientation='h',   # horizontal bars
        text="# Calls",    # show values on bars
        color="City"   # optional: color by quadrant
    )

    st.plotly_chart(fig2, use_container_width=True)

    # --- Pie chart for Call %% ---
    st.markdown("Call % by City")

    # Exclude TOTAL row for pie chart
    pie_df2 = df2[df2["City"] != "TOTAL"]
    fig2 = px.pie(pie_df2, names="City", values="Call %",
                 title="Call % Distribution by City",
                 color="City")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Overlapping Calls")
    df4 = load_overlapping_calls()
    st.dataframe(df4, use_container_width=False, hide_index=True)

    st.subheader("Additional Call Details")
    df5 = load_call_data_points()
    st.dataframe(df5, use_container_width=False, hide_index=True)

    st.subheader("Springville Details")
    df6 = load_springville_details()
    st.dataframe(df6, use_container_width=False, hide_index=True)

