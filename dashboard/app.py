import streamlit as st
import pandas as pd
from pathlib import Path

# page setup
st.set_page_config(
    page_title="Shopping website Review Analysis",
    page_icon="🛍️",
    layout="wide"
)

# title
st.title("Meesho Review Analysis")
st.caption("Big Data Analysis Lab project using Hadoop MapReduce and PySpark")

# results folder
results_folder = Path(__file__).resolve().parent.parent / "results"

# load summary
summary = pd.read_csv(results_folder / "summary.csv")

st.subheader("Analysis Summary")

# show main stats
metrics = summary.iloc[0]

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Reviews", f"{int(metrics['total_reviews']):,}")
col2.metric("Average Rating", f"{metrics['average_rating']:.2f} / 5")
col3.metric("Low-Rated Reviews", f"{metrics['low_rating_percentage']:.2f}%")
col4.metric("Company Reply Rate", f"{metrics['reply_percentage']:.2f}%")

# rating distribution
ratings = pd.read_csv(results_folder / "rating_distribution.csv")

st.subheader("How Reviewers Rated Meesho")

ratings["rating"] = ratings["rating"].astype(str) + " star"

st.bar_chart(
    ratings,
    x="rating",
    y="review_count",
    color="#E7258A"
)

st.caption("Review counts calculated using Java Hadoop MapReduce.")

# monthly rating trends
monthly = pd.read_csv(results_folder / "monthly_trends.csv")
monthly["month"] = pd.to_datetime(monthly["month"])

st.subheader("How Ratings Changed Over Time")

st.line_chart(
    monthly,
    x="month",
    y="average_rating",
    color="#E7258A"
)

st.caption(
    "Monthly average ratings calculated using PySpark. "
    "Months with fewer reviews may show larger fluctuations."
)

# common complaints
topics = pd.read_csv(results_folder / "complaint_topics.csv")

st.subheader("Common Topics in Low-Rated Reviews")

st.bar_chart(
    topics,
    x="topic",
    y="review_count",
    color="#E7258A"
)

st.caption(
    "Keyword-based topic mentions in 1–2 star reviews. "
    "One review can match multiple topics. "
    "A keyword match does not confirm a complaint."
)

# company replies by rating
replies = pd.read_csv(results_folder / "reply_by_rating.csv")
replies["rating"] = replies["rating"].astype(str) + " star"

st.subheader("Company Replies by Star Rating")

st.bar_chart(
    replies,
    x="rating",
    y=["replied", "unanswered"],
    color=["#E7258A", "#B8BCC8"],
    stack=True
)

st.caption(
    "Each bar shows replied and unanswered reviews. "
    "A company reply does not confirm that the issue was resolved."
)

# top 10 helpful reviews
helpful = pd.read_csv(
    results_folder / "helpful_critical_reviews.csv"
)

st.subheader("Top 10 Most Helpful Critical Reviews")

st.dataframe(
    helpful[["rating", "review", "thumbs_up", "company_replied"]],
    hide_index=True,
    column_config={
        "rating": st.column_config.NumberColumn("Star Rating"),
        "review": st.column_config.TextColumn("Review", width="large"),
        "thumbs_up": st.column_config.NumberColumn("Helpful Votes"),
        "company_replied": st.column_config.CheckboxColumn("Company Replied")
    }
)

st.caption(
    "1–2 star reviews ranked by thumbs-up votes. "
    "Votes indicate perceived helpfulness, not verified accuracy."
)

# unanswered critical reviews
unanswered = pd.read_csv(
    results_folder / "unanswered_critical_reviews.csv"
)

st.subheader("Critical Reviews Without a Company Reply")
st.write(f"{len(unanswered):,} low-rated reviews have no recorded reply.")

st.dataframe(
    unanswered[["rating", "review", "thumbs_up"]],
    hide_index=True,
    column_config={
        "rating": st.column_config.NumberColumn("Star Rating"),
        "review": st.column_config.TextColumn("Review", width="large"),
        "thumbs_up": st.column_config.NumberColumn("Helpful Votes")
    }
)

st.caption(
    "1–2 star reviews without recorded company replies, "
    "ordered by helpfulness votes."
)

# project details
with st.expander("About this academic project"):
    st.markdown("""
    **Dataset:** 36,000 Meesho app reviews from September 2018
    to August 2024.

    **Processing flow:** CSV preparation → Hadoop HDFS →
    Java MapReduce → PySpark → Streamlit.

    - **HDFS:** stores the original dataset and prepared ratings.
    - **Java MapReduce:** counts reviews for each star rating.
    - **PySpark:** calculates summary metrics, monthly trends,
      keyword topic counts, reply coverage, and review rankings.
    - **Streamlit:** displays the saved analysis results.

    """)
