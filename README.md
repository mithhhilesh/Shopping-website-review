# 🛍️ Shopping Website Review Analysis

An academic project analyzing **36,000 Meesho app reviews** using **Hadoop HDFS, Java MapReduce, PySpark, and Streamlit**.

## Overview

The project explores customer ratings, monthly trends, topics mentioned in low-rated reviews, company reply coverage, and helpful critical feedback.

## Workflow

```text
CSV Preparation → HDFS → Java MapReduce → PySpark → CSV Results → Streamlit
```

- **HDFS:** Stores the original dataset and prepared ratings.
- **MapReduce:** Counts reviews for each star rating.
- **PySpark:** Calculates metrics, trends, topic counts, and review rankings.
- **Streamlit:** Displays saved results through cards, charts, and tables.

## Dashboard Features

- Summary cards for review count, average rating, low-rating share, and reply rate.
- Rating distribution and monthly average-rating charts.
- Keyword-based topic analysis of 1–2 star reviews.
- Replied versus unanswered reviews by rating.
- Top 10 helpful critical reviews.
- Unanswered critical review table.

## Key Findings

| Metric | Result |
|---|---:|
| Reviews analyzed | 36,000 |
| Average rating | 2.35 / 5 |
| Low-rated reviews | 63.41% |
| Company reply rate | 96.88% |
| Unanswered low-rated reviews | 485 |

## Tech Stack

Java 8 · Hadoop 3.2.4 · Python 3.10 · PySpark 3.3.4 · pandas · Streamlit

## Project Structure

```text
├── data/          # Original CSV and prepared ratings
├── preparation/   # Data preparation
├── mapreduce/     # Java Mapper, Reducer, and Driver
├── analysis/      # PySpark analysis
├── results/       # Generated analytical outputs
└── dashboard/     # Streamlit app
```

## Run the Dashboard

With Python 3.10 and the generated result files available, run from the project root:

```bash
python -m pip install streamlit pandas
python -m streamlit run dashboard/app.py
```

The dashboard reads precomputed results; Hadoop and Spark are not required to display them. Regenerating results requires the processing environment. Preparation and analysis scripts currently use `C:\BDA Project`; update their paths if needed.

## Academic Context

Developed for **Big Data Analytics Laboratory**, Course Code **24CS02PR0503**, applying HDFS commands, Java MapReduce, and PySpark RDD operations.

## Limitations

Historical data; keyword matches are not verified complaints, helpfulness votes do not verify accuracy, and company replies do not establish resolution. Hadoop runs on one computer, and PySpark analysis runs locally.
````
