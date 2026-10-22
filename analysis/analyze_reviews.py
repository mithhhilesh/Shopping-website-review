import os
import sys
from pathlib import Path
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("Python used by Spark:", sys.executable)

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

print("Spark version:", sc.version)
print("Spark is ready")

rating_lines = sc.textFile(
    "file:///C:/BDA Project/results/rating_counts.tsv"
)

print("MapReduce results:", rating_lines.collect())

rating_counts = rating_lines.map(
    lambda line: tuple(int(value) for value in line.split())
)

print("Rating-count pairs:", rating_counts.collect())

total_reviews = rating_counts.map(lambda pair: pair[1]).sum()

print("Total reviews from Hadoop results:", total_reviews)

total_rating_points = rating_counts.map(
    lambda pair: pair[0] * pair[1]
).sum()

average_rating = total_rating_points / total_reviews

print("Average rating:", round(average_rating, 2))

low_rating_count = rating_counts.filter(
    lambda pair: pair[0] <= 2
).map(lambda pair: pair[1]).sum()

low_rating_percentage = low_rating_count / total_reviews * 100

print("Low-rated reviews:", low_rating_count)
print("Low-rating percentage:", round(low_rating_percentage, 2), "%")

import csv

reviews_file = Path(r"C:\BDA Project\data\Meesho.csv")

with reviews_file.open(encoding="utf-8-sig", newline="") as file:
    reviews = list(csv.DictReader(file))

print("Original reviews loaded:", len(reviews))

reviews_rdd = sc.parallelize(reviews)

print("Reviews in Spark RDD:", reviews_rdd.count())

replied_count = reviews_rdd.filter(
    lambda review: bool(review["replyContent"].strip())
).count()

reply_percentage = replied_count / total_reviews * 100

print("Reviews with company replies:", replied_count)
print("Company reply rate:", round(reply_percentage, 2), "%")

reply_by_rating = reviews_rdd.map(
    lambda review: (
        int(review["score"]),
        (1, int(bool(review["replyContent"].strip())))
    )
).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

print("Reply coverage by rating:", sorted(reply_by_rating.collect()))

reply_file = r"C:\BDA Project\results\reply_by_rating.csv"

with open(reply_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["rating", "total_reviews", "replied", "unanswered"])

    for rating, (total, replied) in sorted(reply_by_rating.collect()):
        writer.writerow([rating, total, replied, total - replied])

print("Saved reply coverage:", reply_file)

first_review_date = datetime.fromtimestamp(
    int(reviews[0]["at"]) / 1000,
    tz=timezone.utc
)

print("First review date (UTC):", first_review_date)
print("Review month:", first_review_date.strftime("%Y-%m"))

monthly_totals = reviews_rdd.map(
    lambda review: (
        datetime.fromtimestamp(
            int(review["at"]) / 1000, tz=timezone.utc
        ).strftime("%Y-%m"),
        (int(review["score"]), 1)
    )
).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

print("First 5 months:", monthly_totals.sortByKey().take(5))

monthly_averages = monthly_totals.mapValues(
    lambda totals: (totals[1], totals[0] / totals[1])
)

print("First 5 monthly averages:",
      monthly_averages.sortByKey().take(5))

monthly_file = r"C:\BDA Project\results\monthly_trends.csv"

with open(monthly_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["month", "review_count", "average_rating"])

    for month, (count, average) in monthly_averages.sortByKey().collect():
        writer.writerow([month, count, average])

print("Saved monthly trends:", monthly_file)


topic_keywords = {
    "Delivery": ["delivery", "delivered", "shipping"],
    "Refund and Return": ["refund", "return", "exchange"],
    "Product Quality": ["quality", "damaged", "broken", "defective"],
    "Payment": ["payment", "deducted", "transaction"],
    "App Issues": ["crash", "error", "not opening", "not working"]
}

print("Topics defined:", list(topic_keywords))

critical_reviews = reviews_rdd.filter(
    lambda review: int(review["score"]) <= 2
)

print("Reviews selected for topic analysis:", critical_reviews.count())

def find_topics(review):
    text = review["content"].lower()
    matches = []

    for topic, keywords in topic_keywords.items():
        if any(keyword in text for keyword in keywords):
            matches.append((topic, 1))

    return matches

print("First review topic matches:", find_topics(reviews[0]))

topic_counts = critical_reviews.flatMap(find_topics).reduceByKey(
    lambda a, b: a + b
)

topic_results = dict(topic_counts.collect())

topic_file = r"C:\BDA Project\results\complaint_topics.csv"

with open(topic_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["topic", "review_count", "percentage_of_low_rated"])

    for topic in topic_keywords:
        count = topic_results.get(topic, 0)
        percentage = count / low_rating_count * 100
        writer.writerow([topic, count, percentage])
        print(topic, ":", count)

print("Saved complaint topics:", topic_file)

top_critical_reviews = critical_reviews.takeOrdered(
    10,
    key=lambda review: (
        -int(review["thumbsUpCount"]),
        review["reviewId"]
    )
)

helpful_file = r"C:\BDA Project\results\helpful_critical_reviews.csv"

with open(helpful_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "review_id", "rating", "review",
        "thumbs_up", "company_replied"
    ])

    for review in top_critical_reviews:
        writer.writerow([
            review["reviewId"],
            int(review["score"]),
            review["content"],
            int(review["thumbsUpCount"]),
            bool(review["replyContent"].strip())
        ])

print("Helpful critical reviews saved:", len(top_critical_reviews))

unanswered_reviews = critical_reviews.filter(
    lambda review: not review["replyContent"].strip()
)

unanswered_count = unanswered_reviews.count()

unanswered_results = unanswered_reviews.sortBy(
    lambda review: (
        -int(review["thumbsUpCount"]),
        review["reviewId"]
    )
).collect()

unanswered_file = r"C:\BDA Project\results\unanswered_critical_reviews.csv"

with open(unanswered_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["review_id", "rating", "review", "thumbs_up"])

    for review in unanswered_results:
        writer.writerow([
            review["reviewId"],
            int(review["score"]),
            review["content"],
            int(review["thumbsUpCount"])
        ])

print("Unanswered critical reviews:", unanswered_count)

rating_file = r"C:\BDA Project\results\rating_distribution.csv"

with open(rating_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["rating", "review_count"])

    for rating, count in rating_counts.sortByKey().collect():
        writer.writerow([rating, count])

print("Saved rating distribution:", rating_file)

monthly_review_count = monthly_totals.values().map(
    lambda totals: totals[1]
).sum()

reply_total = reply_by_rating.values().map(
    lambda totals: totals[1]
).sum()

assert total_reviews == len(reviews), "Review totals do not match"
assert monthly_review_count == total_reviews, "Monthly counts do not match"
assert reply_total == replied_count, "Reply counts do not match"
assert unanswered_count == 485, "Unexpected unanswered critical review count"

print("All result checks passed")



# Summary
summary_file = r"C:\BDA Project\results\summary.csv"

with open(summary_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["total_reviews", "average_rating",
                     "low_rating_count", "low_rating_percentage",
                     "replied_count", "reply_percentage"])
    writer.writerow([total_reviews, average_rating,
                     low_rating_count, low_rating_percentage,
                     replied_count, reply_percentage])

print("Saved summary:", summary_file)

sc.stop()
