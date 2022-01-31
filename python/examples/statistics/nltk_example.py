from turtle import down
import nltk 
import pandas as pd
import pyarrow as pa

def download():
    nltk.download([
        "names",
        "stopwords",
        "state_union",
        "twitter_samples",
        "movie_reviews",
        "averaged_perceptron_tagger",
        "vader_lexicon",
        "punkt",
    ])
    
def test_nltk():
    from nltk.sentiment import SentimentIntensityAnalyzer
    sia = SentimentIntensityAnalyzer()
    score = sia.polarity_scores("Wow, NLTK is really powerful!")
    for item in score:
        print(item, score[item])


def get_nltk_tweets():
    tweets = [t.replace("://", "//") for t in nltk.corpus.twitter_samples.strings()]
    return tweets


def make_product_info(tweets):
    import random
    num_records = len(tweets)
    product_names = []
    product_quantities = []
    regions = []
    region_types = {0: "US", 1: "UK", 2: "JPN", 3: "IND", 4: "AUS"}
    for id in range(num_records):
        product_name = "prod-" + str(id)
        product_quantity = random.randint(0, 1000)
        region = region_types[random.randint(0, 4)]
        product_names.append(product_name)
        product_quantities.append(product_quantity)
        regions.append(region)
    dict_data = {"product_name": product_names, 
                 "product_quantity": product_quantities,
                 "region": regions,
                 "review" : tweets}
    
    data_table = pa.Table.from_pydict(dict_data)
    return data_table
        

data_table = make_product_info(get_nltk_tweets())

print(data_table[0:5].to_pandas())


