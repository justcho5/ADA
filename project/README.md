# Title
Is there more to reviews than just the product?

# Abstract
Online product reviews are a major factor when purchasing products. Frequently, before purchasing an item, we assess the quality by looking at the star rating and reviews. But how much are the ratings influenced by factors other than the product itself? Different cognitive biases may influence raters to rate or speak more highly of certain products over others. For example, we often ascribe more positive attributes to purchases we’ve made and this is known as post-purchase rationalization. In this project we want to analyze the Amazon review dataset in order to discover whether factors such as price, position in the sales ranking and time of the year in which a review is written can impact how customers perceive a product. We want to use this data to gain an insight on how these factors can play a role in user reviews.

# Data story
The final data story is available [here](https://justcho5.github.io/)

# Research questions
1. Can we observe a periodic pattern in average user rating?
    * Does ice cream get better reviews in summer?
    * Is cold medicine reviewed more highly in winter?

2. Can we calculate a sentiment score based on the review text and is sentiment score related to user ratings?
    * Is a negative sentiment correlated to a lower user rating?

3. Are more expensive items rated higher? Is there choice supportive bias?

### Update
We tried to look at our hypothesis on influence of price, salesRank, and time of year on user reviews and we found little on the impact of these factors. However as we looked through our reviews more closely, we saw that some of the reviews had disclaimers that stated that they were giving unbiased reviews despite receiving the product for free. We thought it would be interesting to look at this subset of the reviews and ask whether these so-called unbiased reviews are really unbiased. Our brief look into this phenomena on a fraction of the full dataset has given us interesting results. We want to dig deeper:

1. Do supposedly unbiased but incentivized reviews have an inherent bias in terms of skew in rating?
2. Are certain category of products more prone to rating discrepency between incentivized and non-incentivized reviews?
3. Are incentivized product reviews longer? Are they more positive in terms of text sentiment?


# Dataset
We are going to use the Amazon reviews dataset. This dataset is large (about 100 GB for the reviews file and 20 GB for the metadata file when uncompressed) and needs to be processed using the cluster. Each line in the data files is a JSON object and the files can be imported line by line using a for loop in Python. The dataset's page has some Python code examples that explain how to extract the data out of the raw files and import it into a Pandas DataFrame so we can use that to get started. Since the metadata about the product being reviewed (category ranking, price...) are in a separate dataset we will need to use Pandas' join functionality to combine them. We are going to use the overall score, the product's price, the product's category ranking, the review's text and its time in our project.
Example of accessing the data:
```python
import json
import pandas as pd

with open("Musical_Instruments_5.json", "r") as f:
    reviews = []
    # Read the file line by line
    for line in f:
    	# Read one line as a json object
        dict = json.loads(line)
        # Add it to the rest of the data
        reviews.append(dict)

# Load everything into a dataframe
df = pd.DataFrame(reviews)

# How other users rated this review
df.helpful[0]

# The review's full text
df.reviewText[0]

# The score given to the product
df.overall[0]
```

# A list of internal milestones up until project milestone 2
We plan our progress in weeks:
1. Get access to the dataset and clusters. Import the dataset into memory. Get some initial statistical description of the data (average review score per price tier, average review score per category, average number of reviews per month...).
2. Perform data cleaning and wrangling (remove duplicates, fill in missing values...).
3. Study the data, get an idea of whether the correlations that we're trying to prove the existence of actually exist or if we need to adjust the scope of our project.
4. Document progress and plan for the next step. Write an outline of the data story that we will tell.


# A list of next steps
The next steps we have planned for our project up to milestone 3:
* Clean the data better to make a better spearation between incentivized reviews and non-incentivized.
* Explore additional ways to extract data from the text and related products in the dataset, in particular if it's possible to predict the final score of a review using a different type of classifier, for example by analyzing which keywords occur more frequently in reviews that the users consider more helpful or that we can determine to be more biased or correlates with high ratings.
* Try to determine if any of the users obtained any incentives to write any of the reviews by looking at particularly positive or negative sentiment scores or phrases that suggest that the review is biased (like the user stating the contrary in the description).
* Look for products that have high discrepancy between the incentivized reviews and non-incentivized and see whether incentivized reviews increase with time because this may be a way that businesses increase product exposure.
* Run the analysis on the full data using spark on the cluster to see if the results we obtained from the subsample are any different from the ones that we would obtain by analyzing the data in its entirety.
* Possibly enrich the data by using Amazon's item lookup API to download additional features.


# Questions for TAa
Will there be limits on how often we're allowed to use the compute cluster?
Can we get temporal data on price and sales ranking of products?
Are we going in the right direction with this proposal? Do you think there are any other research subtopics we should consider?


# Contributions of all members
* Matteo: Data processing on the cluster, incentivized review detection algorithm,
code documentation, initial data analysis, final presentation;
* Hyun Jii: Data story, initial analysis, incentivized review idea, final presentation;
* Chia-An: Data story, data processing on the cluster, initial analysis, cluster
result processing and plotting, final presentation;
