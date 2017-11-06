# Title
Is there more to reviews than just the product?

# Abstract
Online product reviews are a major factor when purchasing products. Frequently, before purchasing an item, we assess the quality by looking at the star rating and reviews. But how much are the ratings influenced by factors other than the product itself? Different cognitive biases may influence raters to rate or speak more highly of certain products over others. For example, we often ascribe more positive attributes to purchases we’ve made and this is known as post-purchase rationalization. In this project we want to analyze the Amazon review dataset in order to discover whether factors such as price, position in the sales ranking and time of the year in which a review is written can impact how customers perceive a product. We want to use this data to gain an insight on how these factors can play a role in user reviews.

# Research questions
1. Can we observe a periodic pattern in average user rating?
    * Does ice cream get better reviews in summer?
    * Is cold medicine reviewed more highly in winter?

2. Can we calculate a sentiment score based on the review text and is sentiment score related to user ratings?
    * Is a negative sentiment correlated to a lower user rating?

3. Are more expensive items rated higher? Is there choice supportive bias?

# Dataset
We are going to use the Amazon reviews dataset. Since this dataset is fairly large we expect that we will have to use the compute cluster to deal with it unless we decide to use a stripped down version. Each line in the data files is a JSON object and the files can be imported line by line using a for loop in Python. The dataset's page has some Python code examples that explain how to extract the data out of the raw files and import it into a Pandas DataFrame so we can use that to get started. Since the metadata about the product being reviewed (category ranking, price...) are in a separate dataset we will need to use Pandas' join functionality to combine them. We are going to use the overall score, the product's price, the product's category ranking, the review's text and its time in our project.
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

# Questions for TAa
Will there be limits on how often we're allowed to use the compute cluster?
Can we get temporal data on price and sales ranking of products?
Are we going in the right direction with this proposal? Do you think there are any other research subtopics we should consider?