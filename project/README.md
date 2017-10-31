# Title

# Abstract
A 150 word description of the project idea, goals, dataset used. What story you would like to tell and why? What's the motivation behind your project?

Online product reviews are a major factor when purchasing products. Frequently, before purchasing an item, we assess the quality by looking at the star rating and reviews. But how much are the ratings influenced by product price, time of year, and sales rank? Different cognitive biases may influence raters to rate or speak more highly of certain products over others. For example, we often ascribe more positive attributes to purchases we’ve made and this is known as post-purchase rationalization. In addition, seasons of the year can affect our mood and this may in turn affect our review of products. Could we find a relationship between purchase price and the reviewer rating? Or does the season of the year affect how harshly we rate items? In order to understand more about how these factors influence reviewer ratings we can use the Amazon dataset which provides us with product reviews and information on product price, time when review was written, and the sales rank of the product. 

# Research questions
A list of research questions you would like to address during the project. 
1. Can we observe a periodic pattern in average user rating?
	a. For example: does ice cream get better reviews in summer? Is cold medicine reviewed more highly in winter?
2. Can we calculate a sentiment score based on the review text and is sentiment score related to user ratings?
	a. Is a negative sentiment correlated to a lower user rating?
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

List the dataset(s) you want to use, and some ideas on how do you expect to get, manage, process and enrich it/them. Show us you've read the docs and some examples, and you've a clear idea on what to expect. Discuss data size and format if relevant.

# A list of internal milestones up until project milestone 2

We plan our progress in weeks:
1. Get access to the dataset and clusters. Import the dataset into memory. Get familiar with the dataset.
2. Start to work with the dataset. Clean up the dataset (remove duplicates, fill in missing values, extract information of interest. Visualize the data, get an idea of its distribution and other interesting properties.
3. Study correlations between factors extracted from the dataset, get an idea of whether the correlations that we're trying to prove the existence of actually exist or if we need to adjust the scope of our project.
4. Document progress and plan for the next step.


# Questions for TAa
Add here some questions you have for us, in general or project-specific.
