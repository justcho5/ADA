# Title

# Abstract
A 150 word description of the project idea, goals, dataset used. What story you would like to tell and why? What's the motivation behind your project?

# Research questions
A list of research questions you would like to address during the project. 

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
Add here a sketch of your planning for the next project milestone.

# Questions for TAa
Add here some questions you have for us, in general or project-specific.
