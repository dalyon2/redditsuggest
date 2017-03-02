# redditsuggest
<snippet>
## Subreddit Subcultures
Imagine you're a manager at a social network and you're worried about how your users are clustered. If they're too fragmented into small groups, they may feel isolated and not connected to enough variety of people and content to keep them interested. Inversely, if the communities are too large, they may encounter people and content that are too far from their core interests and become disengaged. Subreddit Subcultures will look through billions of user comments and tell you how your users were grouped during a given time period.

## Usage
1. Go to redditcluster.us
2. Select a year and month, then press submit
3. Explore the clusters with your mouse pointer to see which subreddits are clustered together

## Technology
Subreddit Subcultures is written in Pyspark. The data is ingested from S3 and filtered. Then PCA is applied to reduce the dimensionality and increase accuracy. Finally, KMeans is used with silhouette analysis to determine the ideal number of clusters. The clustering data is sent to a Redis database to be displayed using Flask and d3.js.

</snippet>
