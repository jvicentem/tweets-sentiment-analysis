# tweets-sentiment-analysis
Tweets sentiment analysis using Python's MapReduce library MrJob.

This assignment has been made by Jon Kobeaga (https://github.com/jkobeaga) and me.

You can find a proper "readme" here https://github.com/jvicentem/tweets-sentiment-analysis/report/Practica.pdf but I'm afraid you'll have to learn Spanish first.

Anyway, this assignment shows the top 10 hashtags on Twitter and the "happiest" state in the USA according to word's sentiment score (https://github.com/fnielsen/afinn/blob/master/afinn/data/AFINN-en-165.txt) in each tweet. 

In case you want to try out this project, first change directory to mapreduce_jobs and then you need to make sure Python 3 and the dependencies in requirements.txt are installed in your computer. After that, you may run our work from the terminal: ```
python __init__.py -r local /pathToOneTweetInJsonFormatPerLine.json --file ../assets/AFINN-en-165.txt --file ./word_utils.py ```
In case you want run this in AWS Elastic MapReduce, add your AWS keys to tweets_mrjob.conf. Of course, feel free to customize the instance_type (http://aws.amazon.com/ec2/instance-types/) and num_core_instances values. Finally, type the next line on the terminal:  
```
python __init__.py -r emr s3://s3BucketName/<folder>/OneTweetInJsonFormatPerLine.json -c ./tweets_mrjob.conf --output-dir=s3://s3BucketName/nonExistingFolder
```

If you want to fetch tweets, you've got two options: use twitterstream.py or tweets_getter_USA.py. The latter is a modified version of the first one but only fetches tweets from USA which are written in English.
 
