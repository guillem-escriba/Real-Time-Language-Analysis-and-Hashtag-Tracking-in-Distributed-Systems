
# Real-Time Language Analysis and Hashtag Tracking in Distributed Systems

## Overview

This project focuses on analyzing language data from Mastodon streams, filtering and processing data in real-time, and implementing stateful and stateless operations for language analysis.

### EX3: LanguageMapUtils
- Filters out headers from `map.tsv`.
- Discards lines with less than 2 columns, focusing on rows with valid language codes and English names.
- Returns a pair RDD using `mapToPair`.

### MastodonStateless
- Transforms tweet streams into a pair stream with the language of the tweet and an initialized count.
- Joins with RDD from LanguageMap to group tweets by language, selecting the English name and tweet count.
- Performs operations like `reduceByKey`, `sortByKey` (in descending order), and prints the number of tweets per language.

#### Command:
```bash
spark-submit --class edu.upf.MastodonStateless target/lab3-mastodon-1.0-SNAPSHOT.jar ./src/main/resources/map.tsv --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties
```

#### Sample Output:
```
(English,410)
(German,70)
(French,52)
...
```

### EX4: MastodonWindows
- Similar to MastodonStateless, with additional operations on a windowed stream (60 seconds).
- Prints the top 15 languages from the stream.

#### Command:
```bash
spark-submit --class edu.upf.MastodonWindow target/lab3-mastodon-1.0-SNAPSHOT.jar ./src/main/resources/map.tsv --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties
```

#### Sample Output:
```
1st batch:
(English,287)
...

1st window:
(English,287)
...
```

### EX5: MastodonWithState
- Extends functionality to include language and user name in the final stream.
- Filters out pairs without usernames.
- Uses reduceByKey, swaps, and sortByKey operations.

#### Command:
```bash
spark-submit --class edu.upf.MastodonWithState target/lab3-mastodon-1.0-SNAPSHOT.jar ./src/main/resources/map.tsv en --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties
```

#### Sample Output:
```
1st batch:
(4,John Manoogian III)
...
```

### General Notes
- Despite error lines related to BigBone Exception in the command prompt, the output is unaffected.

### EXTRA: DynamoHashtagRepository
- Two main methods: `write(SimplifiedTweetWithHashtags h)` for mapping and updating items in DynamoDB, and `readTop10(String lang)` for retrieving the top 10 hashtags.
- The writer class fills the Dynamo table, while the reader prints the top 10 hashtags of a given language.

#### Sample Output for "en":
```
{"hashTag":"press","lang":"en","count":16}
...
```





