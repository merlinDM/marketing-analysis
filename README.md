## Marketing Analytics

The project is dedicated to building a marketing attribution projection for subsequent analysis of marketing campaigns and channels.

### Assumptions
* Event DS >> Purchase DS;
* App_open events might be missing => mechanism to distinguish sessions is needed. It is implemented using timeouts, by default timeouts are disabled;
* App_open events without attributes has null campaignId and channelId;
* Data is stored in Hive tables.

### Build
Java 1.8 is required. To build and run tests:
```bash
cd ./marketing-attribution
./gradlew build
```

### Deploy
You can submit application locally via following command:
```bash
./bin/spark-submit \
  --class "com.gd.App" \
  --master local[*] \
  ./build/libs/marketing-attribution-1.0.0-SNAPSHOT-all.jar
```
For cluster deployments make sure *-all.jar is available on all nodes. More information on how to deploy Spark applications you can find here:
https://spark.apache.org/docs/latest/submitting-applications.html

### Tables
Application reads data from:
* default.event
* default.purchase

Application writes attribution projection to the following location:
* default.attribution

Aggregates are stored as:
* default.top_campaign
* default.engagement

### TODOs:
* Read configuration from file;
* Utilize typesafe.scala-logging library;
* Add partitioning (by month?) and bucketing (by user) to events and purchases;
* Create more test cases, app_opens without attributes for example.
