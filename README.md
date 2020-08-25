# SparkWithScala

This project contains Apache Spark(SQL) implementation based on Scala.
*  Scala version: 2.12.3 
*  Apache Spark version: 3.0.0
*  SBT version: 1.3.10

Make sure to replace data base url with valid value.

TwitterStream.scala consist of functions which contain logic to pull tweets from the twitter and will show the hashtags with occurrence of the same.
 
Make sure to add below keys values in `twitter4j.properties` under `src/main/resources`

    oauth.consumerKey=
    oauth.consumerSecret=
    oauth.accessToken=
    oauth.accessTokenSecret=

Visit [developer.twitter.com](https://developer.twitter.com) to get your own API keys.

Please refer build.sbt file for more details. 

### How to import

*  Clone the repository.
*  Import this project as sbt project.

### How to create package
*  Type `sbt clean package`

## Credits
* Samadhan
* Amit Dilip Thakur(thakuramitcomp@gmail.com)

