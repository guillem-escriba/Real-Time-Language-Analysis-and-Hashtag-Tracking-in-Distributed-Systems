package edu.upf.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;
import edu.upf.model.HashTagCount;
import edu.upf.model.HashTagCountComparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

  final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
  final static String region = "us-east-1";
  

  @Override
  public void write(SimplifiedTweetWithHashtags h) {
    // TODO IMPLEMENT ME
    // get the credentials
    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                                .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(endpoint, region)
                                ).withCredentials(new ProfileCredentialsProvider("default")) // our user was default
                                .build();
    final DynamoDB dynamoDB = new DynamoDB(client); // creates the dynamo instance
    final Table dynamoDBTable = dynamoDB.getTable("LsdsTwitterHashtags"); // gets the table where we'll write

    List<String> hashtagList = h.getHashtags(); // list of hashtags
    String lang = h.getLanguage(); // language of the tweet
    if (hashtagList.isEmpty()!=true && lang!=null) { // checks if the hashtag is empty or language is null 
      List<Long> tweetId = List.of(h.getTweetId()); 
      
      NameMap nameMap = new NameMap() // map the names of the expression with its corresponding attributes
        .with("#counter", "counter") // counter in expression refers to attribute counter
        .with("#tweet_ids", "tweet_ids"); // "" "" to attribute tweet_ids
      
      ValueMap valueMap = new ValueMap() // maps the values of the expression with its corresponding values or variables
        .withNumber(":increment", 1)  // increment = 1
        .withNumber(":zero", 0) // zero = 0
        .withList(":empty_list", Collections.emptyList()) // empty_list = Collections.emptyList()
        .withList(":tweet_id", tweetId); // tweet_id = tweetId (LIST)
      
        
      for (String hashtag : hashtagList) { // for each hashtag
        PrimaryKey primaryKey = new PrimaryKey("hashtag", hashtag, "lang",lang); // set the primary and sort keys
        UpdateItemSpec updateItemSpec = new UpdateItemSpec() // create the properties of the update
        .withPrimaryKey(primaryKey) // previously defined primary key
        .withUpdateExpression("set #counter = if_not_exists(#counter, :zero) + :increment,"+ // expresion that checks if counter has already a value, if not it initializes to 0, otherwise it adds 1,
                                  "#tweet_ids = list_append(if_not_exists(#tweet_ids, :empty_list), :tweet_id)") // the same with tweet_ids, if exists, appends, otherwise, empty list.
        .withNameMap(nameMap) // assign previous maps
        .withValueMap(valueMap);

        dynamoDBTable.updateItem(updateItemSpec); // applies the update with previously defined features
      }
      
    }
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {

    // credentials
    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                                .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(endpoint, region)
                                ).withCredentials(new ProfileCredentialsProvider("default"))
                                .build();

    final DynamoDB dynamoDB = new DynamoDB(client);
    final Table dynamoDBTable = dynamoDB.getTable("LsdsTwitterHashtags");
    
    // mapping
    NameMap nameMap = new NameMap()
      .with("#lang", "lang");

    ValueMap valueMap = new ValueMap()
    .withString(":lang", lang);

    // takes all hashtags of a given language
    ScanSpec scanSpec = new ScanSpec()
      .withFilterExpression("#lang = :lang") // expression that compares the languages
      .withNameMap(nameMap) // previous maps
      .withValueMap(valueMap);

    ItemCollection<ScanOutcome> items = dynamoDBTable.scan(scanSpec); // takes the output as items
    List<HashTagCount> htList = new ArrayList<HashTagCount>();; // inits a List
    
    for (Item item : items) { // for each item
      String hashtag = item.get("hashtag").toString(); // get the hashtag as String
      String language = item.get("lang").toString(); // get the language as String
      Long counter = Long.parseLong(item.get("counter").toString()); // get the counter as String and parse to Long

      HashTagCount ht = new HashTagCount(hashtag, language, counter); // create an instance of HashTagCount 
      htList.add(ht); // append it to the List

    }
    Collections.sort(htList, new HashTagCountComparator()); // sort using the provided class
    Collections.reverse(htList); // since the provided class orders ascendently, we need to reverse it
    return htList; // returns the whole list of hashtags ordered
  }
}