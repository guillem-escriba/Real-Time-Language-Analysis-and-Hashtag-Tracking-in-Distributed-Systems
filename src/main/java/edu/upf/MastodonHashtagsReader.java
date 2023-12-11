package edu.upf;

import java.util.ArrayList;
import java.util.List;

import edu.upf.model.HashTagCount;
import edu.upf.storage.DynamoHashTagRepository;

public class MastodonHashtagsReader {

    public static void main(String[] args){
        if (args.length > 0) { // checks the amount of parameters
            String lang = args[0]; // language to filter
            //String lang = "en";
            DynamoHashTagRepository dynamoHR = new DynamoHashTagRepository(); // creates an instance of the Dynamo
            List<HashTagCount> htList =  dynamoHR.readTop10(lang); // reads the list of hashtags
            List<HashTagCount> topN = new ArrayList<HashTagCount>(); // creates another list
            int top = 10; // amount of hashtags to take
            if(htList.size()<top){top = htList.size();} // if there are not enough hashtags
            for (int i = 0; i < top; i++) {
                HashTagCount ht = htList.get(i); 
                topN.add(ht); // appends the top 10 hashtags
                System.out.println(ht.toString()); // prints the top 10 hashtags
            }
        } else {
            System.out.println("Language parameter missing!");
        } 
    }
}