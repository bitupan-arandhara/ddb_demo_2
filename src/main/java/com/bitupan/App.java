package com.bitupan;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Attribute;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.bitupan.EnvVars.EnvVars;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App 
{
    public static void main( String[] args )
    {
        AWSCredentialsProvider creds = new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(EnvVars.ACCESS_KEY,
                    EnvVars.SECRET_KEY)
        );
        AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard().withCredentials(creds).withRegion("ap-south-1").build();
        DynamoDB dynamoDB = new DynamoDB(ddbClient);
    
        Table table = dynamoDB.getTable("Transactions"); //transactionId, date, amount, type, customer
        

        //1. Putting An Item
        // HashMap<String, String> customer = new HashMap<String, String>();
        // customer.put("customerId", "c3");
        // customer.put("customerName", "bitu");
        // putItem(table, "t6", "20-12-2020", "PURCHASE", 15, customer);
    
        //2. Get Item
        //getItem(table, "t6", "20-12-2020");

        //3. Retrieve only a specific list of attributes using AttributesToGet
        // GetItemSpec spec = new GetItemSpec()
        //     .withPrimaryKey("transactionId", "t6", "date", "20-12-2020")
        //     .withAttributesToGet("amount", "type");
        // Item item = table.getItem(spec);
        // System.out.println(item.toJSONPretty());

        //4. Retreive only a specific list of attributs using ProjectionExpression
        GetItemSpec spec = new GetItemSpec()
            .withPrimaryKey("transactionId", "t6", "date", "20-12-2020");
        System.out.println(table.getItem(spec).toJSONPretty());
        System.out.println();
        spec.withProjectionExpression("customer.customerName");
        System.out.println(table.getItem(spec).toJSONPretty());
        System.out.println();
        spec.withProjectionExpression("transactionId, amount, customer.customerName");
        System.out.println(table.getItem(spec).toJSONPretty());

    }
    public static void putItem(Table table, String hashKeyValueString, String rangeKeyValueString, String type, int amount, HashMap<String, String> customer){
        //build item
        Item item = new Item()
            .withPrimaryKey("transactionId", hashKeyValueString, "date", rangeKeyValueString)
            .withString("type", type)
            .withInt("amount", amount)
            .withMap("customer", customer);
        //put item to the table
        PutItemOutcome outcome = table.putItem(item);
    }
    public static void getItem(Table table, String hashKeyValueString, String rangeKeyValueString){
        Item item = table.getItem("transactionId", hashKeyValueString, "date", rangeKeyValueString);
        //LETS PLAY WITH item

        //1. No of attributes
        //int  noOfAttr = item.numberOfAttributes();
        //System.out.println("Number of attributes: "+ noOfAttr);
        
        //2. get an attribute named "customer"
        //System.out.println(item.get("customer"));

        //3. convert the item to a map
        //Map<String, Object> map = item.asMap();
        //System.out.println(map);
        //System.out.println(map.get("customer"));

        //4. Get an attribute named "customer" as map
        //Map<String, Object> customerAsMap = item.getMap("customer");
        //System.out.println(customerAsMap.get("customerName"));

        //5. Get an attribute named "customer" as JSON string
        //System.out.println(item.getJSON("customer"));

        //6. get item as an Iterator 
        //System.out.println(item.attributes());
        // for (Map.Entry<String, Object> attribute : item.attributes()) {
        //     System.out.println(attribute);
        // }

        //7. Convert the item to a JSON String
        String itemAsJsonString = item.toJSONPretty();
        System.out.println(itemAsJsonString);
        // ObjectMapper mapper = new ObjectMapper();
        // try{
        //     JsonNode rootNode = mapper.readTree(itemAsJsonString);
        //     //System.out.println(rootNode);
        //     //rootNode.path("date");
        //     //System.out.println(rootNode.path("customer").path("customerName"));
        //     JsonNode customerAJsonNode = rootNode.path("customer");
        //     System.out.println(customerAJsonNode); 
        //     if(Integer.parseInt(rootNode.path("amount").toString())==15){
        //         System.out.println("true");
        //     }
        //     else System.out.println("false");
        // }
        // catch(Exception e){
        //     System.out.println(e);
        // }
    }
}
