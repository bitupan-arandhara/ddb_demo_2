package com.bitupan;

import java.util.HashMap;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.bitupan.EnvVars.EnvVars;

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

        HashMap<String, String> customer = new HashMap<String, String>();
        customer.put("customerId", "c3");
        customer.put("customerName", "bitu");

        putItem(table, "t6", "20-12-2020", "PURCHASE", 15, customer);
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
}
