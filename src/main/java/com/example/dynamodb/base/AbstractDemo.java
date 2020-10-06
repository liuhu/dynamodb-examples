package com.example.dynamodb.base;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/6
 **/
public abstract class AbstractDemo {

    protected static final String tableName = "Movies";
    protected static final DynamoDB dynamoDB;
    protected static final DynamoDBMapper mapper;
    protected static final AmazonDynamoDB client;


    static {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:18000", "us-west-2");
        client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        dynamoDB = new DynamoDB(client);
        mapper = new DynamoDBMapper(client);
    }
}
