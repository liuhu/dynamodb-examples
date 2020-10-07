package com.example.dynamodb.mapper.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;




/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
@DynamoDBTable(tableName = "Movies")
@Data
public class Movies {

    @DynamoDBHashKey(attributeName = "year")
    private Integer year;

    @DynamoDBRangeKey(attributeName = "title")
    private String title;

    @DynamoDBAttribute
    private String info;
}
