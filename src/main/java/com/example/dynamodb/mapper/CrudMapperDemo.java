package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodb.base.AbstractDemo;
import com.example.dynamodb.mapper.model.CatalogItem;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/6
 **/
public class CrudMapperDemo extends AbstractDemo {

    public static void main(String[] args) {
        //createTable();

//        createItem();
//        getItem();
//        updateItem();
//        retrieveItem();
//        deleteItem();

        deleteTable();
    }

    private static void createTable() {
        CreateTableRequest tableRequest = mapper.generateCreateTableRequest(CatalogItem.class);
        tableRequest.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        dynamoDB.createTable(tableRequest);
    }

    private static void deleteTable() {
        DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(CatalogItem.class);
        client.deleteTable(deleteTableRequest);
    }

    private static void createItem() {
        CatalogItem item = new CatalogItem();
        item.setId(601);
        item.setTitle("Book 601");
        item.setISBN("611-1111111111");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author1", "Author2")));
        mapper.save(item);
    }

    private static void getItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        System.out.println("Item get:");
        System.out.println(item);
    }

    private static void updateItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        item.setISBN("622-2222222222");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author1", "Author3")));
        mapper.save(item);
        System.out.println("Item updated:");
        System.out.println(item);
    }

    private static void retrieveItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        item.setISBN("633-3333333333333");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author4", "Author6")));
        mapper.save(item);

        CatalogItem item2 = mapper.load(CatalogItem.class, 601);
        System.out.println("Item retrieved:");
        System.out.println(item2);

        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        CatalogItem updatedItem = mapper.load(CatalogItem.class, 601, config);
        System.out.println("Retrieved the previously updated item:");
        System.out.println(updatedItem);
    }

    private static void deleteItem() {
        CatalogItem item = mapper.load(CatalogItem.class, 601);
        // Delete the item.
        mapper.delete(item);

        CatalogItem itemAgain = mapper.load(CatalogItem.class, 601);
        System.out.println("Item retrieved again:");
        System.out.println(itemAgain);

        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        // Try to retrieve deleted item.
        CatalogItem deletedItem = mapper.load(CatalogItem.class, item.getId(), config);
        if (deletedItem == null) {
            System.out.println("Done - Sample item is deleted.");
        }
    }
}
