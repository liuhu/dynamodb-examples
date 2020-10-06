package com.example.dynamodb.demo;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.*;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/6
 **/
public class TableDemo {

    private static final String tableName = "Movies";
    private static final DynamoDB dynamoDB;

    static {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:18000", "us-west-2");
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        dynamoDB = new DynamoDB(client);
    }

    public static void main(String[] args) throws Exception {
//        createTable();
//        loadData();
//        createItem();
//        updateItem();

//        for (int i = 0; i < 3; i++) {
//            updateItem_AtomicCounter();
//        }


//        updateItem_Conditionally();
        deleteItem();
//         deleteTable();
    }

    /**
     * 创建表
     * @throws InterruptedException
     */
    private static void createTable() throws InterruptedException {
        List<KeySchemaElement> keySchema = Arrays.asList(
                new KeySchemaElement("year", KeyType.HASH), // Partition
                new KeySchemaElement("title", KeyType.RANGE)); // Sort key
        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition("year", ScalarAttributeType.N),
                new AttributeDefinition("title", ScalarAttributeType.S));
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(10L, 10L);

        Table table = dynamoDB.createTable(tableName, keySchema, attributeDefinitions, provisionedThroughput);
        table.waitForActive();

        System.out.println(table);
    }

    /**
     * 删除表
     */
    private static void deleteTable() {
        DeleteTableResult result = dynamoDB.getTable(tableName).delete();
        System.out.println(result);
    }

    /**
     * 装载数据
     *
     * @throws IOException
     */
    private static void loadData() throws IOException {
        Table table = dynamoDB.getTable(tableName);

        try (JsonParser parser = new JsonFactory().createParser(TableDemo.class.getClassLoader().getResourceAsStream("moviedata.json"))) {
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();
            ObjectNode currentNode;

            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();

                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText();

                try {
                    Item item = new Item()
                            .withPrimaryKey("year", year, "title", title)
                            .withJSON("info", currentNode.path("info").toString());
                    table.putItem(item);
                    System.out.println("PutItem succeeded: " + year + " " + title);

                } catch (Exception e) {
                    System.err.println("Unable to add movie: " + year + " " + title);
                    System.err.println(e.getMessage());
                    break;
                }
            }
        }
    }

    /**
     * 插入数据
     */
    private static void createItem() {
        Table table = dynamoDB.getTable("Movies");

        // 构造数据
        int year = 2015;
        String title = "The Big New Movie";
        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);

        try {
            System.out.println("Adding a new item...");

            Item item = new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap);
            PutItemOutcome outcome = table.putItem(item);

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        } catch (Exception e) {
            System.err.println("Unable to add item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }

    /**
     * 读取数据
     */
    private static void readItem() {
        Table table = dynamoDB.getTable("Movies");
        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec().withPrimaryKey("year", year, "title", title);

        System.out.println("Attempting to read the item...");
        Item outcome = table.getItem(spec);
        System.out.println("GetItem succeeded: " + outcome);

    }

    /**
     * 更新数据
     */
    private static void updateItem() {
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("year", year, "title", title)
                .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
                .withValueMap(new ValueMap()
                        .withNumber(":r", 5.5)
                        .withString(":p", "Everything happens all at once.")
                        .withList(":a", Arrays.asList("Larry", "Moe", "Curly","LiuHu")))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        System.out.println("Updating the item...");
        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
    }

    /**
     * 更新数据, 原子计数器
     */
    private static void updateItem_AtomicCounter() {
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("year", year, "title", title)
                .withUpdateExpression("set info.rating = info.rating + :val")
                .withValueMap(new ValueMap().withNumber(":val", 1))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        System.out.println("Incrementing an atomic counter...");
        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
    }

    /**
     * 更新数据带有条件
     */
    private static void updateItem_Conditionally() {
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(new PrimaryKey("year", year, "title", title))
                .withUpdateExpression("remove info.actors[0]")
                .withConditionExpression("size(info.actors) >= :num")
                .withValueMap(new ValueMap().withNumber(":num", 3))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        System.out.println("Attempting a conditional update...");
        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
    }

    /**
     * 删除数据
     */
    private static void deleteItem() {
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(new PrimaryKey("year", year, "title", title))
                .withConditionExpression("info.rating >= :val")
                .withValueMap(new ValueMap().withNumber(":val", 1.0));

        System.out.println("Attempting a conditional delete...");
        table.deleteItem(deleteItemSpec);
        System.out.println("DeleteItem succeeded");

    }
}
