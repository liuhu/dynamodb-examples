package com.example.dynamodb.simple;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.example.dynamodb.base.AbstractDemo;
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
public class SimpleDemo extends AbstractDemo {


    public static void main(String[] args) throws Exception {
//        createTable();
//        loadData();
//        createItem();
//        updateItem();

//        for (int i = 0; i < 3; i++) {
//            updateItem_AtomicCounter();
//        }


//        updateItem_Conditionally();
//        deleteItem();

//        queryItem();
        scanItem();

//        deleteTable();


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

        try (JsonParser parser = new JsonFactory().createParser(SimpleDemo.class.getClassLoader().getResourceAsStream("moviedata.json"))) {
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

    /**
     * 查询数据
     */
    private static void queryItem() {
        Table table = dynamoDB.getTable("Movies");

        // nameMap 提供名称替换功能。
        // 因为 year 是 Amazon DynamoDB 中的保留字, 不能直接在任何表达式中使用它, 用表达式属性名称 #yr 来解决此问题。
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        // valueMap 提供值替换功能。
        // 因为不能在任何表达式中使用文本。用表达式属性值 :yyyy 来解决此问题。
        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Movies from 1985");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": " + item.getString("title"));
            }

        }
        catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }


        valueMap.put(":yyyy", 1992);
        valueMap.put(":letter1", "A");
        valueMap.put(":letter2", "L");

        querySpec.withProjectionExpression("#yr, title, info.genres, info.actors[0]")
                .withKeyConditionExpression("#yr = :yyyy and title between :letter1 and :letter2")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        try {
            System.out.println("Movies from 1992 - titles A-L, with genres and lead actor");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": " + item.getString("title") + " " + item.getMap("info"));
            }

        }
        catch (Exception e) {
            System.err.println("Unable to query movies from 1992:");
            System.err.println(e.getMessage());
        }
    }


    @Deprecated
    private static void scanItem() {
        Table table = dynamoDB.getTable("Movies");

        ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression("#yr, title, info.rating")
                .withFilterExpression("#yr between :start_yr and :end_yr")
                .withNameMap(new NameMap().with("#yr", "year"))
                .withValueMap(new ValueMap()
                        .withNumber(":start_yr", 1950)
                        .withNumber(":end_yr", 1959));

        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);

            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                System.out.println(item.toString());
            }

        }
        catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    }
}
