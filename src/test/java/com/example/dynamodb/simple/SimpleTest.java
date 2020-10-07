package com.example.dynamodb.simple;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.example.dynamodb.base.AbstractTestBase;
import com.example.dynamodb.mapper.model.Movies;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
@DisplayName("基础API")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimpleTest extends AbstractTestBase {

    private static final String tableName = "Movies";


    @BeforeEach
    void init() {
    }

    @Test
    @DisplayName("创建表")
    @Order(1)
    public void createTable() {
        try {
            List<KeySchemaElement> keySchema = Arrays.asList(
                    new KeySchemaElement("year", KeyType.HASH), // Partition
                    new KeySchemaElement("title", KeyType.RANGE)); // Sort key
            List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                    new AttributeDefinition("year", ScalarAttributeType.N),
                    new AttributeDefinition("title", ScalarAttributeType.S));
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(10L, 10L);

            Table table = dynamoDB.createTable(tableName, keySchema, attributeDefinitions, provisionedThroughput);
            table.waitForActive();

            System.out.println("创建表结果: ");
            System.out.println(table);

            Assertions.assertEquals("ACTIVE", table.getDescription().getTableStatus());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ResourceInUseException e) {
            // 表已存在忽略
            Assertions.assertEquals("Cannot create preexisting table", e.getErrorMessage());
        }
    }

    @Test
    @DisplayName("插入数据")
    @Order(2)
    public void createItem() {

        // 构造数据
        int year = 2015;
        String title = "The Big New Movie";
        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);

        // 构造 Item
        Item item = new Item()
                .withPrimaryKey("year", year, "title", title)
                .withMap("info", infoMap);

        // 插入数据
        // 如果根据主键数据已经存在，则会更覆盖原有数据
        Table table = dynamoDB.getTable(tableName);
        table.putItem(item);
    }

    @Test
    @DisplayName("根据主键读取数据")
    @Order(3)
    public void readItemByPk() {
        Table table = dynamoDB.getTable(tableName);
        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey("year", year, "title", title);

        System.out.println("Attempting to read the item...");
        Item outcome = table.getItem(spec);
        System.out.println("GetItem succeeded: " + outcome);

        Assertions.assertEquals(outcome.getLong("year"), 2015);
        Assertions.assertEquals(outcome.getString("title"), "The Big New Movie");
    }

    @Test
    @DisplayName("更新数据")
    @Order(4)
    public void updateItemByPk() {
        Table table = dynamoDB.getTable(tableName);

        // 构造数据
        int year = 2015;
        String title = "The Big New Movie";
        BigDecimal rating = BigDecimal.valueOf(5.5);
        String plot = "Everything happens all at once.";
        List<String> actors = Arrays.asList("Larry", "Moe", "Curly", "LiuHu");

        // 构造更新请求
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("year", year, "title", title)
                .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
                .withValueMap(new ValueMap()
                        .withNumber(":r", rating)
                        .withString(":p", plot)
                        .withList(":a", actors))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        // 执行更新
        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

        Assertions.assertEquals(outcome.getItem().getMap("info").get("rating"), rating);
        Assertions.assertEquals(outcome.getItem().getMap("info").get("plot"), plot);
        Assertions.assertEquals(outcome.getItem().getMap("info").get("actors"), actors);

    }


    /**
     * 更新数据, 原子计数器
     */
    @Test
    @DisplayName("更新数据, 原子计数器")
    @Order(5)
    protected void updateItemAtomicCounter() {
        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("year", year, "title", title)
                // 更新语句, 在原值基础上操作
                .withUpdateExpression("set info.rating = info.rating + :val")
                .withValueMap(new ValueMap().withNumber(":val", 1))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

        Assertions.assertEquals(outcome.getItem().getMap("info").get("rating"), new BigDecimal("6.5"));

    }

    /**
     * 更新数据带有条件
     * 更新数据，如果 Condition 条件没有符合数据，会抛出 ConditionalCheckFailedException 异常
     */
    @Test
    @DisplayName("更新数据, 带有条件")
    @Order(6)
    protected void updateItemConditionally() {
        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(new PrimaryKey("year", year, "title", title))
                // 更新语言, 移除一个元素
                .withUpdateExpression("remove info.actors[0]")
                // 条件语句
                .withConditionExpression("size(info.actors) >= :num")
                .withValueMap(new ValueMap().withNumber(":num", 3))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

        Assertions.assertEquals(outcome.getItem().getMap("info").get("actors"),  Arrays.asList("Moe", "Curly", "LiuHu"));
    }

    /**
     * 删除数据
     * 即使删除语言给定了主键，如果 Condition 条件不符合，会抛出 ConditionalCheckFailedException 异常，更新失败
     */
    @Test
    @DisplayName("删除数据, 带有条件")
    @Order(7)
    protected void deleteItemConditionally() {
        Table table = dynamoDB.getTable(tableName);

        int year = 2015;
        String title = "The Big New Movie";

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(new PrimaryKey("year", year, "title", title))
                .withConditionExpression("info.rating >= :val")
                .withValueMap(new ValueMap().withNumber(":val", 1.0));

        table.deleteItem(deleteItemSpec);
    }


    /**
     * 装载数据
     */
    @Test
    @DisplayName("装载数据")
    @Order(8)
    protected void loadData() throws IOException {
        Table table = dynamoDB.getTable(tableName);

        try (JsonParser parser = new JsonFactory().createParser(SimpleTest.class.getClassLoader().getResourceAsStream("moviedata.json"))) {
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();
            ObjectNode currentNode;

            List<Movies> movies = new ArrayList<>();

            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();

                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText();

                try {
                    Item item = new Item()
                            .withPrimaryKey("year", year, "title", title)
                            .withJSON("info", currentNode.path("info").toString());
                    table.putItem(item);
//                    Movies movie = new Movies();
//                    movie.setTitle(title);
//                    movie.setYear(year);
//                    movie.setInfo(currentNode.path("info").toString());
//                    movies.add(movie);
                } catch (Exception e) {
                    System.err.println("Unable to add movie: " + year + " " + title);
                    System.err.println(e.getMessage());
                    break;
                }
            }
            mapper.batchSave(movies);
        }
    }

    /**
     * 查询数据
     */
    @Test
    @DisplayName("Query条件查询")
    @Order(9)
    protected void queryItem() {
        Table table = dynamoDB.getTable(tableName);

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

        } catch (Exception e) {
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

        } catch (Exception e) {
            System.err.println("Unable to query movies from 1992:");
            System.err.println(e.getMessage());
        }
    }


    @Test
    @DisplayName("Scan 条件扫描")
    @Order(9)
    protected void scanItem() {
        Table table = dynamoDB.getTable(tableName);

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

        } catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    }


    @Test
    @DisplayName("删除表")
    @Order(Order.DEFAULT)
    public void deleteTable() {
        try {
            DeleteTableResult result = dynamoDB.getTable(tableName).delete();
            System.out.println("删除表结果: ");
            System.out.println(result);
        } catch (ResourceNotFoundException e) {
            // 表不存在忽略
            Assertions.assertEquals("Cannot do operations on a non-existent table", e.getErrorMessage());
        }
    }
}
