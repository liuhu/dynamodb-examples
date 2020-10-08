package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodb.base.AbstractTest;
import com.example.dynamodb.mapper.model.Bicycle;
import com.example.dynamodb.mapper.model.Book;
import com.example.dynamodb.mapper.model.Reply;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/8
 **/
@DisplayName("条件查询")
public class QueryTest extends AbstractTest {

    @BeforeAll
    public static void init() {
        // 创建 Reply 表
        CreateTableRequest createReplyTable = mapper.generateCreateTableRequest(Reply.class);
        createReplyTable.setProvisionedThroughput(new ProvisionedThroughput(5L,5L));
        try {
            dynamoDB.createTable(createReplyTable);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        CreateTableRequest createBookTable = mapper.generateCreateTableRequest(Book.class);
        createBookTable.setProvisionedThroughput(new ProvisionedThroughput(5L,5L));
        try {
            dynamoDB.createTable(createBookTable);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        CreateTableRequest createBicycleTable = mapper.generateCreateTableRequest(Bicycle.class);
        createBicycleTable.setProvisionedThroughput(new ProvisionedThroughput(5L,5L));
        try {
            dynamoDB.createTable(createBicycleTable);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    //@AfterAll
    public static void destroy() {
        try {
            DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(Reply.class);
            client.deleteTable(deleteTableRequest);
        } catch (Exception e){
            System.out.println(e.getMessage());
        }

        try {
            DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(Book.class);
            client.deleteTable(deleteTableRequest);
        } catch (Exception e){
            System.out.println(e.getMessage());
        }

        try {
            DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(Bicycle.class);
            client.deleteTable(deleteTableRequest);
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @BeforeEach
    void initReplyData() {
        List<Reply> scanResult = mapper.scan(Reply.class, new DynamoDBScanExpression());
        if (!scanResult.isEmpty()) {
            return;
        }
        // 初始化 Reply 表数据
        Reply reply1 = new Reply();
        reply1.setId("aws#dynamodb");
        reply1.setReplyDateTime(dateFormat(1582992000000L));
        reply1.setMessage("Welcome to dynamodb world");
        reply1.setPostedBy("liu1");

        Reply reply2 = new Reply();
        reply2.setId("aws#dynamodb");
        reply2.setReplyDateTime(dateFormat(new Date().getTime()));
        reply2.setMessage("Welcome to dynamodb world");
        reply2.setPostedBy("liu2");

        Reply reply3 = new Reply();
        reply3.setId("aws#ec2");
        reply3.setReplyDateTime(dateFormat(new Date().getTime() - (14L * 24L * 60L * 60L * 1000L) + 300000));
        reply3.setMessage("Welcome to dynamodb world");
        reply3.setPostedBy("liu3");
        mapper.batchSave(Arrays.asList(reply1, reply2, reply3));
    }

    @BeforeEach
    void initBookData() {
        List<Book> bookResult = mapper.scan(Book.class, new DynamoDBScanExpression());
        if (!bookResult.isEmpty()) {
            return;
        }
        Book book1 = new Book();
        book1.setId(1);
        book1.setTitle("Dynamodb");
        book1.setISBN("ISBN-Dynamodb");
        book1.setPrice(10);
        book1.setPageCount(300);
        book1.setProductCategory("Book");
        book1.setInPublication(true);

        Book book2 = new Book();
        book2.setId(2);
        book2.setTitle("ec2");
        book2.setISBN("ISBN-ec2");
        book2.setPrice(234);
        book2.setPageCount(300);
        book2.setProductCategory("Book");
        book2.setInPublication(true);
        mapper.batchSave(Arrays.asList(book1, book2));
    }

    @BeforeEach
    void initBicycleData() {
        List<Bicycle> bookResult = mapper.scan(Bicycle.class, new DynamoDBScanExpression());
        if (!bookResult.isEmpty()) {
            return;
        }
        Bicycle bicycle1 = new Bicycle();
        bicycle1.setId(1);
        bicycle1.setTitle("公路自行车");
        bicycle1.setDescription("公路自行车详细介绍");
        bicycle1.setBicycleType("Road");
        bicycle1.setBrand("凤凰");
        bicycle1.setPrice(124);
        bicycle1.setColor(Arrays.asList("蓝","红"));
        bicycle1.setProductCategory("Bicycle");

        Bicycle bicycle2 = new Bicycle();
        bicycle2.setId(2);
        bicycle2.setTitle("山地自行车");
        bicycle2.setDescription("山地自行车详细介绍");
        bicycle2.setBicycleType("Mountain");
        bicycle2.setBrand("捷安特");
        bicycle2.setPrice(400);
        bicycle2.setColor(Arrays.asList("黄","绿"));
        bicycle2.setProductCategory("Bicycle");

        mapper.batchSave(Arrays.asList(bicycle1, bicycle2));
    }

    /**
     * 条件查询最近15天的回复数据
     * @param forumName
     * @param threadSubject
     */
    @ParameterizedTest(name = "{index} ==> forumName is ''{0}'' and  threadSubject is ''{1}''")
    @CsvSource({ "aws, dynamodb", "aws, ec2"})
    @DisplayName("query @ > ?")
    public void findRepliesInLast15Days(String forumName, String threadSubject) {
        String partitionKey = forumName + "#" + threadSubject;

        // 获取当前事件前15天的时间字符串
        long twoWeeksAgoMilli = (new Date()).getTime() - (15L * 24L * 60L * 60L * 1000L);
        String twoWeeksAgoStr = dateFormat(twoWeeksAgoMilli);

        // 构建查询属性值
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(partitionKey));
        eav.put(":val2", new AttributeValue().withS(twoWeeksAgoStr));

        // 条件语句
        DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
                .withKeyConditionExpression("Id = :val1 and ReplyDateTime > :val2")
                .withExpressionAttributeValues(eav);

        List<Reply> latestReplies = mapper.query(Reply.class, queryExpression);

        for (Reply reply : latestReplies) {
            System.out.format("Id=%s, Message=%s, PostedBy=%s, ReplyDateTime=%s %n", reply.getId(),
                    reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
        }
    }

    /**
     * 查询在某个时间段内的回复数据
     * @param forumName
     * @param threadSubject
     */
    @ParameterizedTest(name = "{index} ==> forumName is ''{0}'' and  threadSubject is ''{1}''")
    @CsvSource({ "aws, dynamodb", "aws, ec2"})
    @DisplayName("query @ between ? and ?")
    public void findRepliesPostedWithinTimePeriod(String forumName, String threadSubject) {
        String partitionKey = forumName + "#" + threadSubject;

        long startDateMilli = (new Date()).getTime() - (14L * 24L * 60L * 60L * 1000L); // Two weeks ago.
        long endDateMilli = (new Date()).getTime() - (7L * 24L * 60L * 60L * 1000L); // One week ago.

        String startDate = dateFormat(startDateMilli);
        String endDate = dateFormat(endDateMilli);

        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(partitionKey));
        eav.put(":val2", new AttributeValue().withS(startDate));
        eav.put(":val3", new AttributeValue().withS(endDate));

        // 条件语句
        DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
                .withKeyConditionExpression("Id = :val1 and ReplyDateTime between :val2 and :val3")
                .withExpressionAttributeValues(eav);

        List<Reply> betweenReplies = mapper.query(Reply.class, queryExpression);

        for (Reply reply : betweenReplies) {
            System.out.format("Id=%s, Message=%s, PostedBy=%s, PostedDateTime=%s %n", reply.getId(),
                    reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
        }
    }


    /**
     * 条件查询，价格 < value 的书籍
     * @param value
     */
    @ParameterizedTest(name = "{index} ==> book value is ''{0}''")
    @CsvSource({ "200", "10"})
    @DisplayName("scan @ < ?, 条件查询没有索引或主键的属性")
    public void findBooksPricedLessThanSpecifiedValue(String value) {

        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withN(value));
        eav.put(":val2", new AttributeValue().withS("Book"));

        // 条件语句
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("Price < :val1 and ProductCategory = :val2")
                .withExpressionAttributeValues(eav);

        List<Book> scanResult = mapper.scan(Book.class, scanExpression);

        for (Book book : scanResult) {
            System.out.println(book);
        }
    }

    /**
     * 并行扫描产品类型是 Bicycle 的指定类型的自行车
     * @param numberOfThreads 并行参数
     * @param bicycleType 自行车类型
     */
    @ParameterizedTest(name = "{index} ==> numberOfThreads is ''{0}'' and  bicycleType is ''{1}''")
    @CsvSource({ "1, Road", "2, Mountain"})
    @DisplayName("parallel scan")
    public void findBicyclesOfSpecifyTypeWithMultipleThreads(int numberOfThreads, String bicycleType) {
        System.out.println("FindBicyclesOfSpecificTypeWithMultipleThreads: Scan ProductCatalog With Multiple Threads.");
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS("Bicycle"));
        eav.put(":val2", new AttributeValue().withS(bicycleType));

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("ProductCategory = :val1 and BicycleType = :val2")
                .withExpressionAttributeValues(eav);

        // 条件语句
        // 并发参数
        List<Bicycle> scanResult = mapper.parallelScan(Bicycle.class, scanExpression, numberOfThreads);
        for (Bicycle bicycle : scanResult) {
            System.out.println(bicycle);
        }
    }

    /**
     * 时间格式化
     * @param dateTime
     * @return
     */
    private static String dateFormat(long dateTime) {
        Date date = new Date();
        date.setTime(dateTime);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormatter.format(date);
    }
}
