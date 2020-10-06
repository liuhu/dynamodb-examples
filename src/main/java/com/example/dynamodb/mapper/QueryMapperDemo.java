package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.example.dynamodb.base.AbstractDemo;
import com.example.dynamodb.mapper.model.Bicycle;
import com.example.dynamodb.mapper.model.Book;
import com.example.dynamodb.mapper.model.Reply;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/6
 **/
public class QueryMapperDemo extends AbstractDemo {


    /**
     * 根据主键查询
     * @param id
     */
    private static void findBookById(int id) {
        System.out.println("GetBook: Get book Id=" + id);
        System.out.println("Book table has no sort key. You can do GetItem, but not Query.");
        Book book = mapper.load(Book.class, id);
        System.out.format("Id = %s Title = %s, ISBN = %s %n", book.getId(), book.getTitle(), book.getISBN());
    }

    /**
     * 条件查询最近15天的回复数据
     * @param forumName
     * @param threadSubject
     */
    private static void findRepliesInLast15Days(String forumName, String threadSubject) {
        System.out.println("FindRepliesInLast15Days: Replies within last 15 days.");

        String partitionKey = forumName + "#" + threadSubject;

        // 获取当前事件前15天的时间字符串
        long twoWeeksAgoMilli = (new Date()).getTime() - (15L * 24L * 60L * 60L * 1000L);
        Date twoWeeksAgo = new Date();
        twoWeeksAgo.setTime(twoWeeksAgoMilli);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String twoWeeksAgoStr = dateFormatter.format(twoWeeksAgo);

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
            System.out.format("Id=%s, Message=%s, PostedBy=%s %n, ReplyDateTime=%s %n", reply.getId(),
                    reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
        }
    }

    /**
     * 查询在某个时间段内的回复数据
     * @param forumName
     * @param threadSubject
     */
    private static void findRepliesPostedWithinTimePeriod(String forumName, String threadSubject) {
        String partitionKey = forumName + "#" + threadSubject;

        System.out.println("FindRepliesPostedWithinTimePeriod: Find replies for thread Message = 'DynamoDB Thread 2' posted within a period.");

        long startDateMilli = (new Date()).getTime() - (14L * 24L * 60L * 60L * 1000L); // Two weeks ago.
        long endDateMilli = (new Date()).getTime() - (7L * 24L * 60L * 60L * 1000L); // One week ago.

        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String startDate = dateFormatter.format(startDateMilli);
        String endDate = dateFormatter.format(endDateMilli);

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
            System.out.format("Id=%s, Message=%s, PostedBy=%s %n, PostedDateTime=%s %n", reply.getId(),
                    reply.getMessage(), reply.getPostedBy(), reply.getReplyDateTime());
        }
    }


    /**
     * 条件查询，价格 < value 的书籍
     * @param value
     */
    private static void findBooksPricedLessThanSpecifiedValue(String value) {
        System.out.println("FindBooksPricedLessThanSpecifiedValue: Scan ProductCatalog.");

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
    private static void findBicyclesOfSpecifyTypeWithMultipleThreads(int numberOfThreads, String bicycleType) {
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
}
