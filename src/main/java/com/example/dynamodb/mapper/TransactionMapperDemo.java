package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.example.dynamodb.base.AbstractDemo;
import com.example.dynamodb.mapper.model.Forum;
import com.example.dynamodb.mapper.model.Thread;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/7
 **/
public class TransactionMapperDemo extends AbstractDemo {


    private static void testPutAndUpdateInTransactionWrite() {
        // Create new Forum item for S3 using save
        Forum s3Forum = new Forum();
        s3Forum.setName("S3 Forum");
        s3Forum.setCategory("Core Amazon Web Services");
        s3Forum.setThreads(0);
        mapper.save(s3Forum);

        // Update Forum item for S3 and Create new Forum item for DynamoDB using transactionWrite
        s3Forum.setCategory("Amazon Web Services");
        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(0);
        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        transactionWriteRequest.addUpdate(s3Forum);
        transactionWriteRequest.addPut(dynamodbForum);
        executeTransactionWrite(transactionWriteRequest);
    }


    private static void testPutWithConditionalUpdateInTransactionWrite() {
        // Create new Thread item for DynamoDB forum and update thread count in DynamoDB forum
        // if the DynamoDB Forum exists
        Thread dynamodbForumThread = new Thread();
        dynamodbForumThread.setForumName("DynamoDB Forum");
        dynamodbForumThread.setSubject("Sample Subject 1");
        dynamodbForumThread.setMessage("Sample Question 1");

        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(1);

        DynamoDBTransactionWriteExpression transactionWriteExpression = new DynamoDBTransactionWriteExpression()
                .withConditionExpression("attribute_exists(Category)");

        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        transactionWriteRequest.addPut(dynamodbForumThread);
        transactionWriteRequest.addUpdate(dynamodbForum, transactionWriteExpression);
        executeTransactionWrite(transactionWriteRequest);
    }


    private static void testTransactionLoadWithSave() {
        // Create new Forum item for DynamoDB using save
        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(0);
        mapper.save(dynamodbForum);

        // Add a thread to DynamoDB Forum
        Thread dynamodbForumThread = new Thread();
        dynamodbForumThread.setForumName("DynamoDB Forum");
        dynamodbForumThread.setSubject("Sample Subject 1");
        dynamodbForumThread.setMessage("Sample Question 1");
        mapper.save(dynamodbForumThread);

        // Update DynamoDB Forum to reflect updated thread count
        dynamodbForum.setThreads(1);
        mapper.save(dynamodbForum);


        // Read DynamoDB Forum item and Thread item at the same time in a serializable manner
        TransactionLoadRequest transactionLoadRequest = new TransactionLoadRequest();

        // Read entire item for DynamoDB Forum
        transactionLoadRequest.addLoad(dynamodbForum);

        // Only read subject and message attributes from Thread item
        DynamoDBTransactionLoadExpression loadExpressionForThread = new DynamoDBTransactionLoadExpression()
                .withProjectionExpression("Subject, Message");
        transactionLoadRequest.addLoad(dynamodbForumThread, loadExpressionForThread);

        // Loaded objects are guaranteed to be in same order as the order in which they are
        // added to TransactionLoadRequest
        List<Object> loadedObjects = executeTransactionLoad(transactionLoadRequest);
        Forum loadedDynamoDBForum = (Forum) loadedObjects.get(0);
        System.out.println("Forum: " + loadedDynamoDBForum.getName());
        System.out.println("Threads: " + loadedDynamoDBForum.getThreads());
        Thread loadedDynamodbForumThread = (Thread) loadedObjects.get(1);
        System.out.println("Subject: " + loadedDynamodbForumThread.getSubject());
        System.out.println("Message: " + loadedDynamodbForumThread.getMessage());
    }

    /**
     *
     */
    private static void testTransactionLoadWithTransactionWrite() {
        // Create new Forum item for DynamoDB using save
        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(0);
        mapper.save(dynamodbForum);

        // Update Forum item for DynamoDB and add a thread to DynamoDB Forum, in
        // an ACID manner using transactionWrite

        dynamodbForum.setThreads(1);
        Thread dynamodbForumThread = new Thread();
        dynamodbForumThread.setForumName("DynamoDB New Forum");
        dynamodbForumThread.setSubject("Sample Subject 2");
        dynamodbForumThread.setMessage("Sample Question 2");

        // 写事务
        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        transactionWriteRequest.addPut(dynamodbForumThread);
        transactionWriteRequest.addUpdate(dynamodbForum);
        executeTransactionWrite(transactionWriteRequest);


        // Read DynamoDB Forum item and Thread item at the same time in a serializable manner
        TransactionLoadRequest transactionLoadRequest = new TransactionLoadRequest();

        // Read entire item for DynamoDB Forum
        transactionLoadRequest.addLoad(dynamodbForum);

        // Only read subject and message attributes from Thread item
        DynamoDBTransactionLoadExpression loadExpressionForThread = new DynamoDBTransactionLoadExpression()
                .withProjectionExpression("Subject, Message");
        transactionLoadRequest.addLoad(dynamodbForumThread, loadExpressionForThread);

        // Loaded objects are guaranteed to be in same order as the order in which they are
        // added to TransactionLoadRequest
        List<Object> loadedObjects = executeTransactionLoad(transactionLoadRequest);
        Forum loadedDynamoDBForum = (Forum) loadedObjects.get(0);
        System.out.println("Forum: " + loadedDynamoDBForum.getName());
        System.out.println("Threads: " + loadedDynamoDBForum.getThreads());
        Thread loadedDynamodbForumThread = (Thread) loadedObjects.get(1);
        System.out.println("Subject: " + loadedDynamodbForumThread.getSubject());
        System.out.println("Message: " + loadedDynamodbForumThread.getMessage());
    }

    /**
     * 写事务，增加事务 Condition 检查
     */
    private static void testPutWithConditionCheckInTransactionWrite() {
        // Create new Thread item for DynamoDB forum and update thread count in DynamoDB forum if a thread already exists
        Thread dynamodbForumThread2 = new Thread();
        dynamodbForumThread2.setForumName("DynamoDB Forum");
        dynamodbForumThread2.setSubject("Sample Subject 2");
        dynamodbForumThread2.setMessage("Sample Question 2");

        Thread dynamodbForumThread1 = new Thread();
        dynamodbForumThread1.setForumName("DynamoDB Forum");
        dynamodbForumThread1.setSubject("Sample Subject 1");
        DynamoDBTransactionWriteExpression conditionExpressionForConditionCheck = new DynamoDBTransactionWriteExpression()
                .withConditionExpression("attribute_exists(Subject)");

        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(2);

        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        transactionWriteRequest.addPut(dynamodbForumThread2);
        transactionWriteRequest.addConditionCheck(dynamodbForumThread1, conditionExpressionForConditionCheck);
        transactionWriteRequest.addUpdate(dynamodbForum);
        executeTransactionWrite(transactionWriteRequest);
    }

    /**
     * 混合事务操作
     * 1. 创建 "S3" 论坛新的主题 "Sample Subject 1"
     * 2. 修改 "S3" 论坛主题为 "Amazon Web Services" 的主题个数为 1
     * 3. 删除 "DynamoDB" 论坛主题 "Sample Subject 1"
     * 4. 检查 "Sample Subject 2" 论坛主题是否存在
     * 5. 更新 "DynamoDB" 论坛，"Amazon Web Services" 主题个数为 1
     */
    private static void testMixedOperationsInTransactionWrite() {
        // Create new Thread item for S3 forum and delete "Sample Subject 1" Thread from DynamoDB forum if
        // "Sample Subject 2" Thread exists in DynamoDB forum
        Thread s3ForumThread = new Thread();
        s3ForumThread.setForumName("S3 Forum");
        s3ForumThread.setSubject("Sample Subject 1");
        s3ForumThread.setMessage("Sample Question 1");

        Forum s3Forum = new Forum();
        s3Forum.setName("S3 Forum");
        s3Forum.setCategory("Amazon Web Services");
        s3Forum.setThreads(1);


        Thread dynamodbForumThread1 = new Thread();
        dynamodbForumThread1.setForumName("DynamoDB Forum");
        dynamodbForumThread1.setSubject("Sample Subject 1");

        Thread dynamodbForumThread2 = new Thread();
        dynamodbForumThread2.setForumName("DynamoDB Forum");
        dynamodbForumThread2.setSubject("Sample Subject 2");
        DynamoDBTransactionWriteExpression conditionExpressionForConditionCheck = new DynamoDBTransactionWriteExpression()
                .withConditionExpression("attribute_exists(Subject)");

        Forum dynamodbForum = new Forum();
        dynamodbForum.setName("DynamoDB Forum");
        dynamodbForum.setCategory("Amazon Web Services");
        dynamodbForum.setThreads(1);


        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();
        transactionWriteRequest.addPut(s3ForumThread);
        transactionWriteRequest.addUpdate(s3Forum);
        transactionWriteRequest.addDelete(dynamodbForumThread1);
        transactionWriteRequest.addConditionCheck(dynamodbForumThread2, conditionExpressionForConditionCheck);
        transactionWriteRequest.addUpdate(dynamodbForum);
        executeTransactionWrite(transactionWriteRequest);
    }





    private static List<Object> executeTransactionLoad(TransactionLoadRequest transactionLoadRequest) {
        List<Object> loadedObjects = new ArrayList<>();
        try {
            loadedObjects = mapper.transactionLoad(transactionLoadRequest);
        } catch (DynamoDBMappingException ddbme) {
            System.err.println("Client side error in Mapper, fix before retrying. Error: " + ddbme.getMessage());
        } catch (ResourceNotFoundException rnfe) {
            System.err.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getMessage());
        } catch (InternalServerErrorException ise) {
            System.err.println("Internal Server Error, generally safe to retry with back-off. Error: " + ise.getMessage());
        } catch (TransactionCanceledException tce) {
            System.err.println("Transaction Canceled, implies a client issue, fix before retrying. Error: " + tce.getMessage());
        } catch (Exception ex) {
            System.err.println("An exception occurred, investigate and configure retry strategy. Error: " + ex.getMessage());
        }
        return loadedObjects;
    }

    private static void executeTransactionWrite(TransactionWriteRequest transactionWriteRequest) {
        try {
            mapper.transactionWrite(transactionWriteRequest);
        } catch (DynamoDBMappingException ddbme) {
            System.err.println("Client side error in Mapper, fix before retrying. Error: " + ddbme.getMessage());
        } catch (ResourceNotFoundException rnfe) {
            System.err.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getMessage());
        } catch (InternalServerErrorException ise) {
            System.err.println("Internal Server Error, generally safe to retry with back-off. Error: " + ise.getMessage());
        } catch (TransactionCanceledException tce) {
            System.err.println("Transaction Canceled, implies a client issue, fix before retrying. Error: " + tce.getMessage());
        } catch (Exception ex) {
            System.err.println("An exception occurred, investigate and configure retry strategy. Error: " + ex.getMessage());
        }
    }

}
