package com.example.dynamodb.mapper;

import com.amazonaws.services.dynamodbv2.datamodeling.TransactionWriteRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodb.base.AbstractTest;
import com.example.dynamodb.mapper.model.Order;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;

/**
 * @description:
 * @author: LiuHu
 * @create: 2020/10/8
 **/
public class PutAndUpdateTest extends AbstractTest {

    @BeforeAll
    static void init() {
        // 创建 Reply 表
        CreateTableRequest createOrderTable = mapper.generateCreateTableRequest(Order.class);
        createOrderTable.setProvisionedThroughput(new ProvisionedThroughput(5L,5L));
        try {
            dynamoDB.createTable(createOrderTable);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * addUpdate() 会更新已有记录，如果记录不存在会增加
     * addPut() 会增加新记录，如果记录已存在会更新记录
     * 同一个记录，不能在事务中操作多次
     */
    @Test
    public void comparePutAndUpdate() {
//        Order order1 = new Order();
//        order1.setName("name1");
//        order1.setInfo("info");
//        order1.setCreateTime(new Date());
//        mapper.save(order1);

        Order order2 = new Order();
        order2.setId("123");
        order2.setName("name1");
        order2.setCreateTime(new Date());

        Order order3 = new Order();
        order3.setId("1234");
        order3.setName("name2");
        order3.setInfo("info");

        Order order4 = new Order();
        order4.setId("12345");
        order4.setName("name2");
        order4.setInfo("info");

        TransactionWriteRequest request = new TransactionWriteRequest()
                .addPut(order3)
                .addUpdate(order2)
                .addUpdate(order4);


        mapper.transactionWrite(request);
    }
}
