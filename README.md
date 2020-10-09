# DynamoDB 小例子
* 参考文档
  * [官方文档](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)
* 总结文档
  * [《DynamoDB 基础概念》](https://www.yuque.com/docs/share/221490bb-4e96-4706-a39b-00ba979b3a54)

## 说明

测试代码在 `test` 目录下，通过 JUnit 驱动测试用例执行。

具体结构如下：

``` 
.
|____test
| |____resources
| | |____moviedata.json                     -- 测试数据
| |____java
| | |____com
| | | |____example
| | | | |____dynamodb
| | | | | |____mapper                       -- DBMapper 高级 API 的测试用例
| | | | | | |____TransactionTest.java
| | | | | | |____CrudTest.java
| | | | | | |____QueryTest.java
| | | | | | |____ConvertTest.java
| | | | | | |____PutAndUpdateTest.java
| | | | | |____simple                       -- 基础 API 的测试用例
| | | | | | |____SimpleTest.java
| | | | | |____base                         -- 封装了
| | | | | | |____AbstractTest.java    
|____main
| |____resources
| | |____moviedata.json
| |____java
| | |____com
| | | |____example
| | | | |____dynamodb
| | | | | |____mapper
| | | | | | |____model                       -- 表结构定义
```
