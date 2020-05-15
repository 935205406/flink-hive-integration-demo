package com.example.flink;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.table.planner.expressions.*;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Date 5/13/2020 8:21 AM
 * @Created by 
 */
public class HiveWriteDemo {
    public static void main(String[] args) throws Exception {
        // flink 集成 hive，从1.9开始有官方集成，1.10开始支持hive的3.1.2版本
        // 但是文档不全，无论是改造并启动集群还是本地开发调试，都遇到很多问题
        // 如果单纯将hive作为sink，还是可以用的
        // 目前只能使用 Table&SQL API 来集成hive，只能用于 Batch 模式
        // flink1.9后，阿里贡献了Blink，优化了 Table&SQL，flink的Table Planner就有了两个版本OldPlanner&BlinkPlanner
        // flink对hive的支持是阿里贡献的，因此只能使用 BlinkPlanner
        // 而 BlinkPlanner 在使用时与 OldPlanner 不一样，且有一定局限性
        // 在我们的预计需求中： 将数据做转化，然后写入hive，是可以满足的
        // 这种简单的 ETL 不涉及复杂的业务逻辑处理，一些SQL不易操作的转化，可以写 UDF 实现（看api，目前仅支持 ScalarFunction UDF）
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 连接mysql，以mysql作为source，进行处理后，写入hive
        // 注册catalog与databse （此处注册是 InMemoryCatalog, 意为 catalog,database以及注册的table与udf都存在内存中，任务结束就消失，其他flink session无法获取）
        tableEnv.registerCatalog("arkMysql",new GenericInMemoryCatalog("arkMysql","arkDB"));
        // 使用该catalog
        tableEnv.useCatalog("arkMysql");
        // 使用该database
        tableEnv.useDatabase("arkDB");
        // 注册数据源表，该表的结构存放在 catalog=arkMysql, database=arkDB中，如果后面catalog变化了，要写全路径来访问该表 arkMysql.arkDB.arkTB
        tableEnv.sqlUpdate("CREATE TABLE arkTB (\n" +
                "  id INT,\n" +
                "  `date` INT,\n" +
                "  stat_type INT,\n" +
                "  date_type INT,\n" +
                "  `value` INT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc', \n" +
                "  'connector.property-version' = '1', \n" +
                "  \n" +
                "  'connector.url' = 'jdbc:mysql://xxx:3306/xxx',\n" +
                "  \n" +
                "  'connector.table' = 'tableName', \n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver', \n" +
                "\n" +
                "  'connector.username' = 'xxx',\n" +
                "  'connector.password' = 'xxx',\n" +
                // 数据读取设置，可选项
                "'connector.read.partition.column' = 'id',\n" +
                "  'connector.read.partition.num' = '1',\n" + // 一般设置为任务并发度
                "  'connector.read.partition.lower-bound' = '390', \n" +
                "  'connector.read.partition.upper-bound' = '430', \n" + // 相当于sql语句 id between x and y
                "  'connector.read.fetch-size' = '10'"+  // 每次从mysql获取10条
                ")");

        // 注册 UDF 验证，看API，使用BLINK目前只能注册 ScalarFunction 函数
        tableEnv.registerFunction("arkDateToStr",new MyUDF());
        // 这个UDF默认注册在当前 catalog.database 中，如果catalog变化了，也需要以全路径来访问该UDF

        Table dayResultTable = tableEnv.sqlQuery("select id,CAST(`value` AS VARCHAR) as val,'test' as val2,arkDateToStr(`date`) as dt " +
                "from arkMysql.arkDB.arkTB " +
                "where date_type = 3 " // AND `date` > 20200416
        );
        // 注册中间结果表
        tableEnv.createTemporaryView("arkMysql.arkDB.dayRT",dayResultTable);
        // flink 的 Table Api 和 SQL 可以混合使用
        // SQL 查询的结果得到的 Table 对象，可以使用Table的API继续操作
        // Table API操作后的结果可以再使用SQL继续操作，但是这个结果需要注册到 TableEnv 中

        // 使用 TableUtils 的 collectToList(Table) 方法，可以将结果存到java的list中，方便调试时查看结果。
        // 内部原理就是执行该flink任务，获取计算结果，转化为List
        // List<Row> rows = TableUtils.collectToList(dayResultTable);


        // 下面注册 hive catalog
        String name            = "myhive"; // catalog name
        String defaultDatabase = "test"; // database to use 这里写的是hive中真实存在的database，你要操作哪个表，就写哪个表所在的database
        String hiveConfDir     = "C:\\IDEA_WorkSpace\\flink-demo\\src\\main\\resources"; // a local path 本地开发调试
        //String hiveConfDir     = "/usr/local/hive/conf"; // a local path  远程部署执行任务时，注意打开这一行注释，注释掉上面的语句
        String version         = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        // 注册 hive catalog
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        // 切换当前catalog
        tableEnv.useCatalog("myhive");

        // 因为hive自己使用 mysql 来存储表信息，因此构造好 HiveCatalog 对象后，就可以知道 test 这个 database 中的表结构
        // 并且，所有注册到该 catalog.database （myhive.test） 的表，都会被写入 mysql 进行持久化（对比 InMemoryCatalog 存在内容）
        // 这样，注册在该 catalog.database 的表，结构是被共享的，所有flink任务，只要使用 hive 的该 catalog.database 就能自动获取到表信息
        // 实现了不同任务间，表结构的共享

        //tableEnv.loadModule(name, new HiveModule(version));

        // 测试flink hive集成的连通性
        //tableEnv.sqlUpdate("insert into test values (2, 'unique_bbb','bbb','2020-05-13')");
        // 测试静态的分区插入
        //tableEnv.sqlUpdate("insert into myhive.test.test partition (dt='2020-05-14') select * from arkMysql.arkDB.dayRT");
        // 测试动态的分区插入数据，哪个分区不指定，字段内容是哪个分区，该条内容就写入哪个分区
        tableEnv.sqlUpdate("insert into myhive.test.test select * from arkMysql.arkDB.dayRT");

        tableEnv.execute("test hive write");
    }
}
