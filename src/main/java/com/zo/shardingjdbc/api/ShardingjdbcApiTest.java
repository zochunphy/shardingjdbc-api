package com.zo.shardingjdbc.api;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * 基本的分库分表，
 * 如果不指定分片键，会路由到所有库表
 */
public class ShardingjdbcApiTest {

    public static void main(String[] args) throws SQLException {
        dbexecute();
    }

    public static void dbexecute() throws SQLException {

        //配置数据源
        Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
        BasicDataSource dataSource1 = new BasicDataSource();
        dataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource1.setUrl("jdbc:mysql://localhost:3306/ds00?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        dataSource1.setUsername("root");
        dataSource1.setPassword("root");
        dataSourceMap.put("ds00", dataSource1);

        BasicDataSource dataSource2 = new BasicDataSource();
        dataSource2.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource2.setUrl("jdbc:mysql://localhost:3306/ds01?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        dataSource2.setUsername("root");
        dataSource2.setPassword("root");
        dataSourceMap.put("ds01", dataSource2);

        // 配置 t_order 表规则
        ShardingTableRuleConfiguration orderTableRuleConfig = new
                ShardingTableRuleConfiguration("t_order", "ds0${0..1}.t_order_0${0..1}");
        // 配置分库策略
        orderTableRuleConfig.setDatabaseShardingStrategy(new
                StandardShardingStrategyConfiguration("user_id", "dbShardingAlgorithm"));
        // 配置分表策略
        orderTableRuleConfig.setTableShardingStrategy(new
                StandardShardingStrategyConfiguration("order_id", "tableShardingAlgorithm"));
        // 省略配置 t_order_item 表规则...
        // ...
        // 配置分⽚规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTables().add(orderTableRuleConfig);
        // 配置分库算法
        Properties dbShardingAlgorithmrProps = new Properties();
        dbShardingAlgorithmrProps.setProperty("algorithm-expression", "ds0${user_id % 2}");
        shardingRuleConfig.getShardingAlgorithms().put("dbShardingAlgorithm", new ShardingSphereAlgorithmConfiguration("INLINE", dbShardingAlgorithmrProps));
        // 配置分表算法
        Properties tableShardingAlgorithmrProps = new Properties();
        tableShardingAlgorithmrProps.setProperty("algorithm-expression", "t_order_0${order_id % 2 }");
        shardingRuleConfig.getShardingAlgorithms().put("tableShardingAlgorithm", new ShardingSphereAlgorithmConfiguration("INLINE", tableShardingAlgorithmrProps));
        List list = new ArrayList();
        list.add(shardingRuleConfig);
        //创建数据源
        DataSource dataSource = ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, list, new Properties());
        Connection connection = dataSource.getConnection();
        String sql = "insert into t_order(order_id,user_id,amount) values(?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, 10001);
        preparedStatement.setInt(2, 10002);
        preparedStatement.setString(3, "100");
        preparedStatement.execute();

        String querysql = "select * from t_order where amount >= '100'";

        PreparedStatement preparedStatement1 = connection.prepareStatement(querysql);
        ResultSet rs = preparedStatement1.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            int user_id = rs.getInt("user_id");
            int order_id = rs.getInt("order_id");
            String amount = rs.getString("amount");
            System.out.println("id:" + id + " user_id：" + user_id + " order_id：" + order_id + "amount:" + amount);
        }
    }
}
