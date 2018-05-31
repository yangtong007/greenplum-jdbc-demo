package com.iflytek.edcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/12/1
 * Time: 15:18
 * Description
 */

public class GreenplumJdbcTest {

    public static void main(String[] args) throws Exception {

        System.out.println("加载驱动");
        Class.forName("com.pivotal.jdbc.GreenplumDriver");
        System.out.println("数据库连接");
        Connection db = DriverManager.getConnection(
                "jdbc:pivotal:greenplum://172.31.17.223:6432;DatabaseName=edu_edcc_dev",
                "xrding",
                "LgqYAsfLkxfCnNMr");
        System.out.println("数据库连接成功");

        Statement st = db.createStatement();

//        createTable(st);

//        insert(st);

        select(st);

        st.close();
    }

    //创建表
    private static void createTable(Statement st) throws Exception {
        //gp数据库表创建

        //Heap 或 Append-Only存储
        //GP默认使用堆表。堆表最好用在小表，如：维表(初始化后经常更新)
        //Append-Only表不能update和delete。一般用来做批量数据导入。 不建议单行插入。对于压缩表跟列存储来说，前提是必须是appendonly的表

        //DISTRIBUTED BY 分布键，分布键是按照这个字段值将表中的数据平均分布到每一个节点机器上，
        // 是从物理上把数据分散到各个SEGMENT上，这样更有利于并行查询。

        //分区是将一张大表按照分区的方式拆成N张小表,
        //是从逻辑上把一个大表分开，这样可以优化查询性能。分区是不会影响数据在各个SEGMENT上的分布情况的。
        //1. 范围分区 range partition range
        //2. 列表分区 list partition LIST
        //3. 组合分区

        //范围分区
        boolean result = st.execute("CREATE TABLE test_gp_create_table (\n" +
                "  id int,\n" +
                "  province_id varchar(10),\n" +
                "  statistics_date date \n" +
                ") \n" +
                "with (APPENDONLY=true,ORIENTATION=COLUMN)\n" +
                "DISTRIBUTED BY (statistics_date) \n" +
                "partition by range(statistics_date)\n" +
                "(start ( '2018-04-01') inclusive end ( '2018-04-02') exclusive every (interval '1 day'))");
        System.out.println(result);

        //列表分区
        result = st.execute("CREATE TABLE test_gp_create_table (\n" +
                "  id int,\n" +
                "  province_id varchar(10),\n" +
                "  statistics_date date \n" +
                ") \n" +
                "with (APPENDONLY=true,ORIENTATION=COLUMN)\n" +
                "DISTRIBUTED BY (statistics_date) \n" +
                "partition by list(statistics_date)\n" +
                "(" +
                "partition p1 values('2018-05-30')," +
                "partition p2 values('2018-05-30')" +
                ")");
        System.out.println(result);
    }

    /**
     * 插入数据
     * @param st
     * @throws Exception
     */
    private static void insert(Statement st) throws Exception {
        boolean result = st.execute("INSERT INTO test_gp_create_table (id,province_id,statistics_date) SELECT 1 as id,'1123' as province_id,'2018-05-31' as statistics_date");
        System.out.println(result);
    }

    /**
     * 查询数据
     * @param st
     * @throws Exception
     */
    private static void select(Statement st) throws Exception{
        ResultSet rs = st.executeQuery(
                "SELECT * FROM test_gp_create_table " +
                        "where statistics_date = '2018-04-30' " +
                        "limit 100");
        while (rs.next()) {
            System.out.println(rs.getString(2));
        }
        rs.close();
    }

}
