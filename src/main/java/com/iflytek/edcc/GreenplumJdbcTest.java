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
                "jdbc:pivotal:greenplum://192.168.151.164:6432;DatabaseName=edu_edcc",
                "xrding",
                "LgqYAsfLkxfCnNMr");
        System.out.println("数据库连接成功");

        Statement st = db.createStatement();

//        createTable(st);

        select(st);

        st.close();
    }

    //创建表
    private static void createTable(Statement st) throws Exception {
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
    }

    /**
     * 查询数据
     * @param st
     * @throws Exception
     */
    private static void select(Statement st) throws Exception{
        ResultSet rs = st.executeQuery(
                "SELECT * FROM zx_knowledge_point_detail " +
                        "where statistics_date = '2018-04-30' " +
                        "limit 100");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
    }

}
