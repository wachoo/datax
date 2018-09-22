package com.apus.datax;

import java.sql.*;

/**
 * @desc:
 * @author: YanMeng
 * @date: 18-8-9
 */
public class HiveJdbcTest {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection("jdbc:hive2://bigdata-gw001.eq-sg-2.apus.com:10000/apus_dws", "yanmeng", "abc");
		//Statement stmt = con.createStatement();

		Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(0);

//		ResultSet res0 = stmt.executeQuery("SELECT count(1) from dws_biz_search_model_day where dt = '2018-07-24'");
//		while (res0.next()) {
//			System.out.println(res0.getString(1));
//		}
		ResultSet res = stmt.executeQuery("select event_name_s from dws_biz_search_model_day where dt = '2018-07-24' limit 10");
		while (res.next()) {
			System.out.println(res.getString(1));
		}

	}
}
