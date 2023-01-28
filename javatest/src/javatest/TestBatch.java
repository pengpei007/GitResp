package javatest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.PreparedStatement;

public class TestBatch {

		// 定义 DM JDBC 驱动串
		static String jdbcString = "dm.jdbc.driver.DmDriver";
		// 定义 DM URL 连接串
		static String urlString = "jdbc:dm://192.168.80.133:5236?logLevel=all&logdir=E:\\jdbclog&batchType=2";
		// 定义连接用户名
		static String userName = "SYSDBA";
		// 定义连接用户口令
		static String password = "SYSDBA";
		// 定义连接对象
		static Connection conn = null;
		// 定义 SQL 语句执行对象
		static Statement state = null;
		// 定义结果集对象
		static ResultSet rs = null;

		public static void main(String[] args) throws InterruptedException {
			try {
				// 1.加载 JDBC 驱动程序
				System.out.println("Loading JDBC Driver...");
				Class.forName(jdbcString);
				// 2.连接 DM 数据库
				System.out.println("Connecting to DM Server...");
				conn = DriverManager.getConnection(urlString, userName, password);
				System.out.println("Connected");
				// 3.通过连接对象创建 java.sql.Statement 对象
				//构建测试表
				 String sql_ini="begin\r\n"
				 		+ "execute IMMEDIATE 'drop table if exists table_batch0118';\r\n"
				 		+ "execute IMMEDIATE 'create table table_batch0118(a int)';\r\n"
				 		+ "end;";
			 
				PreparedStatement stmtc=conn.prepareStatement(sql_ini);
				stmtc.execute();
				stmtc.close();
//使用批量绑定方法，填充测试数据
				String sql="insert into table_batch0118 values(?)";
				PreparedStatement pstmStatement=conn.prepareStatement(sql);
				for (int i = 0; i < 5; i++) {

						pstmStatement.setInt(1, i);
						pstmStatement.addBatch();	
//						Thread.sleep(10*1000);
				}

				int[] count=pstmStatement.executeBatch();
				System.out.println("绑定次数："+count.length);
				for (int i = 0; i < count.length; i++) {
					System.out.println("第"+i+"次，影响行数："+count[i]);
				};
				pstmStatement.close();
				//更新5条记录语句
//				sql="update table_batch0118 set a=? ";
//				PreparedStatement pstmu=conn.prepareStatement(sql);
//				pstmu.setInt(1,99);
//				pstmu.addBatch();
//				int[] count1=pstmu.executeBatch();
//				System.out.println("绑定次数："+count1.length);
//				for (int i = 0; i < count1.length; i++) {
//					System.out.println("第"+i+"次，影响行数："+count1[i]);
//				};
//				pstmu.close();
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					// 关闭资源
//					rs.close();
//					state.close();
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
}



 
