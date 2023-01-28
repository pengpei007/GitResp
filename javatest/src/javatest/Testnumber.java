package javatest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class Testnumber {
	// 定义 DM JDBC 驱动串
	static String jdbcString = "dm.jdbc.driver.DmDriver";
	// 定义 DM URL 连接串
	static String urlString = "jdbc:dm://192.168.80.133:5236";
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
	public static void main(String[] args) {
		try {
			//1.加载 JDBC 驱动程序
			System.out.println("Loading JDBC Driver...");
			Class.forName(jdbcString);
			//2.连接 DM 数据库
			System.out.println("Connecting to DM Server...");
			conn = DriverManager.getConnection(urlString, userName, password);
			//3.通过连接对象创建 java.sql.Statement 对象
			state = conn.createStatement();
			System.out.println("connect successfull");
			//删测试表
			String sql_drop = "drop table if exists test_java0916";
			state.execute(sql_drop);
			System.out.println(" 删表成功");
			//建测试表
			String sql_create = "create table test_java0916(a numeric(10,0),b varchar(100))";
			state.execute(sql_create);
			System.out.println(" 建表成功");
			//增加记录 直接赋值
			String sql_insert = "insert into test_java0916(a,b) values(-1,'直接赋值新增记录')";
			//执行添加的 SQL 语句
			state.execute(sql_insert);
			System.out.println(" 直接赋值新增记录成功");
			//查询表中数据
			//定义查询 SQL
			String sql_selectAll = "select * from test_java0916 where a=-1";
			//执行查询的 SQL 语句
			rs = state.executeQuery(sql_selectAll);
			System.out.println("------查询语句直接赋值查询结果-------");
			displayResultSet(rs);
 
			//增加记录 变量赋值
			String sql_insertvar = "insert into test_java0916(a,b) values(?,'变量赋值新增记录')";
			PreparedStatement         pstate = conn.prepareStatement(sql_insertvar);
			pstate.setDouble(1, -1);
			pstate.executeUpdate();
			System.out.println(" 变量赋值新增记录成功");
			//查询表中数据
			String sql_queryvar="select * from test_java0916 where a=?";   
			PreparedStatement pstatequery=conn.prepareStatement(sql_queryvar); 
			pstatequery.setDouble(1, -1);
			ResultSet rs1 = pstatequery.executeQuery();
			System.out.println("------查询语句变量赋值查询结果-------");
			displayResultSet(rs1);


		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				//关闭资源
				rs.close();
				state.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	//显示结果集
	public static void displayResultSet(ResultSet rs) throws SQLException{
		while (rs.next()) {
			System.out.println("查询结果a="+rs.getString(1)+" b="+rs.getString(2));
		}
	}
}
