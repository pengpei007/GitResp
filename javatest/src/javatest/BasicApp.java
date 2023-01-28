package javatest;
/*璇ヤ緥绋嬪疄鐜版彃鍏ユ暟鎹紝淇敼鏁版嵁锛屽垹闄ゆ暟鎹紝鏁版嵁鏌ヨ绛夊熀鏈搷浣溿��*/
import java.awt.Color;
import analysisplan.analysisplan;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.font.FontRenderContext;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import javax.imageio.ImageIO;
import javax.xml.bind.PrintConversionEvent;
public class BasicApp {
	// 瀹氫箟DM JDBC椹卞姩涓�
	String jdbcString = "dm.jdbc.driver.DmDriver";
	// 瀹氫箟DM URL杩炴帴涓�
//	String urlString = "jdbc:dm://192.168.80.133:5237?sessionTimeout=1&logDir=&logLevel=all&logFlushFreq=1"; //192.168.80.137:7236";
//	String urlString = "jdbc:dm://192.168.80.133:5237?logDir=F:\\\\workplace\\\\javatest&logLevel=all&logLevel=all&logFlushFreq=1"; //192.168.80.137:7236";
	String urlString = "jdbc:dm://192.168.80.133:5236?logDir=F:\\\\workplace\\\\javatest&logLevel=all&logFlushFreq=1"; //192.168.80.137:7236";

	//	String urlString = "jdbc:dm://dmrw?dmrw=(192.168.80.133:5236,192.168.80.134:5236)&rwSeparate=0&rwPercent=0&login_mode=(1)&logDir=F:\\workplace\\javatest&logLevel=all"; //192.168.80.137:7236";
	// 瀹氫箟杩炴帴鐢ㄦ埛鍚�
	String userName = "SYSDBA";
	// 瀹氫箟杩炴帴鐢ㄦ埛鍙ｄ护
	String password = "SYSDBA";
	// 瀹氫箟杩炴帴瀵硅薄
	Connection conn = null;
	/* 鍔犺浇JDBC椹卞姩绋嬪簭
	 * @throws SQLException 寮傚父 */
	public void loadJdbcDriver() throws SQLException {
		try {
			System.out.println("Loading JDBC Driver...");
			// 鍔犺浇JDBC椹卞姩绋嬪簭
			Class.forName(jdbcString);
		} catch (ClassNotFoundException e) {
			throw new SQLException("Load JDBC Driver Error : " + e.getMessage());
		} catch (Exception ex) {
			throw new SQLException("Load JDBC Driver Error : "
					+ ex.getMessage());
		}
	}
	/* 杩炴帴DM鏁版嵁搴�
	 * @throws SQLException 寮傚父 */
	public void connect() throws SQLException {
		try {
			System.out.println("Connecting to DM Server...");
			// 杩炴帴DM鏁版嵁搴�
			conn = DriverManager.getConnection(urlString, userName, password);
		} catch (SQLException e) {
			throw new SQLException("Connect to DM Server Error : "
					+ e.getMessage());
		}
	}
	/* 鍏抽棴杩炴帴 
	 * @throws SQLException 寮傚父 */
	public void disConnect() throws SQLException {
		try {
			// 鍏抽棴杩炴帴
			conn.close();
		} catch (SQLException e) {
			throw new SQLException("close connection error : " + e.getMessage());
		}
	}
	/* 寰�浜у搧淇℃伅琛ㄦ彃鍏ユ暟鎹�
	 * @throws SQLException 寮傚父 */
	public void insertTable() throws SQLException {
		// 鎻掑叆鏁版嵁璇彞
		String sql = "INSERT INTO production.product(name,author,publisher,publishtime,"
				+ "product_subcategoryid,productno,satetystocklevel,originalprice,nowprice,discount,"
				+ "description,photo,type,papertotal,wordtotal,sellstarttime,sellendtime) "
				+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		// 鍒涘缓璇彞瀵硅薄
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 涓哄弬鏁拌祴鍊�
		pstmt.setString(1, "涓夊浗婕斾箟");
		pstmt.setString(2, "缃楄疮涓�");
		pstmt.setString(3, "涓崕涔﹀眬");
		pstmt.setDate(4, Date.valueOf("2005-04-01"));
		pstmt.setInt(5, 4);
		pstmt.setString(6, "9787101046121");
		pstmt.setInt(7, 10);
		pstmt.setBigDecimal(8, new BigDecimal(19.0000));
		pstmt.setBigDecimal(9, new BigDecimal(15.2000));
		pstmt.setBigDecimal(10, new BigDecimal(8.0));
		pstmt.setString(11, "銆婁笁鍥芥紨涔夈�嬫槸涓浗绗竴閮ㄩ暱绡囩珷鍥炰綋灏忚锛屼腑鍥藉皬璇寸敱鐭瘒鍙戝睍鑷抽暱绡囩殑鍘熷洜涓庤涔︽湁鍏炽��");
		// 璁剧疆澶у瓧娈靛弬鏁� 
		try {
			// 鍒涘缓涓�涓浘鐗囩敤浜庢彃鍏ュぇ瀛楁
			String filePath = "C:\\Users\\ThinkPad\\Desktop\\寰俊鍥剧墖_20210912151300.png";
			CreateImage(filePath);
			File file = new File(filePath); 
			InputStream in = new BufferedInputStream(new FileInputStream(file)); 
			pstmt.setBinaryStream(12, in, (int) file.length());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			// 濡傛灉娌℃湁鍥剧墖璁剧疆涓篘ULL
			pstmt.setNull(12, java.sql.Types.BINARY);
		} catch (IOException e) {
		System.out.println(e.getMessage());
		} 
		pstmt.setString(13, "25");
		pstmt.setInt(14, 943);
		pstmt.setInt(15, 93000);
		pstmt.setDate(16, Date.valueOf("2006-03-20"));
		pstmt.setDate(17, Date.valueOf("1900-01-01"));
		// 鎵ц璇彞
		pstmt.executeUpdate();
		// 鍏抽棴璇彞
		pstmt.close();
	}
	/* 寰�娴嬭瘯琛ㄦ彃鍏ユ暟鎹嚜澧炲垪鏁版嵁
	 * @throws SQLException 寮傚父 */
	public void insertSeQTable() throws SQLException {
		// 鎻掑叆鏁版嵁璇彞
		String sql = "SET IDENTITY_INSERT FUT_BGL_CRZZL ON;"
				+"INSERT INTO FUT_BGL_CRZZL(c1, c2) "
				+ "VALUES(?,?);"
				;
		// 鍒涘缓璇彞瀵硅薄
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 涓哄弬鏁拌祴鍊�
		pstmt.setInt(1, 101);
		pstmt.setInt(2, 2);
		// 鎵ц璇彞
		pstmt.executeUpdate();
		// 鍏抽棴璇彞
		pstmt.close();
	}
	/* 鏌ヨ浜у搧淇℃伅琛�
	 * @throws SQLException 寮傚父 */
	public void queryProduct() throws SQLException {
		// 鏌ヨ璇彞
		String sql = "SELECT productid,name,author,description,photo FROM production.product WHERE productid=11";
		// 鍒涘缓璇彞瀵硅薄
		Statement stmt = conn.createStatement();
		
		// 鎵ц鏌ヨ
//		stmt.execute("explain for");
		ResultSet rs = stmt.executeQuery(sql);
		// 鏄剧ず缁撴灉闆�
		displayResultSet(rs);
//		stmt.execute("explain for off");
		// 鍏抽棴缁撴灉闆�
		rs.close();
		// 鍏抽棴璇彞
		stmt.close();
	} 
	
	/* 鑾峰彇鎵ц璁″垝
	 * @throws SQLException 寮傚父 */
	public void GetplanQueryProduct() throws SQLException {
		// 鏌ヨ璇彞
		String sql = "SELECT productid,name,author,description,photo FROM production.product WHERE productid=11;";
		String sqlplan;
		// 鑾峰彇鎵ц璁″垝
		 sqlplan = analysisplan.getExecutePlan(sql, conn);
		 System.out.print(sqlplan); 
//		while(rs.next())
//		{
//		    Object object = rs.getObject(1);
//		    // should log the query plan
////		    log.info("Query Plan {}  ", object);
//		    System.out.print(object.toString()); 
//		}
		// 鍏抽棴缁撴灉闆�
//		rs.close();
//		// 鍏抽棴璇彞
//		stmt.close();
	} 
	/* 淇敼浜у搧淇℃伅琛ㄦ暟鎹�
	 * @throws SQLException 寮傚父 */
	public void updateTable() throws SQLException {
		// 鏇存柊鏁版嵁璇彞
		String sql = "UPDATE production.product SET name = ?"
				+ "WHERE productid = 11;";
		// 鍒涘缓璇彞瀵硅薄
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 涓哄弬鏁拌祴鍊�
		pstmt.setString(1, "涓夊浗婕斾箟锛堜笂锛�");
		// 鎵ц璇彞
		pstmt.executeUpdate();
		// 鍏抽棴璇彞
		pstmt.close();
	}
	/* 鍒犻櫎浜у搧淇℃伅琛ㄦ暟鎹�
	 * @throws SQLException 寮傚父 */
	public void deleteTable() throws SQLException {
		// 鍒犻櫎鏁版嵁璇彞
		String sql = "DELETE FROM production.product WHERE productid = 11;";
		// 鍒涘缓璇彞瀵硅薄
		Statement stmt = conn.createStatement();
		// 鎵ц璇彞
		stmt.executeUpdate(sql);
		// 鍏抽棴璇彞
		stmt.close();
	}
	/* 鏌ヨ浜у搧淇℃伅琛�
	 * @throws SQLException 寮傚父 */
	public void queryTable() throws SQLException {
		// 鏌ヨ璇彞
//		String sql = "sleep(3);SELECT productid,name,author,publisher FROM production.product;";
		String sql ="select a from T1.test0826;";
		// 鍒涘缓璇彞瀵硅薄
		Statement stmt = conn.createStatement();
		// 鎵ц鏌ヨ
//		ResultSet rs = stmt.executeQuery(sql);
//		try {
//		 stmt.executeQuery(sql);
//		} catch (Exception e) {
//			// TODO: handle exception
//			throw new SQLException("chaxun : " + e.getMessage());
//		}
		ResultSet rs = stmt.executeQuery(sql);
		// 鏄剧ず缁撴灉闆�
		displayResultSet(rs);
		// 鍏抽棴缁撴灉闆�
		rs.close();
		// 鍏抽棴璇彞
		stmt.close();
	}
	
	/* dblink鏌ヨ琛�
	 * @throws SQLException 寮傚父 */
	public void queryTabledblink() throws SQLException {
		// 鏌ヨ璇彞
		String sql = "select  *\r\n"
				+ "from sysdba.test_dpi@LINK_DPI_80_137;";
		// 鍒涘缓璇彞瀵硅薄
		Statement stmt = conn.createStatement();
		// 鎵ц鏌ヨ
		ResultSet rs = stmt.executeQuery(sql);
		// 鏄剧ず缁撴灉闆�
		displayResultSetNoclob(rs);
		// 鍏抽棴缁撴灉闆�
		rs.close();
		// 鍏抽棴璇彞
		stmt.close();
	}
	/* 璋冪敤瀛樺偍杩囩▼淇敼浜у搧淇℃伅琛ㄦ暟鎹�
	 * @throws SQLException 寮傚父 */
	public void updateProduct() throws SQLException {
		// 鏇存柊鏁版嵁璇彞
		String sql = "{ CALL production.updateProduct(?,?) }";
		// 鍒涘缓璇彞瀵硅薄
		CallableStatement cstmt = conn.prepareCall(sql);
		// 涓哄弬鏁拌祴鍊�
		cstmt.setInt(1, 1);
		cstmt.setString(2, "绾㈡ゼ姊︼紙涓婏級");
		// 鎵ц璇彞
		cstmt.execute();
		// 鍏抽棴璇彞
		cstmt.close();
	}
	/* 鏄剧ず缁撴灉闆�
	 * @param rs 缁撴灉闆嗗璞�
	 * @throws SQLException 寮傚父 */
	private void displayResultSet(ResultSet rs) throws SQLException {
		// 鍙栧緱缁撴灉闆嗗厓鏁版嵁
		ResultSetMetaData rsmd = rs.getMetaData();
		// 鍙栧緱缁撴灉闆嗘墍鍖呭惈鐨勫垪鏁�
		int numCols = rsmd.getColumnCount();
		// 鏄剧ず鍒楁爣澶�
		for (int i = 1; i <= numCols; i++) {
			if (i > 1) {
				System.out.print(",");
			}
			System.out.print(rsmd.getColumnLabel(i));
		}
		System.out.println("");
		// 鏄剧ず缁撴灉闆嗕腑鎵�鏈夋暟鎹�
		while (rs.next()) {
			for (int i = 1; i <= numCols; i++) {
				if (i > 1) {
					System.out.print(",");
				}
				// 澶勭悊澶у瓧娈�
				if ("IMAGE".equals(rsmd.getColumnTypeName(i))) {
					byte[] data = rs.getBytes(i);
					if (data != null && data.length > 0) {
						FileOutputStream fos;
						try {
							fos = new FileOutputStream("c:\\涓夊浗婕斾箟1.jpg");
							fos.write(data);
							fos.close();
						} catch (FileNotFoundException e) {
							System.out.println(e.getMessage());
						} catch (IOException e) {
							System.out.println(e.getMessage());
						}
					}
		System.out.print("瀛楁鍐呭宸插啓鍏ユ枃浠禼:\\涓夊浗婕斾箟1.jpg锛岄暱搴�" + data.length);
				} else {
					// 鏅�氬瓧娈�
					System.out.print(rs.getString(i));
				}
			}
			System.out.println("");
		}
	}
	
	/* 鏄剧ず缁撴灉闆�
	 * @param rs 缁撴灉闆嗗璞�
	 * @throws SQLException 寮傚父 */
	private void displayResultSetNoclob(ResultSet rs) throws SQLException {
		// 鍙栧緱缁撴灉闆嗗厓鏁版嵁
		ResultSetMetaData rsmd = rs.getMetaData();
		// 鍙栧緱缁撴灉闆嗘墍鍖呭惈鐨勫垪鏁�
		int numCols = rsmd.getColumnCount();
		// 鏄剧ず鍒楁爣澶�
		for (int i = 1; i <= numCols; i++) {
			if (i > 1) {
				System.out.print(",");
			}
			System.out.print(rsmd.getColumnLabel(i));
		}
		System.out.println("");
		// 鏄剧ず缁撴灉闆嗕腑鎵�鏈夋暟鎹�
		while (rs.next()) {
			for (int i = 1; i <= numCols; i++) {
				if (i > 1) {
					System.out.print(",");
				}
				 
					// 鏅�氬瓧娈�
					System.out.print(rs.getString(i));
				
			}
			System.out.println("");
		}
	}
	 /* 鍒涘缓涓�涓浘鐗囩敤浜庢彃鍏ュぇ瀛楁
	 * @throws IOException 寮傚父 */
	private void CreateImage(String path) throws IOException {
		int width = 100;
		int height = 100;
		String s = "涓夊浗婕斾箟";
		File file = new File(path);
		Font font = new Font("Serif", Font.BOLD, 10);
		BufferedImage bi = new BufferedImage(width, height,
				BufferedImage.TYPE_INT_RGB);
		Graphics2D g2 = (Graphics2D) bi.getGraphics();
		g2.setBackground(Color.WHITE);
		g2.clearRect(0, 0, width, height);
		g2.setPaint(Color.RED);
		FontRenderContext context = g2.getFontRenderContext();
		Rectangle2D bounds = font.getStringBounds(s, context);
		double x = (width - bounds.getWidth()) / 2;
		double y = (height - bounds.getHeight()) / 2;
		double ascent = -bounds.getY();
		double baseY = y + ascent;
		g2.drawString(s, (int) x, (int) baseY);
		ImageIO.write(bi, "jpg", file);
	}
	
	 /* 鍒涘缓ResultSetMetaData瀵硅薄
	 * @throws IOException 寮傚父 */
	private void CreateResultSetMetaData() throws IOException, SQLException {	
		// 鍒涘缓璇彞瀵硅薄
	Statement stmt = conn.createStatement();
	ResultSet rs = stmt.executeQuery("SELECT * FROM T1");
	ResultSetMetaData rsmd = rs.getMetaData();
	for(int i = 1; i <= rsmd.getColumnCount(); i ++)
	{
	   String typeName = rsmd.getColumnTypeName(i);
	   System.out.println("绗�" + i + "鍒楃殑绫诲瀷涓猴細" + typeName);
	}
}
	/*
	 * 绫讳富鏂规硶 @param args 鍙傛暟
	 */
	public static void main(String args[]) throws IOException {
		try {
			// 瀹氫箟绫诲璞�
			BasicApp basicApp = new BasicApp();
			// 鍔犺浇椹卞姩绋嬪簭
			basicApp.loadJdbcDriver();
			System.out.println("--- aa浜у搧淇℃伅 ---");
			// 杩炴帴DM鏁版嵁搴�
			basicApp.connect();
			System.out.println("--- bb浜у搧淇℃伅 ---");
			// 鏄剧ず鍏冩暟鎹�	
			basicApp.CreateResultSetMetaData();
//			// 鎻掑叆鏁版嵁
//			System.out.println("--- 鎻掑叆浜у搧淇℃伅 ---");
//			basicApp.insertTable();
//			basicApp.insertSeQTable();
//			// 鏌ヨ鍚湁澶у瓧娈电殑浜у搧淇℃伅
//			System.out.println("--- 鏄剧ず鎻掑叆缁撴灉 ---");
//			basicApp.queryProduct();
			// 鑾峰彇鎵ц璁″垝
//			System.out.println("--- 鑾峰彇鎵ц璁″垝 ---");
//			basicApp.GetplanQueryProduct();			
			// dblink鏌ヨ鍚湁鏅�氬瓧娈电殑淇℃伅
//			System.out.println("--- 鏄剧ず鎻掑叆缁撴灉 ---");
//			basicApp.queryTabledblink();
			// 鍦ㄤ慨鏀瑰墠鏌ヨ浜у搧淇℃伅琛�
			System.out.println("--- 鍦ㄤ慨鏀瑰墠鏌ヨ浜у搧淇℃伅 ---");
			basicApp.queryTable();
//			// 淇敼浜у搧淇℃伅琛�
//			System.out.println("--- 淇敼浜у搧淇℃伅 ---");
//			basicApp.updateTable();
//			// 鍦ㄤ慨鏀瑰悗鏌ヨ浜у搧淇℃伅琛�
//			System.out.println("--- 鍦ㄤ慨鏀瑰悗鏌ヨ浜у搧淇℃伅 ---");
//			basicApp.queryTable();
//			// 鍒犻櫎浜у搧淇℃伅琛�
//			System.out.println("--- 鍒犻櫎浜у搧淇℃伅 ---");
//			basicApp.deleteTable();
//			// 鍦ㄥ垹闄ゅ悗鏌ヨ浜у搧淇℃伅琛�
//			System.out.println("--- 鍦ㄥ垹闄ゅ悗鏌ヨ浜у搧淇℃伅 ---");
//			basicApp.queryTable();
//			// 璋冪敤瀛樺偍杩囩▼淇敼浜у搧淇℃伅琛�
//			System.out.println("--- 璋冪敤瀛樺偍杩囩▼淇敼浜у搧淇℃伅 ---");
//			basicApp.updateProduct();
//			// 鍦ㄥ瓨鍌ㄨ繃绋嬫洿鏂板悗鏌ヨ浜у搧淇℃伅琛�
//			System.out.println("--- 璋冪敤瀛樺偍杩囩▼鍚庢煡璇骇鍝佷俊鎭� ---");
//			basicApp.queryTable();
			// 鍏抽棴杩炴帴
			basicApp.disConnect();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
}