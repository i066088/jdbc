//STEP 1. Import required packages
package mypkg;
import java.sql.*;

public class JdbcAPIPerfTest {
	//schema 
	static final String SCHEMA_NAME = "SBODEMOUS";
	
   // HANA JDBC
   static final String HANA_JDBC_DRIVER = "com.sap.db.jdbc.Driver";  
   static final String HANA_DB_URL = "jdbc:sap://hanaserver:30015?reconnect=true";
   static final String HANA_USER = "SYSTEM";
   static final String HANA_PASSWORD = "xxxx";
   
   //MSSQL JDBC
   static final String MSSQL_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";  
   static final String MSSQL_DB_URL = "jdbc:sqlserver://localhost:1433";
   static final String MSSQL_USER = "sa";
   static final String MSSQL_PASSWORD = "xxxx";
   
   static final String direct_select_sql = "SELECT USERID FROM OUSR WHERE \"USER_CODE\" = 'Workflow'";
   static final String prepare_select_sql = "SELECT USERID FROM OUSR WHERE \"USER_CODE\" = ?";
   
   //Control parameter
   static final int LOOP_NUMBER = 1000;
   static final Boolean m_bOutputResult = false;
   
   static{
	   try{
		   Class.forName(MSSQL_JDBC_DRIVER);
		   
	   } catch (ClassNotFoundException e) {
		   e.printStackTrace();
	}finally{
		   System.out.println("MSSQL_JDBC_DRIVER Load OK");
	   }
   }
   
   static{
	   try{
		   Class.forName(HANA_JDBC_DRIVER);
	   } catch (ClassNotFoundException e) {
		   e.printStackTrace();
	}finally{
		   System.out.println("HANA_JDBC_DRIVER Load OK");
	   }
   }
   
   static void direct_exect_query_test(String dbURL, String user, String password, String initSQL){
	   Connection conn = null;
	   Statement stmt = null;
	   long executeQueryDuration = 0;
	   long closeStatementDuration = 0;
	   try{
	      conn = DriverManager.getConnection(dbURL, user, password);
	      conn.createStatement().executeUpdate(initSQL);
	     
	      long startTime = 0;
	      for(int i = 0; i < LOOP_NUMBER; ++i){
	    	  stmt = conn.createStatement();
		     
		      startTime = System.nanoTime();    
		      ResultSet rs = stmt.executeQuery(direct_select_sql);
		      executeQueryDuration += (System.nanoTime() - startTime);
		      
		      while(rs.next()){
			         String userID = rs.getString("USERID");
			         if(m_bOutputResult)
			        	 System.out.println("userID = " + userID);
			  }
			  rs.close();
			 
			  startTime = System.nanoTime();  
			  stmt.close();
			  closeStatementDuration += (System.nanoTime() - startTime);
	      }
	   }catch(SQLException se){
	      se.printStackTrace();//Handle errors for JDBC
	   }catch(Exception e){
	      e.printStackTrace();//Handle errors for Class.forName
	   }finally{
	      try{
	         if(stmt!=null)
	        	 stmt.close();
	      }catch(SQLException se){
	      }
	      try{
	         if(conn!=null)
	            conn.close();
	      }catch(SQLException se){
	         se.printStackTrace();
	      }
	   }  
	   System.out.println("Statement.executeQuery Duration = " + executeQueryDuration);
	   System.out.println("Statement.close Duration = " + closeStatementDuration);
	}
   
   static void prepare_exect_query_test(String dbURL, String user, String password, String initSQL){
	   Connection conn = null;
	 
	   PreparedStatement ps = null;
	   long prepareStatementDuration = 0;
	   long executeQueryDuration = 0;
	   long prepareStatementCloseDuration = 0;
	   try{	      
	      conn = DriverManager.getConnection(dbURL, user, password);
	      conn.createStatement().executeUpdate(initSQL);
	      
	      long startTime = 0;
	      for(int i = 0; i < LOOP_NUMBER; ++i){
		      startTime = System.nanoTime();
		      ps = conn.prepareStatement(prepare_select_sql);
		      prepareStatementDuration += (System.nanoTime() - startTime);
		     
		      ps.setString(1, "Workflow");
		      
		      startTime = System.nanoTime();
		      ResultSet rs = ps.executeQuery();
		      executeQueryDuration += (System.nanoTime() - startTime);
		     
		      while(rs.next()){
		         String userID = rs.getString("USERID");
		         if(m_bOutputResult)
		        	 System.out.println("userID: " + userID);
		      }
		      rs.close();
		      
		      startTime = System.nanoTime();
		      ps.close();
		      prepareStatementCloseDuration += (System.nanoTime() - startTime);
			  ps = null;
	      }
	   }catch(SQLException se){
	      se.printStackTrace();//Handle errors for JDBC
	   }catch(Exception e){
	      e.printStackTrace(); //Handle errors for Class.forName
	   }finally{
	      //finally block used to close resources
	      try{
	         if(ps!=null)
	        	 ps.close();
	      }catch(SQLException se){
	      }
	      try{
	         if(conn!=null)
	            conn.close();
	      }catch(SQLException se){
	         se.printStackTrace();
	      }
	   }
	   System.out.println("Connection.prepareStatement Duration = " + prepareStatementDuration);
	   System.out.println("PreparedStatement.executeQuery Duration = " + executeQueryDuration);
	   System.out.println("PreparedStatement.close Duration = " + prepareStatementCloseDuration);
	}
   
   public static void main(String[] args) {
	   System.out.println();
	   System.out.println("***********************HANA JDBC API Performance***********************");
	   direct_exect_query_test(HANA_DB_URL, HANA_USER, HANA_PASSWORD, String.format("set schema %s", SCHEMA_NAME));
	   prepare_exect_query_test(HANA_DB_URL, HANA_USER, HANA_PASSWORD, String.format("set schema %s", SCHEMA_NAME));
	   
	   System.out.println();
	   
	   System.out.println("***********************MSSQL JDBC API Performance***********************");
	   direct_exect_query_test(MSSQL_DB_URL, MSSQL_USER, MSSQL_PASSWORD, String.format("use %s", SCHEMA_NAME));
	   prepare_exect_query_test(MSSQL_DB_URL, MSSQL_USER, MSSQL_PASSWORD, String.format("use %s", SCHEMA_NAME));
	   
	   System.out.println();
	   System.out.println("(Time Unit: nano seconds)");
   }
   
}
