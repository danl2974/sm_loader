package strongmail.eventloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by DLubin on 7/18/2014.
 */
public class MsSqlServerLoader {


    //private static String mssqlConnection = "jdbc:sqlserver://10.157.95.101:1433;database=StrongMail;user=StrongMail;password=Str0ngMai!;";
    private static String mssqlConnection = "jdbc:sqlserver://k22cep04af.database.windows.net:1433;database=email_engine;user=emailengine@k22cep04af;password=!Welco2200;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
    private static String dbhost = "jdbc:sqlserver://10.157.95.101:1433;databaseName=EmailDataWarehouse";
    private static String dbusername = "StrongMail";
    private static String dbpassword = "Str0ngMai!";


    public static void loadData(ArrayList<StrongmailMailing> sml){


    Connection mscon = null;

    try{

        System.out.println( sml.get(0).mailingName +  sml.get(0).mailingDepartment);
        //String sproc = "EXECUTE StrongMail.dbo.HealthAdvisor_EmailRequestEvent_Add ?,?,?,?";
        //String insert = "INSERT INTO [EmailDataWarehouse].[Email].[MailingInfo] ( [MailingId],[Mailing],[DepartmentId],[Department] ) VALUES( ?, ?, ?, ? )";
        String insert = "BEGIN IF NOT EXISTS (SELECT * FROM [EmailDataWarehouse].[Email].[MailingInfo] WHERE [MailingId] = ?) BEGIN INSERT INTO [EmailDataWarehouse].[Email].[MailingInfo] ( [MailingId],[Mailing],[DepartmentId],[Department] ) VALUES( ?, ?, ?, ? ) END END";
        //String sproc = "{call dbo.HealthAdvisor_EmailRequestEvent_Add(?,?,?,?)}";
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //Class.forName("net.sourceforge.jtds.jdbc.Driver");
        System.out.println("after Class mssql driver");
        //Class.forName("net.sourceforge.jtds.jdbc.Driver");
        mscon = DriverManager.getConnection(dbhost, dbusername, dbpassword);
        System.out.println("after mssql connection call");
        //con = DriverManager.getConnection(mssqlConnection);

        StrongmailMailing smmtest = sml.get(0);

        for (StrongmailMailing smm: sml) {
            PreparedStatement pstmt = mscon.prepareStatement(insert);
            pstmt.setInt(1, smm.mailingId);
            pstmt.setInt(2, smm.mailingId);
            pstmt.setString(3, smm.mailingName);
            pstmt.setInt(4, smm.mailingDepartmentId);
            pstmt.setString(5, smm.mailingDepartment);
            int callresult = pstmt.executeUpdate();
            System.out.println(String.format("ms call result: %d", callresult));
        }
        mscon.commit();

        //System.out.println(String.format("ms call result: %d", callresult));

		  /*
		  String query = "select * from [dbo].[HealthAdvisor_EmailRequestEvent]";
		  Statement stmt = con.createStatement();
		  System.out.println("inside loadData before Query");
		  ResultSet rs = stmt.executeQuery(query);
		  System.out.println("inside loadData after Query ");
		  System.out.println(String.format("sql warning code %d", rs.getStatement().getWarnings().getErrorCode()) );
		  System.out.println("RS object " + String.valueOf( rs.getMetaData().getColumnCount() ) );

		  while (rs.next()) {

			  System.out.println("RS " + rs.getString("EMAIL") + rs.getString("MAILINGID"));

		  }
		  */
		  /*
		  System.out.println("connection prodname " + con.getMetaData().getDatabaseProductVersion() + " " + String.valueOf(con.getMetaData().getCatalogs().getMetaData().getColumnCount()));
		  PreparedStatement pstmt = con.prepareStatement(insert);
		  //pstmt.setTimestamp(1, er.timestamp);
		  pstmt.setInt(1, 99999);
		  pstmt.setString(2, "Mailing000");
		  pstmt.setInt(3, 99999);
		  pstmt.setString(4, "Department000");
	      int callresult = pstmt.executeUpdate();
	      con.commit();
	      System.out.println("call " + String.format("%s", callresult));
		  */
		  /*
		  callableStatement = con.prepareCall(sproc);

		      callableStatement.setTimestamp(1, er.timestamp);
		      //callableStatement.setString(1, er.timestamp);
		      callableStatement.setString(2, er.emailAddress);
		      callableStatement.setString(3, er.mailingId);
		      callableStatement.setString(4, er.campaignId);
		      int callresult = callableStatement.executeUpdate();
		      con.commit();

		      */
		      /*
		      ResultSet rs = con.getMetaData().getProcedures(null, null, null);
		      while (rs.next()) {
		    	  System.out.println("Procedures " + rs.getString("PROCEDURE_NAME"));
		      }
		      */

        //System.out.println("call sproc " + String.format("%s %s %s %s %s", String.valueOf(callresult), String.valueOf(er.timestamp),  er.emailAddress,  er.mailingId, er.campaignId ));

    }
    catch(SQLException sqle){
        System.out.println(String.format("SQL EXCEPTION: loadData %s %d %s", sqle.getLocalizedMessage(), sqle.getErrorCode(), sqle.getSQLState() ) );
        sqle.printStackTrace();
    }
    catch(Exception e){
        System.out.println("EXCEPTION: loadDataMSSQL " + e.getMessage());
    }
    finally{

        if (mscon != null){
            try {
                mscon.close();
            } catch (SQLException e) {}
        }

    }

}





}
