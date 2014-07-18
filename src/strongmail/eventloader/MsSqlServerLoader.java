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
        //String sproc = "EXECUTE StrongMail.dbo.HealthAdvisor_EmailRequestEvent_Add ?,?,?,?";
        //String insert = "INSERT INTO [EmailDataWarehouse].[Email].[MailingInfo] ( [MailingId],[Mailing],[DepartmentId],[Department] ) VALUES( ?, ?, ?, ? )";
        String insert = "BEGIN IF NOT EXISTS (SELECT * FROM [EmailDataWarehouse].[Email].[MailingInfo] WHERE [MailingId] = ?) BEGIN INSERT INTO [EmailDataWarehouse].[Email].[MailingInfo] ( [MailingId],[Mailing],[DepartmentId],[Department] ) VALUES( ?, ?, ?, ? ) END END";
        //String sproc = "{call dbo.HealthAdvisor_EmailRequestEvent_Add(?,?,?,?)}";
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //Class.forName("net.sourceforge.jtds.jdbc.Driver");
        mscon = DriverManager.getConnection(dbhost, dbusername, dbpassword);
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
            if(callresult >= 0) {
                System.out.println(String.format("MailingInfo add: %d %s", callresult, smm.mailingName));
            }
        }
        mscon.commit();

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
