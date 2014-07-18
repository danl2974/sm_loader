package strongmail.eventloader;

import java.sql.*;
import java.util.ArrayList;

/**
 * Created by DLubin on 7/18/2014.
 */
public class MailingInfoFetch {


    //private static String mssqlConnection = "jdbc:sqlserver://10.157.95.101:1433;database=StrongMail;user=StrongMail;password=Str0ngMai!;";
    private static String mssqlConnection = "jdbc:sqlserver://k22cep04af.database.windows.net:1433;database=email_engine;user=emailengine@k22cep04af;password=!Welco2200;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
    private static String dbhost = "jdbc:sqlserver://10.157.95.101:1433;databaseName=EmailDataWarehouse";
    private static String dbusername = "StrongMail";
    private static String dbpassword = "Str0ngMai!";



    static public void start() {

        CallableStatement callableStatement = null;
        Connection con = null;
        Connection mscon = null;
        ArrayList<StrongmailMailing> smlist = new ArrayList<StrongmailMailing>();

        try {

            Class.forName("org.postgresql.Driver");
            String jdbcFormat = String.format("jdbc:%s://%s:%s/%s", "postgresql", "localhost", 5432, "postgres");
            con = DriverManager.getConnection(jdbcFormat, "postgres", "str0ngmail");
            String select = "select * from mailing order by modified_time desc";
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(select);
            while (rs.next()) {
                StrongmailMailing smm = new StrongmailMailing();
                smm.mailingId = rs.getInt("mailing_id");
                smm.mailingName = rs.getString("name");
                smm.mailingDepartment = rs.getString("from_name");
                smm.mailingDepartmentId = rs.getInt("department_id");
                smlist.add(smm);
            }

            MsSqlServerLoader.loadData(smlist);

        } catch (SQLException sqle) {
            System.out.println(String.format("SQL EXCEPTION: loadData %s %d %s", sqle.getLocalizedMessage(), sqle.getErrorCode(), sqle.getSQLState()));
            sqle.printStackTrace();
        } catch (Exception e) {
            System.out.println("EXCEPTION: loadDataMessageStudio " + e.getMessage());
        } finally {
            if (callableStatement != null) {
                try {
                    callableStatement.close();
                } catch (SQLException e) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                }
            }

        }


    }
}
