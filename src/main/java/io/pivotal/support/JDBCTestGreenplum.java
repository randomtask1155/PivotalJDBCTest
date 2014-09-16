package io.pivotal.support;

import java.sql.*;

public class JDBCTestGreenplum {
    //public static final String DRIVER = "org.postgresql.Driver";
    public static final String DRIVER = "com.pivotal.jdbc.GreenplumDriver";


    public static void main(String[] args) {
// TODO Auto-generated method stub

        if (args.length != 2) {
            System.out.printf("\nUsage:\n\nJDBCTest  \"jdbc:pivotal:greenplum://mdw:5432;DatabaseName=gpadmin\" \"select * from table\"\n\n");
            return;
        }
        String URL = args[0];
        String Q = args[1];

        try {
            performSelect(URL, Q);
        } catch (ClassNotFoundException e) {
            System.out.println("Caught ClassNotFoundException");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Caught SQLException");
            e.printStackTrace();
        }
    }

    // @org.junit.Test
    static public void performSelect(String CONNECTION_URL, String SQL) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVER);
        Connection connection = DriverManager.getConnection(CONNECTION_URL, "gpadmin", "changeme");
        connection.setAutoCommit(false);

        try {
            for (int i = 0; i < 2; i++) {
                System.out.println("Attempt to execute: " + i);
                Statement ps = null;
                try {
                    ps = connection.createStatement();
                    ps.executeQuery(SQL);
                /* for printing results
                ResultSet result = ps.getResultSet();
                while ( result.next() ) {
                    System.out.print("Dumping results: ");
                    System.out.printf("%s\n",result.getInt("foo"));
                }
                result.close();*/

                } finally {
                    if (ps != null)
                        ps.close();
                }
            }
        } finally {
            connection.close();
        }
    }
}
