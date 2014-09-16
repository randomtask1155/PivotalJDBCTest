package io.pivotal.support;

import java.sql.*;

public class JDBCTest {
//public static final String CONNECTION_URL = "jdbc:postgresql://mdw:5432/gpadmin?protocolVersion=4&user=gpadmin&password=changeme&loglevel=2";
//public static final String DRIVER = "org.postgresql.Driver";
//public static final String SQL = "SELECT * from jdbc_test";

    public static void usage() {
        System.out.printf("\nUsage: pivotal.support.JDBCTest <url> <sql statement> <driver> <username> <password>\n\n");
        System.out.printf("JDBCTest postgres :\njava -classpath ./postgresql-8.4-701.jdbc4.jar:./jdbc-pivotal-support.jar pivotal.support.JDBCTest \"jdbc:postgresql://mdw:5432/gpadmin?protocolVersion=3&user=gpadmin&password=changeme&loglevel=2\" \"select * from table\" org.postgresql.Driver\n\n");
        System.out.printf("JDBCTest greenplum:\njava -classpath ./jdbc-pivotal-support.jar:greenplum.jar pivotal.support.JDBCTest \"jdbc:pivotal:greenplum://mdw:5432;DatabaseName=gpadmin;\" \"select * from table\" com.pivotal.jdbc.GreenplumDriver gpadmin changeme\n\n");
    }

    public static void main(String[] args) {
        if ( args.length < 3 ) {
            usage();
            return;
        }

        String URL = args[0];
        String Q = args[1];
        String DRIVER = args[2];

        System.out.printf("Num of args=%d\n", args.length);
        for ( int i = 0; i < args.length; i++) {
            System.out.printf("ARG%d: %s\n", i, args[i]);
        }

        try {
            if (args.length == 3) {
                // username and password can be provided in the postgres JDBC url string
                performSelect(URL, Q, DRIVER, "", "");
            } else if ( args.length == 5) {
                // username and password are required for Greenplum.jar to execute
                performSelect(URL, Q, DRIVER, args[3], args[4]);
            } else {
                usage();
                return;
            }
        } catch (ClassNotFoundException e) {
            System.out.println("Caught ClassNotFoundException");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Caught SQLException");
            e.printStackTrace();
        }
    }

    static public void performSelect(String CONNECTION_URL, String SQL, String DRIVER, String USER, String PASSWORD) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVER);
        Connection connection;

        System.out.printf("user=%s\npass=%s\n", USER, PASSWORD);
        connection = DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD);
        connection.setAutoCommit(false);

        try {
            for (int i = 0; i < 2; i++) {
                System.out.println("Attempt to execute: " + i);
                Statement ps = null;
                try {
                    ps = connection.createStatement();
                    ps.executeQuery(SQL);
                /* Example of how to print results
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
