package sample;

import java.io.File;
import java.net.URL;
import java.sql.*;

public class H2Sample {

    public static void main(String args[]) throws Exception {
        Class c = H2Sample.class.getClass();
        URL url = c.getResource("/user.csv");
        File file = new File(url.toURI());
        String csv = file.getCanonicalPath();

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=Oracle");

            conn.prepareStatement("create table user\n" +
                    "(\n" +
                    "id char(3) PRIMARY KEY,\n" +
                    "name varchar2(10),\n" +
                    "date DATE,\n" +
                    "notempty boolean,\n" +
                    ") AS SELECT * FROM CSVREAD('" + csv + "')").execute();
            ps = conn.prepareStatement("SELECT * FROM user");
            rs = ps.executeQuery();
            int colCount = rs.getMetaData().getColumnCount();
            System.out.println("取得したカラム数:" + colCount);

            while (rs.next()) {
                System.out.println("Id:" + rs.getInt("id"));
                System.out.println("Name:" + rs.getString("name"));
                System.out.println("Date:" + rs.getDate("date").toString());
                System.out.println("Empty:" + rs.getBoolean("notempty"));
            }

        } catch (Exception ex) {
            ex.printStackTrace();

        } finally {
            if (rs != null) rs.close();
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }
}
