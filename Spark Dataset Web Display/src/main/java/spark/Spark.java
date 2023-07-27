package spark;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Spark
 */
@WebServlet("/Spark")
public class Spark extends HttpServlet {
    // ... (your existing code)

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head><title>Receiving Country</title></head>");
        out.println("<body>");
        out.println("<center><h1>Receiving Country Filtered Dataset</h1>");

        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/spark?serverTimezone=Europe/Chisinau";
            String username = "root";
            String password = "root";
            conn = DriverManager.getConnection(url, username, password);
            stmt = conn.createStatement();

            // Modify the query to match your MySQL table structure and column names
            String query = "SELECT * FROM rcc";
            ResultSet rs = stmt.executeQuery(query);

            // Output the data as an HTML table
            out.println("<table border=\"1\">"); // Add a border to the table
            out.println("<tr><th>Sending Country Code</th><th>Count</th></tr>");

            while (rs.next()) {
                // Retrieve data from the result set
                String rccSendingCountry = rs.getString("Sending Country Code");
                int rccCount = rs.getInt("count");

                // Output each row as a table row
                out.println("<tr>");
                out.println("<td>" + rccSendingCountry + "</td>");
                out.println("<td>" + rccCount + "</td>");
                out.println("</tr>");
            }

            out.println("</table>");
        } catch (SQLException | ClassNotFoundException e) {
            out.println("An error occurred while retrieving data: " + e.toString());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
            }
        }

        out.println("</center>");
        out.println("</body>");
        out.println("</html>");
        out.close();
    }
}

