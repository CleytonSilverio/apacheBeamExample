import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.beam.teste.apachebeam.model.Parcel;

public class TesteMySql {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC";
        String user = "cleyton";
        String password = "senha";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM parcel")) {

            while (rs.next()) {
            	Parcel parcela = new Parcel(rs.getInt("id"), 
        				rs.getInt("user_id"), 
        				rs.getBigDecimal("amount"),
        				rs.getDate("due_date").toLocalDate());
            	System.out.println(parcela.toString());
                System.out.println("Parcel: id=" + rs.getInt("id")
                        + ", userId=" + rs.getInt("user_id")
                        + ", amount=" + rs.getBigDecimal("amount")
                        + ", dueDate=" + rs.getDate("due_date"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
