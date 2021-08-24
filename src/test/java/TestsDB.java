import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestsDB extends TestsSetups {

    @Test
    public void testCreateTable() {
        String query = "CREATE TABLE people ("
                + "Id int(6) NOT NULL,"
                + "First_Name VARCHAR(45) NOT NULL,"
                + "Last_Name VARCHAR(45) NOT NULL,"
                + "UNIQUE (ID))";
        JDBCConnection.createTable(query);
    }

    @Test
    public void testInsertInto() {
        String query = "INSERT INTO people(Id, First_Name, Last_Name) VALUES (1, 'Stanislav', 'Motoryn')";
        JDBCConnection.insertIntoTable(query);

        String result = "SELECT * FROM people";
        ResultSet rs = JDBCConnection.selectFromTable(result);
        assertAll(
                () -> assertEquals(DataForTest.ID, rs.getString("Id")),
                () -> assertEquals(DataForTest.NAME, rs.getString("First_Name")),
                () -> assertEquals(DataForTest.SURNAME, rs.getString("Last_Name"))
        );
    }

    @Test
    public void testUpdateOfTable() throws SQLException {
        String query = "UPDATE people SET First_Name = 'Ivan' WHERE Id = 1";
        JDBCConnection.updateTable(query);
        String result = "SELECT * FROM people WHERE First_Name = 'Ivan'";
        ResultSet rs = JDBCConnection.selectFromTable(result);
        String actualName = rs.getString("First_Name");
        assertEquals("Ivan", actualName, "Name not update");
    }

    @Test
    public void testSelect() throws SQLException {
        String query = "SELECT * FROM people";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String actualSurname = rs.getString("Last_Name");
        assertEquals("Motoryn", actualSurname, "Surname is different");
    }

    @Test
    public void testSelectWithWhere() throws SQLException {
        String query = "SELECT * FROM people WHERE Id = 1";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String actualSurname = rs.getString("Last_Name");
        assertEquals("Motoryn", actualSurname, "Surname is different");
    }

    @Test
    public void testSelectWithJoin() throws SQLException {
        String query = "SELECT * FROM city INNER JOIN country ON city.CountryCode = country.Code";
        ResultSet rs = JDBCConnection.selectFromTable(query);
        String actualCode = rs.getString("country.Code");
        assertEquals("ABW", actualCode, "Code is different");
    }

    @Test
    public void testDeleteFromTable() {
        String query = "DELETE FROM people";
        JDBCConnection.deleteFromTable(query);
    }


    @Test
    public void testDropTable() {
       JDBCConnection.dropTable("people");
    }


}
