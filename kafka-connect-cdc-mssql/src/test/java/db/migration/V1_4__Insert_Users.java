package db.migration;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

public class V1_4__Insert_Users implements JdbcMigration {
  @Override
  public void migrate(Connection connection) throws Exception {
    Fairy fairy = Fairy.create();
    Random random = new SecureRandom();

    final String SQL = "INSERT INTO [dbo].[users] (first_name, last_name, email, gender, " +
        "ip_address, company_name, country_code, latitude, longitude, account_balance, username) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    try (PreparedStatement statement = connection.prepareStatement(SQL)) {
      for (int i = 0; i < 100; i++) {
        Person person = fairy.person();

        BigDecimal decimal = BigDecimal.valueOf(random.nextLong(), 6);

        statement.setString(1, person.getFirstName());
        statement.setString(2, person.getLastName());
        statement.setString(3, person.getEmail());
        statement.setString(4, person.getSex().name());
        statement.setString(5, fairy.networkProducer().ipAddress());
        statement.setString(6, person.getCompany().getName());
        statement.setString(7, null);
        statement.setString(8, null);
        statement.setString(9, null);
        statement.setBigDecimal(10, decimal);
        statement.setString(11, person.getUsername());
        statement.addBatch();
      }

      statement.executeBatch();
    }
  }
}
