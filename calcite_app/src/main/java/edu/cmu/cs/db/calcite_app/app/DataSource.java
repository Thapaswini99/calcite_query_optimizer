package edu.cmu.cs.db.calcite_app.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.schema.Schema;
import org.apache.commons.dbcp2.BasicDataSource;

public class DataSource {
    private BasicDataSource dataSource;
    private JdbcConvention jdbcConvention;
    private CalciteConnection calciteConnection;

    DataSource(String databasePath, String driverClassName) {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl("jdbc:duckdb:" + databasePath);
    }

    public SchemaPlus getSchema() throws Exception{
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
            DriverManager.getConnection("jdbc:calcite:", info);
        calciteConnection =
            connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        Expression expression = Schemas.subSchemaExpression(rootSchema, "opt_project", JdbcSchema.class);

        jdbcConvention = new JdbcConvention(PostgresqlSqlDialect.DEFAULT, expression, "opt_project");

        Schema schema = new JdbcSchema(dataSource, PostgresqlSqlDialect.DEFAULT, jdbcConvention, null, null);

        rootSchema.add("opt_project", schema);

        return rootSchema;
    }

    public Connection getDuckDbConnection() throws Exception{
        return dataSource.getConnection();
    }

    public JdbcConvention getJdbcConvention() {
        return jdbcConvention;
    }

    public CalciteConnection getCalciteConnection() {
        return calciteConnection;
    }
}
