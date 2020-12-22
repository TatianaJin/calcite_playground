package org.tatiana.calcite.playground.algebra;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.apache.calcite.plan.Contexts.EMPTY_CONTEXT;

public class SimpleAlgebraBuilder {
    /**
     * Create FrameworkConfig that uses MYSQL and the schema in test_model.json
     *
     * @throws IOException
     * @throws SQLException
     */
    static FrameworkConfig createFrameworkConfig() throws IOException, SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
        String schema = Resources.toString(SimpleAlgebraBuilder.class.getResource("/test_model.json"),
                Charset.defaultCharset());
        // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + schema);

        return Frameworks.newConfigBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(connection.getRootSchema()
                        .getSubSchema(connection.getSchema())) // Sets the schema to use by the planner
                .traitDefs((List<RelTraitDef>) null)
                .context(EMPTY_CONTEXT) // Context can store data within the planner session for access by planner rules
                .build();
    }

    public static void main(String[] argv) throws IOException, SQLException {
        FrameworkConfig config = createFrameworkConfig();
        RelBuilder builder = RelBuilder.create(config);

        // Print all table names
        System.out.println("Tables in schema:");
        config.getDefaultSchema().getTableNames().forEach(System.out::println);

        // TODO: No statistics? How to supply?
        System.out.println(config.getDefaultSchema().getTable("Parts").getStatistic().getRowCount());

        // Table Scan
        System.out.println(RelOptUtil.toString(builder.scan("Parts").build()));
    }
}
