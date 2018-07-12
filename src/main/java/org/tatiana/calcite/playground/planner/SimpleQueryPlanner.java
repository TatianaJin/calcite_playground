package org.tatiana.calcite.playground.planner;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.calcite.plan.Contexts.EMPTY_CONTEXT;

public class SimpleQueryPlanner {
    private final Planner planner;

    public SimpleQueryPlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

        FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(schema) // Sets the schema to use by the planner
                .traitDefs(traitDefs)
                .context(EMPTY_CONTEXT) // Context can store data within the planner session for access by planner rules
                .ruleSets(RuleSets.ofList()) // Rule sets to use in transformation phases
                .costFactory(null) // Custom cost factory to use during optimization
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
    }

    public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
        String schema = Resources.toString(SimpleQueryPlanner.class.getResource("/test_model.json"),
                Charset.defaultCharset());
        // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + schema);

        // Create the query planner with the toy schema
        SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema()
                .getSubSchema(connection.getSchema()));
        RelNode logicalPlan = queryPlanner.getLogicalPlan("select color from parts where color='aka'");
        System.out.println(logicalPlan.getDescription());
        System.out.println(RelOptUtil.toString(logicalPlan));
    }

    public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error.", e);
        }
        SqlNode validatedSqlNode = planner.validate(sqlNode);

        System.out.println(validatedSqlNode.toString());
        return planner.rel(validatedSqlNode).project();
    }

}
