package edu.cmu.cs.db.calcite_app.app;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

public class Optimizer {

    public static void optimize(String sql) throws Exception {

        Logger.getLogger("org.apache.calcite.plan.volcano").setLevel(Level.ALL);
        Logger.getLogger("org.apache.calcite.plan.RelOptPlanner").setLevel(Level.ALL);

        DataSource dataSource = new DataSource(
                "/Users/thapaswinikancharla/Desktop/15799-s25-project1/calcite_app/workload/data/opt_project.duckdb",
                "org.duckdb.DuckDBDriver");

        SchemaPlus rootSchema = dataSource.getSchema();

        CalciteConnection calciteConnection = dataSource.getCalciteConnection();

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .sqlToRelConverterConfig(SqlToRelConverter.config()
                        .withDecorrelationEnabled(true)
                        .withTrimUnusedFields(true)
                        .withExpand(false))
                .sqlValidatorConfig(
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true).withColumnReferenceExpansion(true))
                .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
                .defaultSchema(rootSchema)
                .operatorTable(SqlStdOperatorTable.instance())
                .convertletTable(StandardConvertletTable.INSTANCE)
                .build();

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode parsedNode = planner.parse(sql);
        SqlNode validatedNode = planner.validate(parsedNode);

        RelNode relNode = planner.rel(validatedNode).rel;

        RelBuilder relBuilder = RelBuilder.create(frameworkConfig);

        RelOptCluster cluster = relNode.getCluster();

        // HepPlanner hepPlanner = CustomPlanner.createHepPlanner();

        VolcanoPlanner volcanoPlanner = CustomPlanner.createVolcanoPlanner(cluster);

        volcanoPlanner.addListener(RuleTracer.buildListener());

        relNode = volcanoPlanner.changeTraits(relNode, cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        System.out.println(relNode.explain());

        volcanoPlanner.setRoot(relNode);

        RelNode optimizedRelNode = null;
        try {
            // This will throw an exception if the planner is not set up correctly
            optimizedRelNode = volcanoPlanner.findBestExp();
            ;
        } catch (Exception e) {
            System.out.println("Error getting RelTraitSet: " + e.getMessage());
        }

        System.out.println(
                RelOptUtil.dumpPlan("plan", optimizedRelNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        System.out.println("Optimized RelNode using VolcanoPlanner:");
        System.out.println(optimizedRelNode.explain());

        RelNode decorrelatedRelNode = RelDecorrelator.decorrelateQuery(optimizedRelNode, relBuilder);

        RelRunner relRunner = calciteConnection.unwrap(RelRunner.class);

        PreparedStatement statement = relRunner.prepareStatement(decorrelatedRelNode);
        long startTime = System.currentTimeMillis();
        runSql(statement);
        long endTime = System.currentTimeMillis();
        System.out.println("Optimized Execution Time: " + (endTime - startTime) + " ms");

        statement.close();

    }

    private static void runSql(PreparedStatement statement) throws Exception {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            // Process the result set
            // For example, print the first column of each row
            System.out.println(resultSet.getString(1));
            continue;
        }
    }
}
