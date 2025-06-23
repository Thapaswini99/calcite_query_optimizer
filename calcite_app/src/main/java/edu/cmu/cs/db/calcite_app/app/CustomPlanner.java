package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverterRule;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;

public class CustomPlanner {

    private static final ImmutableList<RelOptRule> getJdbcRules(JdbcConvention jdbcConvention) {
        
        return ImmutableList.copyOf(JdbcRules.rules(jdbcConvention));
    }

    private static ImmutableList<RelOptRule> getDefaultRules() {
        return ImmutableList.of(
            // Add default optimization rules here
            CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.FILTER_CORRELATE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST,
            CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND,
            CoreRules.PROJECT_CORRELATE_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,
            CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER,
            CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
            CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER,
            CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
            CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER
        );
    }

    public static ArrayList<RelOptRule> enumerableRules() {

        ArrayList<RelOptRule> rules = new ArrayList<>();
        rules.add(EnumerableRules.ENUMERABLE_SORT_RULE);

        rules.add(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        rules.add(EnumerableRules.ENUMERABLE_JOIN_RULE);
        rules.add(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        rules.add(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        rules.add(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        rules.add(EnumerableRules.ENUMERABLE_FILTER_RULE);
        rules.add(EnumerableRules.ENUMERABLE_VALUES_RULE);

        rules.add(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        rules.add(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

        rules.add(EnumerableRules.ENUMERABLE_CALC_RULE);

        // Wrapper conversion rule. Might not actually need this
        rules.add(AbstractConverter.ExpandConversionRule.INSTANCE);


        return rules;
    }

    public static HepPlanner createHepPlanner() {

        HepProgram program = new HepProgramBuilder().addRuleCollection(getDefaultRules()).build();
        // Create a HepPlanner with default configuration
        return new HepPlanner(program);
    }

    public static VolcanoPlanner createVolcanoPlanner(RelOptCluster cluster) {
        // Create a VolcanoPlanner with default configuration
        VolcanoPlanner volcanoPlanner;
        if(!(cluster.getPlanner() instanceof VolcanoPlanner)) {
            volcanoPlanner = new VolcanoPlanner();
        } else {
            volcanoPlanner = (VolcanoPlanner) cluster.getPlanner();
        }
        volcanoPlanner.clear();
        // for (RelOptRule rule : getJdbcRules(jdbcConvention)) {
        //     if(rule.getClass().getName().contains("JdbcToEnumerableConverterRule")) {
        //         // Skip JdbcRules as they are already included in the getJdbcRules method
        //         continue;
        //     }
        //     System.out.println("Adding rule: " + rule.getClass());
        //     volcanoPlanner.addRule(rule);
        // }

        volcanoPlanner.addRule(AbstractConverter.ExpandConversionRule.INSTANCE);

        for (RelOptRule rule : enumerableRules()) {
            System.out.println("Adding rule: " + rule.getClass());
            volcanoPlanner.addRule(rule);
        }

        for (RelOptRule rule : getDefaultRules()) {
            volcanoPlanner.addRule(rule);
        }
        volcanoPlanner.setTopDownOpt(true);
        return volcanoPlanner;
    }
}
