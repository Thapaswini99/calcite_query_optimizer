package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.plan.RelOptListener;

public class RuleTracer {
    public static RelOptListener buildListener() {
        return new RelOptListener() {
            @Override
            public void relEquivalenceFound(RelEquivalenceEvent event) {
                // System.out
                // .println(
                // "relEquivalenceFound: " + (event.getRel() != null ? event.getRel().explain()
                // : "null"));
            }

            @Override
            public void relDiscarded(RelDiscardedEvent event) {
                // System.out.println("ruleDiscarded: " +
                // event.);
            }

            @Override
            public void ruleAttempted(RuleAttemptedEvent event) {
                System.out.println("ruleAttempted: " +
                event.getRuleCall().getRule().getClass().getSimpleName());
            }

            @Override
            public void ruleProductionSucceeded(RuleProductionEvent event) {
                System.out.println(
                        "ruleProductionSucceeded: " + event.getRuleCall().getRule().getClass().getSimpleName());
                // System.out.println(
                //         "transformed to : " + (event.getRel() != null ? event.getRel().explain() : "null"));
                // System.out.println(
                //         "from: " + (event.getRuleCall().rels[0] != null ? event.getRuleCall().rels[0].explain()
                //                 : "null"));
            }

            @Override
            public void relChosen(RelChosenEvent event) {
                System.out.println("relChosen: " + (event.getRel() != null ?
                event.getRel().explain() : "null"));
            }
        };
    }
    
}
