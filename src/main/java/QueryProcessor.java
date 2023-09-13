import mapping.SqlStdOperatorTablePlus;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import static org.apache.calcite.sql.parser.SqlParser.config;


public class QueryProcessor {

    public static void main(String[] args) throws Exception {

        enum Type implements java.lang.reflect.Type {

            SIMPLE,
            ADVANCED,
            PUSHDOWN;

            @Override
            public String getTypeName() {
                return java.lang.reflect.Type.super.getTypeName();
            }
        }



        String sqlQuery = "SELECT CHAR_LENGTH('abcd')";
        long start = System.currentTimeMillis();
        Enumerable<Objects> rows = execute(sqlQuery, Type.SIMPLE);
        if (rows != null) {
            for (Object row : rows) {
                if (row instanceof Object[]) {
                    System.out.println(Arrays.toString((Object[]) row));
                } else {
                    System.out.println(row);
                }
            }
        }
        long finish = System.currentTimeMillis();
        System.out.println("Elapsed time " + (finish - start) + "ms");
    }


    public static <T> Enumerable<T> execute(String sqlQuery, Type processorType)
            throws SqlParseException {
        System.out.println("[Input query]");
        System.out.println(sqlQuery);




        // Create the schema and table data types
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();


        // Create an SQL parser
         SqlParser parser = SqlParser.create(sqlQuery, config());


        // Parse the query into an AST
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]");
        System.out.println(sqlNode);
        System.out.println();


        // Configure and instantiate validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema, Collections.singletonList("bs"), typeFactory, config);

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTablePlus.instance(), catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());


        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

        logPlan.explain();
        // Display the logical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));



        return null;



    }




    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }

}
