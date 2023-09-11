package mapping;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;


import java.util.*;
import javax.annotation.Nullable;
import javax.xml.validation.Schema;

//amsbsaxsa
//xsxzaszazaz
public class HepSqlPlanner extends SqlParserPlus
{

    public static final ImmutableSet<RelOptRule> RULE_SET = ImmutableSet.of(CoreRules.AGGREGATE_MERGE,
            CoreRules.AGGREGATE_REMOVE, CoreRules.AGGREGATE_JOIN_TRANSPOSE,
            CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED, CoreRules.CALC_MERGE, CoreRules.CALC_REMOVE,
            CoreRules.CALC_REDUCE_DECIMALS, CoreRules.CALC_SPLIT, CoreRules.CALC_TO_WINDOW, CoreRules.COERCE_INPUTS,
            CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS, CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS, CoreRules.FILTER_INTO_JOIN,
            CoreRules.FILTER_MERGE, CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_SCAN, CoreRules.FILTER_CORRELATE, CoreRules.INTERSECT_MERGE, CoreRules.INTERSECT_TO_DISTINCT,
            CoreRules.MINUS_MERGE, CoreRules.PROJECT_JOIN_JOIN_REMOVE, CoreRules.PROJECT_JOIN_REMOVE,
            CoreRules.PROJECT_JOIN_TRANSPOSE, CoreRules.PROJECT_MERGE, CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_TABLE_SCAN, CoreRules.PROJECT_INTERPRETER_TABLE_SCAN,
            CoreRules.JOIN_CONDITION_PUSH, CoreRules.JOIN_PUSH_EXPRESSIONS, CoreRules.SORT_UNION_TRANSPOSE,
            CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH, CoreRules.SORT_REMOVE, CoreRules.SORT_REMOVE_CONSTANT_KEYS,
            CoreRules.UNION_MERGE, CoreRules.UNION_REMOVE, CoreRules.UNION_PULL_UP_CONSTANTS, CoreRules.UNION_TO_DISTINCT,
            CoreRules.AGGREGATE_VALUES, CoreRules.FILTER_VALUES_MERGE, CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE, CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
            // Partition by rules
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW, CoreRules.PROJECT_WINDOW_TRANSPOSE);
    // Temporary workaround: Disabling the sub-query rules
// TODO: Fix the cast conversion error + other errors
//CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
//CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
//CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    private final SqlValidator m_validator;
    private final SqlToRelConverter m_converter;
    private final SqlParser.Config m_parserConfig;
    private final SchemaCustom m_schema;
    private static RelOptCluster m_cluster = null;
    public static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    protected static  SqlAbstractParserImpl parser;



    public HepSqlPlanner(CalciteConnectionConfig config, RelOptCluster cluster, SqlValidator validator,
                         SqlToRelConverter converter, SchemaCustom schema, SqlAbstractParserImpl parser)
    {
        super(parser);
        m_validator = validator;
        m_converter = converter;
        m_parserConfig = SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withCaseSensitive(config.caseSensitive())
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withConformance(config.conformance());

        m_cluster = cluster;
        m_schema = schema;
    }

    public static SqlParserPlus create(CalciteSchema rootSchema, String defaultSchema)
    {
        rootSchema = rootSchema.root();
        SchemaCustom schemaCustom = new SchemaCustom();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootCalciteSchema = CalciteSchema.createRootSchema(true, false, schemaCustom.getSchemaName(), schemaCustom);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootCalciteSchema, ImmutableList.of(schemaCustom.getSchemaName()), typeFactory, config);

        SqlStdOperatorTablePlus operatorTable = new SqlStdOperatorTablePlus() {
            @Override
            public void lookupOperatorOverloads(SqlIdentifier opName, @org.checkerframework.checker.nullness.qual.Nullable SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {

            }

            @Override
            public List<SqlOperator> getOperatorList() {
                return null;
            }
        };

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT.withLenientOperatorLookup(
                        config.lenientOperatorLookup())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true)
                .withTypeCoercionRules(getSqlTypeCoercionRules());

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        // Instrument the query compilation time
        long startQueryCompileTime = System.nanoTime();
        long endQueryCompileTime = System.nanoTime();
        long compileTime = (endQueryCompileTime - startQueryCompileTime) / 1000_000;

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .withDecorrelationEnabled(true)
                .withInSubQueryThreshold(25);

        ArrayList<Pair<HepProgram, HepPlanner>> mapOfHEPPrograms = new ArrayList<>();
        HepProgram programMain1 = new HepProgramBuilder().build();
        mapOfHEPPrograms.add(Pair.of(programMain1, createPlannerPhase(RULE_SET, Integer.MAX_VALUE)));

        // addMatchLimit should have set the rule match limit but it doesn't.
        HepProgram programMain2 = new HepProgramBuilder().addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
                .addRuleCollection(ImmutableList.of(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES))
                .build();
        mapOfHEPPrograms.add(
                Pair.of(programMain2, createPlannerPhase(ImmutableList.of(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES), 10)));
        mapOfHEPPrograms.add(Pair.of(buildHEPProgram(ImmutableList.of(CoreRules.FILTER_PROJECT_TRANSPOSE)),
                createPlannerPhase(ImmutableList.of(CoreRules.FILTER_PROJECT_TRANSPOSE), Integer.MAX_VALUE)));
        mapOfHEPPrograms.add(Pair.of(buildHEPProgram(ImmutableList.of(CoreRules.FILTER_MERGE)),
                createPlannerPhase(ImmutableList.of(CoreRules.FILTER_MERGE), Integer.MAX_VALUE)));

        RelOptCluster cluster = RelOptCluster.create(mapOfHEPPrograms.get(0).right, new RexBuilderPlus(typeFactory));

        SqlToRelConverter converter = new SqlToRelConverter( NOOP_EXPANDER, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE);

        return new HepSqlPlanner(config, cluster, validator, converter, schemaCustom, parser);
    }

    @NotNull
    private static SqlTypeCoercionRule getSqlTypeCoercionRules()
    {
        Map<SqlTypeName, ImmutableSet<SqlTypeName>> map = new HashMap<>();
        map.putAll(SqlTypeCoercionRule.instance().getTypeMapping());
        ImmutableSet<SqlTypeName> typesWhichCanCastToVarchar = map.get(SqlTypeName.VARCHAR);
        HashSet<SqlTypeName> finalTypesWhichCanCastToVarchar = new HashSet<>();
        finalTypesWhichCanCastToVarchar.addAll(typesWhichCanCastToVarchar);
        finalTypesWhichCanCastToVarchar.add(SqlTypeName.ARRAY);
        map.put(SqlTypeName.VARCHAR, ImmutableSet.copyOf(finalTypesWhichCanCastToVarchar));
        SqlTypeCoercionRule sqlTypeCoercionRule = SqlTypeCoercionRule.instance(map);
        return sqlTypeCoercionRule;
    }

    @NotNull
    private static HepPlanner createPlannerPhase(Collection<RelOptRule> ruleSet, @Nullable Integer matchLimit)
    {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleCollection(ruleSet);

        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
        return planner;
    }

    private RelNode convert(SqlNode node)
    {
        RelRoot root = m_converter.convertQuery(validateNode(node), false, true);

        return root.rel;
    }

    private SqlNode validateNode(SqlNode node)
    {
        return m_validator.validate(node);
    }

    public RelNode optimize(RelNode node)
    {
        RelNode optimizedNode = node;
        while (true)
        {
            Pair<Pair<Boolean, HepProgram>, RelNode> pairRelNodePair = runOptimizePhases(optimizedNode);
            Boolean shouldRerun = pairRelNodePair.left.left;
            if (shouldRerun)
            {
                HepProgram programToRemove = pairRelNodePair.left.right;
                continue;
            }
            optimizedNode = pairRelNodePair.right;
            break;
        }
        return optimizedNode;
    }

    @Override
    public RelOptPlanner getPlanner()
    {
        return null;
    }

    @Override
    public RelMetadataQuery getRelMetaDataQuery()
    {
        return null;
    }


    private Pair<Pair<Boolean, HepProgram>, RelNode> runOptimizePhases(RelNode optimizedNode)
    {
        HepProgram programToIgnore;
        HepProgram plannerProgram = null;
        HepPlanner planner;


        return Pair.of(Pair.of(false, null), optimizedNode);
    }


    public RelNode getOptimizedRelNode(SqlNode sqlNode)
    {
        RelNode converted = convert(sqlNode);
        RelNode optimized = optimize(converted);
        QueryCountMetrics.hep();
        return optimized;
    }

    public static HepProgram buildHEPProgram(Collection<RelOptRule> ruleSet)
    {
        return new HepProgramBuilder().addRuleCollection(ruleSet).build();
    }


    protected SqlNode parseX(String query) throws SqlParseException
    {
        return SqlParser.create(query, m_parserConfig).parseStmt();
    }


    protected SchemaCustom getSchema()
    {
        return m_schema;
    }


    public RexBuilder getRexBuilder()
    {
        return m_converter.getRexBuilder();
    }

}
