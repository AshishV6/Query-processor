package mapping;


import org.apache.calcite.jdbc.CalciteSchema;

public class SqlQueryPlan {


    public void getQueryPlan(String sSchemaName, String sQueryString)
    {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        SqlParserPlus parser = getPlanner(schema,  sSchemaName);

        parser.parse("", sSchemaName, sQueryString);

    }

    private SqlParserPlus getPlanner(CalciteSchema rootSchema, String defaultSchema)
    {
        return HepSqlPlanner.create(rootSchema, defaultSchema);
    }

}