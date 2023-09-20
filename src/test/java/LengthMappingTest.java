
import mapping.SqlQueryPlan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import static org.apache.calcite.sql.parser.SqlParser.config;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {

//date_format('2016-04-08', 'y'), format_date('2016-04-08', 'y'), from_utc_timestamp( 'Asia/Seoul', '2016-08-31'), datetime('2016-08-31', 'Asia/Seoul'),

        String sqlQuery = "Select timestamp_diff(2005-10-12 01:25:20, 2005-10-12 02:04:13', 'minute')";



        System.out.println("[Input query]");
        System.out.println(sqlQuery);

        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        SqlParser parser = SqlParser.create(sqlQuery, config());
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]");
        System.out.println(sqlNode);
        System.out.println();



        SqlQueryPlan queryPlanner = new SqlQueryPlan();
        queryPlanner.getQueryPlan(schema.name,sqlQuery);





    }

}