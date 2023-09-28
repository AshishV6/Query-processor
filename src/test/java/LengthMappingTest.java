
import mapping.SqlQueryPlan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import static org.apache.calcite.sql.parser.SqlParser.config;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {

//   date_format('2016-04-08', 'y'), format_date('2016-04-08', 'y')

//        date_diff_databricks('MONTH', TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), " +
//        "date_diff_e6data(TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59', 'MONTH')



//   from_utc_timestamp( 'Asia/Seoul', '2016-08-31'), datetime('2016-08-31', 'Asia/Seoul'),

//   timestampdiff(month, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), timestamp_diff(cast('2005-10-12 01:25:20' as timestamp), cast('2005-10-12 02:04:13' as timestamp), 'MONTH')

//   dummy_datediff('MONTH', TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), date_diff_e6data(TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59', 'MONTH')

//   dateadd('MONTH', -1, TIMESTAMP'2022-03-31 00:00:00'), date_add('MONTH', -1, TIMESTAMP'2022-03-31 00:00:00')


        String sqlQuery = "Select date_format('2016-04-08', 'y'), format_date('2016-04-08', 'y')";




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