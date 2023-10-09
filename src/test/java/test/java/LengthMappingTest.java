/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.java;

import mapping.SqlQueryPlan;
import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
//import org.junit.jupiter.api.Test;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {


//        This query covers the cases of ONLY KW, KW + SWAP(2 OPERANDS), KW + SWAP(3 OPERANDS) , SAME FUNCTION IF IT HAS OPTIONAL OPERANDS

//        String sqlQuery = "SELECT to_timeStamp('2016-12-31 00:12:00'), to_timeStamp('2016-12-31', 'yyyy-MM-dd'), to_unixTimestamp(DATE '2023-10-04', 'yyyy-mm-dd')";

        String sqlQuery1 = "SELECT i.i_item_id,\n" +
                "AVG(cs.cs_quantity) AS agg1,\n" +
                "COUNT(*) AS cnt,\n" +
                "date_format(d.d_date, 'YYYY-MM-DD') AS formatted_date,\n" +
                "date_format1(TIMESTAMP '2010-01-10 21:09:22' ,'HH:mm:ss dd/MM/yyyy'),\n" +
                "dateadd('DAY', 1, d.d_date) AS next_day\n" +
                "FROM catalog_sales cs\n" +
                "LEFT JOIN customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk\n" +
                "RIGHT JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk\n" +
                "INNER JOIN item i ON cs.cs_item_sk = i.i_item_sk\n" +
                "LEFT JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk\n" +
                "WHERE cd.cd_gender = 'F'\n" +
                "AND cd.cd_marital_status = 'M'\n" +
                "AND cd.cd_education_status = 'Secondary'\n" +
                "AND ( p.p_channel_email = 'N' OR p.p_channel_event = 'N' )\n" +
                "AND d.d_year = 2000\n" +
                "GROUP BY i.i_item_id, d.d_date, formatted_date\n" +
                "HAVING cnt > 1\n" +
                "ORDER BY i.i_item_id DESC\n" +
                "LIMIT 50";

        String sqlQuery2 = "SELECT\n" +
                "  s_state,\n" +
                "  s_city,\n" +
                "  SUM(CASE\n" +
                "      WHEN ss_sales_price >= 5000 THEN 1\n" +
                "      ELSE 0\n" +
                "    END) AS high_value_orders,\n" +
                "  SUM(CASE\n" +
                "      WHEN ss_sales_price >= 5000 THEN ss_sales_price\n" +
                "      ELSE 0\n" +
                "    END) AS high_value_sales,\n" +
                "  SUM(CASE\n" +
                "      WHEN ss_sales_price < 5000 THEN 1\n" +
                "      ELSE 0\n" +
                "    END) AS low_value_orders,\n" +
                "  SUM(CASE\n" +
                "      WHEN ss_sales_price < 5000 THEN ss_sales_price\n" +
                "      ELSE 0\n" +
                "    END) AS ss_sales_price,\n" +
                "datediff(DATE '2023-10-07',DATE '2018-09-22'),\n" +
                "timestampdiff(MONTH,TIMESTAMP '2001-01-05 10:22:32',d_date)\n" +
                "FROM\n" +
                "  store_sales\n" +
                "JOIN date_dim ON ss_sold_date_sk = d_date_sk\n" +
                "JOIN store ON ss_store_sk = s_store_sk\n" +
                "JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk\n" +
                "JOIN customer_address ON ss_addr_sk = ca_address_sk\n" +
                "WHERE\n" +
                "  d_year = (SELECT d_year FROM date_dim WHERE d_date = '2001-12-01')\n" +
                "  AND cd_marital_status IN ('M', 'W')\n" +
                "  AND cd_education_status IN ('Advanced Degree', 'College')\n" +
                "  AND ca_country = 'United States'\n" +
                "  AND s_gmt_offset = (SELECT MIN(s_gmt_offset) FROM store)\n" +
                "GROUP BY\n" +
                "  s_state,\n" +
                "  s_city\n" +
                "HAVING\n" +
                "  COUNT(*) > 50\n" +
                "ORDER BY\n" +
                "  s_state,\n" +
                "  s_city LIMIT 100";


        String sqlQuery7 = "select  C_CUSTOMER_ID, C_FIRST_NAME, C_EMAIL_ADDRESS, dateadd('month',2,DATE '2022-02-02') , dateadd('month',2,d_date) \n" +
                "FROM customer c\n" +
                "INNER JOIN date_dim dd ON\n" +
                "        c.c_first_shipto_date_sk  = dd.d_date_sk \n" +
                "        where\n" +
                "        c.C_SALUTATION like 'Dr.'\n" +
                "        and C_PREFERRED_CUST_FLAG like 'Y'\n" +
                "        and D_YEAR =1999\n" +
                "        LIMIT 10000";

        String sqlQuery8 = "SELECT i_item_id,\n" +
                "       i_item_desc,\n" +
                "       i_current_price,\n" +
                "timestampdiff(DAY, TIMESTAMP '2017-05-12 20:32:11', cc_rec_start_date),\n" +
                "timestampdiff(YEAR,cc_rec_start_date,cc_rec_end_date)\n" +
                "FROM call_center \n" +
                "left join item on SUBSTRING(i_item_id,5,16) = SUBSTRING(CC_CALL_CENTER_ID,5,16)\n" +
                "where   CC_COUNTY like '%County%' or CC_COUNTY like '%W%'";

        String sqlQuery9 = "SELECT i_item_id,\n" +
                "       i_item_desc,\n" +
                "       i_current_price, D_DATE, date_format(DATE '2023-10-07','dd/MM/yyyy')\n" +
                "FROM catalog_sales \n" +
                "INNER join item on cs_item_sk = i_item_sk\n" +
                "INNER join    date_dim on  I_REC_START_DATE = D_DATE \n" +
                "WHERE i_current_price BETWEEN 20 AND 20 + 50\n" +
                "AND D_DATE between '1997-10-27' and '2000-10-27'\n" +
                "GROUP BY i_item_id,\n" +
                "         i_item_desc,\n" +
                "         i_current_price,D_DATE\n" +
                "ORDER BY i_item_id\n" +
                "LIMIT 100";

        String sqlQuery10 = "WITH sales_data AS (\n" +
                "  SELECT\n" +
                "    s.ss_net_profit,\n" +
                "    timestampadd(YEAR,2,s_rec_start_date)\n" +
                "  FROM\n" +
                "    store_sales s\n" +
                "    JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk\n" +
                "    JOIN item i ON s.ss_item_sk = i.i_item_sk\n" +
                "    JOIN store st ON s.ss_store_sk = st.s_store_sk\n" +
                "  WHERE\n" +
                "    s.ss_net_profit IS NOT NULL\n" +
                "    AND s.ss_net_profit > 0\n" +
                "    AND s.ss_quantity = 90\n" +
                "    AND CAST(d.d_date AS TIMESTAMP) <= '2022-01-01' \n" +
                "    -- AND i.i_color REGEXP '^R.*'\n" +
                ")\n" +
                "SELECT\n" +
                "  MAX(sd.ss_net_profit) AS max_net_profit,\n" +
                "timestampadd(HOUR,10,TIMESTAMP '2016-12-31 00:12:00')\n" +
                "FROM\n" +
                "  sales_data sd\n" +
                "limit\n" +
                "  100";

        String sqlQuery11 = "WITH monthly_sales AS (\n" +
                "SELECT\n" +
                "i.i_category category,\n" +
                "i.i_class class,\n" +
                "i.i_brand brand,\n" +
                "s.s_store_name store_name,\n" +
                "s.s_company_name company_name,\n" +
                "d.d_moy dmoy,\n" +
                "SUM(ss.ss_sales_price) sum_sales,\n" +
                "AVG(SUM(ss.ss_sales_price)) OVER (\n" +
                "PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name\n" +
                ") avg_monthly_sales,\n" +
                "timestampdiff(MONTH, TIMESTAMP '2001-01-05 10:22:32' ,d_date)\n" +
                "FROM\n" +
                "item i\n" +
                "JOIN store_sales ss ON ss.ss_item_sk = i.i_item_sk\n" +
                "JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk\n" +
                "JOIN store s ON ss.ss_store_sk = s.s_store_sk\n" +
                "WHERE\n" +
                "d.d_year IN (2002)\n" +
                "AND (\n" +
                "(\n" +
                "LOWER(i.i_category) IN ('home', 'men', 'sports')\n" +
                "AND LOWER(i.i_class) IN ('paint', 'accessories', 'fitness')\n" +
                ")\n" +
                "OR (\n" +
                "LOWER(i.i_category) IN ('shoes', 'jewelry', 'women')\n" +
                "AND LOWER(i.i_class) IN ('mens', 'pendants', 'swimwear')\n" +
                ")\n" +
                ")\n" +
                "GROUP BY\n" +
                "i.i_category,\n" +
                "i.i_class,\n" +
                "i.i_brand,\n" +
                "s.s_store_name,\n" +
                "s.s_company_name,\n" +
                "d.d_moy\n" +
                "d.d_date\n"+
                ")\n" +
                "SELECT\n" +
                "category,\n" +
                "class,\n" +
                "brand,\n" +
                "store_name,\n" +
                "company_name,\n" +
                "dmoy,\n" +
                "sum_sales,\n" +
                "avg_monthly_sales,\n" +
                "CEIL((sum_sales - avg_monthly_sales) / avg_monthly_sales * 100) AS sales_difference_percentage\n" +
                "FROM\n" +
                "monthly_sales\n" +
                "WHERE\n" +
                "ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1\n" +
                "ORDER BY\n" +
                "sum_sales - avg_monthly_sales DESC,\n" +
                "store_name\n" +
                "LIMIT\n" +
                "100";


        String sqlQueryOwn = " SELECT Len('AKA',5) ,\n" +
                " CharLen(5,'AKA') ,\n" +
                "dateadd('month',2, DATE '2022-02-02'),\n" +
                "timestampdiff(YEAR,TIMESTAMP '2017-05-12 20:32:11',TIMESTAMP '2022-06-11 21:42:11') ,\n" +
                " datediff( DATE '2022-02-02', DATE '2022-06-02'),\n " +
                "to_timestamp(TIMESTAMP '2016-12-31 00:12:00'), \n" +
                "to_timestamp( DATE '2016-12-31', 'yyyy-MM-dd'), \n" +
                " timestampadd(HOUR,5,TIMESTAMP '2016-12-31 00:12:00'),\n" +
                " from_utc_timestamp(DATE '2016-08-31', 'Asia/Seoul') ,\n" +
                "from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'),\n" +
                " to_unixTimestamp(DATE '2016-04-08', 'yyyy-MM-dd')\n" ;

//                "date_format(DATE '2016-04-08', 'y')\n" +
//                "date_format1(DATE '2022-12-09', 'N')\n";

//        CONTAINS 'ANY' WHICH CAN BE USED FOR 'UNIT' IN OPERANDS
//        String sqlQuery = "SELECT TIMESTAMPADD(MONTH, 5 ,TIMESTAMP '2022-05-02 21:02:02')";

        CalciteSchema schema = CalciteSchema.createRootSchema(true);
//        SchemaCustom customSchema = new SchemaCustom();
//        schema.add("CustomSchema",customSchema);

        SqlQueryPlan queryPlanner = new SqlQueryPlan();
        queryPlanner.getQueryPlan(schema.name,sqlQueryOwn);
    }

}

//        "SELECT e.name AS employee_name, d.name AS department_name\n" +
//                "FROM customSchema.emps e\n" +
//                "JOIN customSchema.depts d ON e.deptno = d.deptno\n"