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
        String sqlQuery = "SELECT Len('AKA',5), CharLen(5,'AKA'), date_diff('months', DATE '2022-02-02', DATE '2022-06-02'), to_timestamp(TIMESTAMP '2016-12-31 00:12:00'), to_timestamp( DATE '2016-12-31', 'yyyy-MM-dd')";
//
//        CONTAINS 'ANY' WHICH CAN BE USED FOR 'UNIT' IN OPERANDS
//        String sqlQuery = "SELECT TIMESTAMPDIFF(MONTH,cast('2022-02-02' as DATE),DATE '2022-05-02')";

        CalciteSchema schema = CalciteSchema.createRootSchema(true);
//        SchemaCustom customSchema = new SchemaCustom();
//        schema.add("CustomSchema",customSchema);

        SqlQueryPlan queryPlanner = new SqlQueryPlan();
        queryPlanner.getQueryPlan(schema.name,sqlQuery);
    }

}

//        "SELECT e.name AS employee_name, d.name AS department_name\n" +
//                "FROM customSchema.emps e\n" +
//                "JOIN customSchema.depts d ON e.deptno = d.deptno\n"