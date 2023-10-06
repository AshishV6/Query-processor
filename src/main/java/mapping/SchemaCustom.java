package mapping;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class SchemaCustom extends AbstractSchema {

    private final ImmutableMap<String, Table> tables;

    public SchemaCustom() {
        tables = ImmutableMap.<String, Table>builder()
                .put("date_dim", Tables.dateDimTable)
                .put("item",Tables.ItemTable)
                .put("store",Tables.StoreTable)
                .put("customer",Tables.CustomerTable)
                .put("web_site",Tables.WebsiteTable)
                .put("store_returns",Tables.StorereturnsTable)
                .put("household_demographics",Tables.Household_demographicsTable)
                .put("web_page",Tables.WebPageTable)
                .put("promotion",Tables.PromotionTable)
                .put("catalog_page",Tables.CatalogPageTable)
                .put("catalog_returns",Tables.CatalogReturnsTable)
                .put("web_returns",Tables.WebReturnsTable)
                .put("web_sales",Tables.WebSalesTable)
                .put("inventory",Tables.InventoryTable)
                .put("catalog_sales",Tables.CatalogSalesTable)
                .put("store_sales",Tables.StoreSalesTable)
                .put("income_band",Tables.IncomeBandTable)
                .put("reason",Tables.ReasonTable)
                .put("time_dim",Tables.TimedimTable)
                .put("ship_mode",Tables.ShipModeTable)
                .put("warehouse",Tables.WarehouseTable)
                .put("customer_demographics",Tables.Customer_demographicsTable)
                .put("customer_address",Tables.Customer_AddressTable)
                .put("call_center",Tables.CallcenterTable)


                .build();
    }



    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }

    public String getSchemaName() {
        return "SchemaCustom";
    }

    private static class PkClusteredTable extends AbstractTable implements ScannableTable {
        private final ImmutableBitSet pkColumns;
        private final List<Object[]> data;
        private final Function<RelDataTypeFactory, RelDataType> typeBuilder;

        PkClusteredTable(
                Function<RelDataTypeFactory, RelDataType> dataTypeBuilder,
                ImmutableBitSet pkColumns,
                List<Object[]> data) {
            this.data = data;
            this.typeBuilder = dataTypeBuilder;
            this.pkColumns = pkColumns;
        }

        @Override
        public Statistic getStatistic() {
            List<RelFieldCollation> collationFields = new ArrayList<>();
            for (Integer key : pkColumns) {
                collationFields.add(
                        new RelFieldCollation(
                                key,
                                RelFieldCollation.Direction.ASCENDING,
                                RelFieldCollation.NullDirection.LAST));
            }
            return Statistics.of(data.size(), ImmutableList.of(pkColumns),
                    ImmutableList.of(RelCollations.of(collationFields)));
        }

        @Override
        public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            return typeBuilder.apply(typeFactory);
        }

        @Override
        public Enumerable<@Nullable Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(data);
        }


    }
}
