package mapping;

import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Tables {

   public static Table dateDimTable =   new CustomTable(
            factory ->
            new RelDataTypeFactory.Builder(factory)
            .add("d_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
            .add("d_date_id", factory.createSqlType(SqlTypeName.VARCHAR))
            .add("d_date", factory.createSqlType(SqlTypeName.DATE))
                    .add("d_month_seq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_week_seq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_quarter_seq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_year", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_dow", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_moy", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_dom", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_qoy", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_fy_year", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_fy_quarter_seq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_fy_week_seq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_day_name", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_quarter_name", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_holiday", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_weekend", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_following_holiday", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_first_dom", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_last_dom", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_same_day_ly", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_same_day_lq", factory.createSqlType(SqlTypeName.INTEGER))
                    .add("d_current_day", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_current_week", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_current_month", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_current_quarter", factory.createSqlType(SqlTypeName.VARCHAR))
                    .add("d_current_year", factory.createSqlType(SqlTypeName.VARCHAR))

                    .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table ItemTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("i_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_item_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_rec_start_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("i_rec_end_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("i_item_desc", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_current_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("i_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("i_brand_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_brand", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_class_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_category_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_category", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_manufact_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_manufact", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_size", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_formulation", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_color", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_units", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_container", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("i_manager_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("i_product_name", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table StoreTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("s_store_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_store_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_rec_start_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("s_rec_end_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("s_closed_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_store_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_number_employees", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_floor_space", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_hours", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_market_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_geography_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_market_desc", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_market_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_division_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_division_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_company_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("s_company_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_street_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_street_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_street_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_suite_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_city", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_county", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_state", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_zip", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("s_gmt_offset", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("s_tax_percentage", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table CustomerTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("c_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_customer_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_current_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_current_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_current_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_first_shipto_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_first_sales_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_salutation", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_first_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_last_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_preferred_cust_flag", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_birth_day", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_birth_month", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_birth_year", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("c_birth_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_login", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_email_address", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("c_last_review_date", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table WebsiteTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("web_site_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("web_site_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_rec_start_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("web_rec_end_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("web_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_open_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("web_close_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("web_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_mkt_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("web_mkt_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_mkt_desc", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_market_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_company_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("web_company_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_street_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_street_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_street_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_suite_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_city", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_county", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_state", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_zip", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("web_gmt_offset", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("web_tax_percentage", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table StorereturnsTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("sr_returned_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_return_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_store_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_reason_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_ticket_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_return_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sr_return_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_return_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_return_amt_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_fee", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_return_ship_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_refunded_cash", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_reversed_charge", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_store_credit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("sr_net_loss", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table Household_demographicsTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("hd_demo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("hd_income_band_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("hd_buy_potential", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("hd_dep_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("hd_vehicle_count", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table WebPageTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("wp_web_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_web_page_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("wp_rec_start_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("wp_rec_end_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("wp_creation_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_access_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_autogen_flag", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("wp_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_url", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("wp_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("wp_char_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_link_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_image_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wp_max_ad_count", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table PromotionTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("p_promo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("p_promo_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_start_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("p_end_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("p_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("p_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("p_response_target", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("p_promo_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_dmail", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_email", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_catalog", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_tv", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_radio", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_press", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_event", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_demo", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_channel_details", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_purpose", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("p_discount_active", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table CatalogPageTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("cp_catalog_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cp_catalog_page_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cp_start_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cp_end_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cp_department", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cp_catalog_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cp_catalog_page_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cp_description", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cp_type", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table CatalogReturnsTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("cr_returned_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_returned_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_refunded_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_refunded_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_refunded_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_refunded_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_returning_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_returning_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_returning_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_returning_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_call_center_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_catalog_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_ship_mode_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_warehouse_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_reason_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_order_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_return_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cr_return_amount", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_return_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_return_amt_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_fee", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_return_ship_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_refunded_cash", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_reversed_charge", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_store_credit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cr_net_loss", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table WebReturnsTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("wr_returned_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_returned_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_refunded_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_refunded_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_refunded_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_refunded_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_returning_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_returning_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_returning_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_returning_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_web_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_reason_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_order_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_return_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("wr_return_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_return_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_return_amt_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_fee", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_return_ship_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_refunded_cash", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_reversed_charge", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_account_credit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("wr_net_loss", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table WebSalesTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("ws_sold_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_bill_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_bill_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_bill_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_bill_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_web_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_web_site_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_ship_mode_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_warehouse_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_promo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_order_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ws_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_discount_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_coupon_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_ext_ship_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_net_paid", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_net_paid_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_net_paid_inc_ship", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_net_paid_inc_ship_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_net_profit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ws_sold_date_sk", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table InventoryTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("inv_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("inv_warehouse_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("inv_quantity_on_hand", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("inv_date_sk", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table CatalogSalesTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("cs_sold_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_bill_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_bill_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_bill_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_bill_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_call_center_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_catalog_page_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_ship_mode_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_warehouse_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_promo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_order_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cs_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_discount_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_coupon_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_ext_ship_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_net_paid", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_net_paid_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_net_paid_inc_ship", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_net_paid_inc_ship_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_net_profit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cs_sold_date_sk", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table StoreSalesTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("ss_sold_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_item_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_customer_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_cdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_hdemo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_addr_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_store_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_promo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_ticket_number", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_quantity", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ss_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_ext_discount_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_ext_sales_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_ext_wholesale_cost", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_ext_list_price", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_ext_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_coupon_amt", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_net_paid", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_net_paid_inc_tax", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_net_profit", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ss_sold_date_sk", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table IncomeBandTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("ib_income_band_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ib_lower_bound", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ib_upper_bound", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table ReasonTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("r_reason_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("r_reason_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("r_reason_desc", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table TimedimTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("t_time_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("t_time_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("t_time", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("t_hour", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("t_minute", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("t_second", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("t_am_pm", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("t_shift", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("t_sub_shift", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("t_meal_time", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table ShipModeTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("sm_ship_mode_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("sm_ship_mode_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("sm_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("sm_code", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("sm_carrier", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("sm_contract", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table WarehouseTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("w_warehouse_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("w_warehouse_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_warehouse_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_warehouse_sq_ft", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("w_street_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_street_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_street_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_suite_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_city", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_county", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_state", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_zip", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("w_gmt_offset", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table Customer_demographicsTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("cd_demo_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cd_gender", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cd_marital_status", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cd_education_status", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cd_purchase_estimate", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cd_credit_rating", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cd_dep_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cd_dep_employed_count", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cd_dep_college_count", factory.createSqlType(SqlTypeName.INTEGER))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table Customer_AddressTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("ca_address_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("ca_address_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_street_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_street_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_street_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_suite_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_city", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_county", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_state", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_zip", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("ca_gmt_offset", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("ca_location_type", factory.createSqlType(SqlTypeName.VARCHAR))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    public static Table CallcenterTable =   new CustomTable(
            factory ->
                    new RelDataTypeFactory.Builder(factory)
                            .add("cc_call_center_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_call_center_id", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_rec_start_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("cc_rec_end_date", factory.createSqlType(SqlTypeName.DATE))
                            .add("cc_closed_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_open_date_sk", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_employees", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_sq_ft", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_hours", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_mkt_id", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_mkt_class", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_mkt_desc", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_market_manager", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_division", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_division_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_company", factory.createSqlType(SqlTypeName.INTEGER))
                            .add("cc_company_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_street_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_street_name", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_street_type", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_suite_number", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_city", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_county", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_state", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_zip", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_country", factory.createSqlType(SqlTypeName.VARCHAR))
                            .add("cc_gmt_offset", factory.createSqlType(SqlTypeName.DECIMAL))
                            .add("cc_tax_percentage", factory.createSqlType(SqlTypeName.DECIMAL))

                            .build(), ImmutableBitSet.of(0), new ArrayList<>());

    private static class CustomTable extends AbstractTable implements ScannableTable {
    private final ImmutableBitSet pkColumns;
    private final List<Object[]> data;
    private final Function<RelDataTypeFactory, RelDataType> typeBuilder;

    CustomTable(
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
