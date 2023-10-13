package mapping;

import mapping.SqlStdOperatorTablePlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public enum Swappable {

    CHARLEN {
        @Override
        public int getSwappableType() {
            return 0;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return INDEXSEQMAP.get(0);
        }
    }, DATEDIFF {
        @Override
        public int getSwappableType() {
            return 0;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return INDEXSEQMAP.get(0);
        }
    }, date_diff {
        @Override
        public int getSwappableType() {
            return 1;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return INDEXSEQMAP.get(4);
        }

    }, TIMESTAMPDIFF {
        @Override
        public int getSwappableType() {
            return 1;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return INDEXSEQMAP.get(4);
        }
    }, TO_TIMESTAMP {
        @Override
        public int getSwappableType() {
            return -1;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return new ArrayList<>();
        }
    }, TO_UNIXTIMESTAMP {
        @Override
        public int getSwappableType() {
            return -1;
        }

        @Override
        public ArrayList<Integer> getSwapOrder() {
            return new ArrayList<>();
        }
    };

    // getSwappableType -->    0 - 2 operand swapping
//                         1 - 3 operand swapping
//                        -1 - optional operands trimming
    public abstract int getSwappableType();

    public abstract ArrayList<Integer> getSwapOrder();

    private final static List<String> SWAPPABLE_LIST = Arrays.stream(Swappable.values()).map(Enum::name).toList();

    public static boolean isInSwappable(String operatorName) {
        return SWAPPABLE_LIST.contains(operatorName);
    }

    public final static HashMap<Integer, ArrayList<Integer>> INDEXSEQMAP = new HashMap<>() {{
        put(0, new ArrayList<>(Arrays.asList(1, 0)));
        put(1, new ArrayList<>(Arrays.asList(0, 1, 2)));
        put(2, new ArrayList<>(Arrays.asList(0, 2, 1)));
        put(3, new ArrayList<>(Arrays.asList(1, 0, 2)));
        put(4, new ArrayList<>(Arrays.asList(1, 2, 0)));
        put(5, new ArrayList<>(Arrays.asList(2, 0, 1)));
        put(6, new ArrayList<>(Arrays.asList(2, 1, 0)));
    }};

    public static HashMap<SqlOperator,SqlOperator> COMMON_IDENTIFIER_MAP = new HashMap<>(){{
        put(SqlStdOperatorTablePlus.LEN, SqlStdOperatorTablePlus.LOC);
        put(SqlStdOperatorTablePlus.CHAR_LEN, SqlStdOperatorTablePlus.LOC);
        put(SqlStdOperatorTablePlus.datediff,SqlStdOperatorTablePlus.DATE_DIFF);
        put(SqlStdOperatorTablePlus.DATE_DIFF,SqlStdOperatorTablePlus.DATE_DIFF);
        put(SqlStdOperatorTablePlus.TO_TIMESTAMP,SqlStdOperatorTablePlus.to_timestamp);
        put(SqlStdOperatorTablePlus.TO_UNIXTIMESTAMP,SqlStdOperatorTablePlus.TO_UNIX_TIMESTAMP);
        put(SqlStdOperatorTablePlus.dateadd,SqlStdOperatorTablePlus.date_add);
        put(SqlStdOperatorTablePlus.date_format,SqlStdOperatorTablePlus.format_date);
        put(SqlStdOperatorTablePlus.date_format1,SqlStdOperatorTablePlus.format_timestamp);
        put(SqlStdOperatorTablePlus.timestampdiff,SqlStdOperatorTablePlus.timestamp_diff);
        put(SqlStdOperatorTablePlus.timestampadd,SqlStdOperatorTablePlus.timestamp_add);
        put(SqlStdOperatorTablePlus.date_trunc,SqlStdOperatorTablePlus.date_trunc);
        put(SqlStdOperatorTablePlus.from_unixtime,SqlStdOperatorTablePlus.from_unixtime);
        put(SqlStdOperatorTablePlus.from_utc_timestamp,SqlStdOperatorTablePlus.datetime);
        put(SqlStdOperatorTablePlus.datepart,SqlStdOperatorTablePlus.date_part);
    }};

}
