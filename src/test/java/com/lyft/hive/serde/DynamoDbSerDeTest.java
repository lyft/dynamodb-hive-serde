package com.lyft.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by eliotwalker on 8/5/15.
 */
public class DynamoDbSerDeTest {

    private static final String BOOL_COL = "boolean_col" + DynamoDbSerDe.ETX + "{\"bool\":\"true\"}\n";
    private static final String BYTE_COL = "byte_col" + DynamoDbSerDe.ETX + "{\"n\":\"1\"}" + DynamoDbSerDe.STX;
    private static final String DATE_COL = "date_col" + DynamoDbSerDe.ETX + "{\"s\":\"2015-05-28\"}" + DynamoDbSerDe.STX;
    private static final String FLOAT_COL = "float_col" + DynamoDbSerDe.ETX + "{\"n\":\"1.23\"}\n";
    private static final String INT_COL = "int_col" + DynamoDbSerDe.ETX + "{\"n\":\"1\"}";
    private static final String LONG_COL = "long_col" + DynamoDbSerDe.ETX + "{\"n\":\"1\"}";
    private static final String SHORT_COL = "short_col" + DynamoDbSerDe.ETX + "{\"n\":\"1\"}" + DynamoDbSerDe.STX;
    private static final String STRING_COL = "string_col" + DynamoDbSerDe.ETX + "{\"s\":\"string_value\"}" + DynamoDbSerDe.STX;
    private static final String TIMESTAMP_COL = "timestamp_col" + DynamoDbSerDe.ETX + "{\"s\":\"2015-05-28 23:59:59.9999\"}" + DynamoDbSerDe.STX;

    private DynamoDbSerDe serde;
    private Configuration config;
    private Text input;

    @Before
    public void setUp() {
        serde = new DynamoDbSerDe();
        config = new Configuration();
        input = new Text();
    }

    @Test
    public void testStringColumn() throws Exception {
        initSerde("string_col", "string");
        input.set(STRING_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals("string_value", output.get(0));
    }

    @Test
    public void testBooleanColumn() throws Exception {
        initSerde("boolean_col", "boolean");
        input.set(BOOL_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertTrue((Boolean) output.get(0));
    }

    @Test
    public void testIntColumn() throws Exception {
        initSerde("int_col", "int");
        input.set(INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals(1, output.get(0));
    }

    @Test
    public void testLongColumn() throws Exception {
        initSerde("long_col", "bigint");
        input.set(LONG_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals(1L, output.get(0));
    }

    @Test
    public void testFloatColumn() throws Exception {
        initSerde("float_col", "decimal");
        input.set(FLOAT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals(HiveDecimal.create(new BigDecimal("1.23")), output.get(0));
    }

    @Test
    public void testByteColumn() throws Exception {
        initSerde("byte_col", "tinyint");
        input.set(BYTE_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        byte expected = 1;
        assertEquals(expected, output.get(0));
    }

    @Test
    public void testShortColumn() throws Exception {
        initSerde("short_col", "smallint");
        input.set(SHORT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        short expected = 1;
        assertEquals(expected, output.get(0));
    }

    @Test
    public void testDateColumn() throws Exception {
        initSerde("date_col", "date");
        input.set(DATE_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());

        Date expected = Date.valueOf("2015-05-28");
        assertEquals(expected, output.get(0));
    }

    @Test
    public void testTimestampColumn() throws Exception {
        initSerde("timestamp_col", "timestamp");
        input.set(TIMESTAMP_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());

        Timestamp expected = Timestamp.valueOf("2015-05-28 23:59:59.9999");
        assertEquals(expected, output.get(0));
    }

    @Test
    public void testMultipleColumns() throws Exception {
        initSerde("string_col,int_col", "string,int");
        input.set(STRING_COL + INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(2, output.size());
        assertEquals("string_value", output.get(0));
        assertEquals(1, output.get(1));
    }

    @Test
    public void testColumnsSubsetOfInput() throws Exception {
        initSerde("string_col,int_col", "string,int");
        input.set(STRING_COL + BOOL_COL + INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(2, output.size());
        assertEquals("string_value", output.get(0));
        assertEquals(1, output.get(1));
    }

    @Test
    public void testCustomTimestampFormatNull() throws Exception {
        Properties properties = new Properties();
        properties.put(serdeConstants.LIST_COLUMNS, "timestamp_col");
        properties.put(serdeConstants.LIST_COLUMN_TYPES, "timestamp");
        properties.put(DynamoDbSerDe.INPUT_TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS");
        serde.initialize(config, properties);
        input.set(TIMESTAMP_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());

        Timestamp expected = null;
        assertEquals(expected, output.get(0));
    }

    @Test
    public void testCustomTimestampFormat() throws Exception {
        Properties properties = new Properties();
        properties.put(serdeConstants.LIST_COLUMNS, "timestamp_col");
        properties.put(serdeConstants.LIST_COLUMN_TYPES, "timestamp");
        properties.put(DynamoDbSerDe.INPUT_TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        serde.initialize(config, properties);
        input.set("timestamp_col" + DynamoDbSerDe.ETX + "{\"s\":\"2015-06-28T18:10:29.123456\"}" + DynamoDbSerDe.STX);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());

        Timestamp expected = Timestamp.valueOf("2015-06-28 18:10:29.123");
        assertEquals(expected, output.get(0));
    }

    private void initSerde(String columnNames, String columnTypes) throws Exception {
        Properties properties = new Properties();
        properties.put(serdeConstants.LIST_COLUMNS, columnNames);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
        serde.initialize(config, properties);
    }
}
