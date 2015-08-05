package com.lyft.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by eliotwalker on 8/5/15.
 */
public class DynamoDbSerDeTest {

    private static final String BOOL_COL = "boolean_col" + DynamoDbSerDe.ETX + "{\"bool\":\"true\"}\n";
    private static final String INT_COL = "int_col" + DynamoDbSerDe.ETX + "{\"n\":\"1\"}";
    private static final String STRING_COL = "string_col" + DynamoDbSerDe.ETX + "{\"s\":\"string_value\"}" + DynamoDbSerDe.STX;

    private DynamoDbSerDe serde;
    private Configuration config;

    @Before
    public void setUp() {
        serde = new DynamoDbSerDe();
        config = new Configuration();
    }

    @Test
    public void testStringColumn() throws Exception {
        initSerde("string_col", "string");

        Text input = new Text();
        input.set(STRING_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals("string_value", output.get(0));
    }

    @Test
    public void testBooleanColumn() throws Exception {
        initSerde("boolean_col", "boolean");

        Text input = new Text();
        input.set(BOOL_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertTrue((Boolean) output.get(0));
    }

    @Test
    public void testIntColumn() throws Exception {
        initSerde("int_col", "int");

        Text input = new Text();
        input.set(INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(1, output.size());
        assertEquals(1, output.get(0));
    }

    @Test
    public void testMultipleColumns() throws Exception {
        initSerde("string_col,int_col", "string,int");

        Text input = new Text();
        input.set(STRING_COL + INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(2, output.size());
        assertEquals("string_value", output.get(0));
        assertEquals(1, output.get(1));
    }

    @Test
    public void testColumnsSubsetOfInput() throws Exception {
        initSerde("string_col,int_col", "string,int");

        Text input = new Text();
        input.set(STRING_COL + BOOL_COL + INT_COL);

        ArrayList<Object> output = (ArrayList<Object>) serde.deserialize(input);
        assertEquals(2, output.size());
        assertEquals("string_value", output.get(0));
        assertEquals(1, output.get(1));
    }

    private void initSerde(String columnNames, String columnTypes) throws Exception {
        Properties properties = new Properties();
        properties.put(serdeConstants.LIST_COLUMNS, columnNames);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
        serde.initialize(config, properties);
    }
}
