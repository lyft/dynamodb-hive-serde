package com.lyft.hive.serde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by eliotwalker on 8/4/15.
 */
public class DynamoDbSerDe extends AbstractSerDe {

    public static final Log LOG = LogFactory.getLog(RegexSerDe.class.getName());

    static final char ETX = '\003';
    static final char STX = '\002';

    int numColumns;
    StructObjectInspector rowOI;
    List<Object> row;
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    Object[] outputFields;
    Text outputRowText;

    @Override
    public void initialize(Configuration configuration, Properties tbl) throws SerDeException {

        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        columnNames = Arrays.asList(columnNameProperty.split(","));
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        /*
         * Constructing the row ObjectInspector:
         * The row consists of some set of primitive columns, each column will
         * be a java object of primitive type.
         */
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
        for (int c = 0; c < numColumns; c++) {
            TypeInfo typeInfo = columnTypes.get(c);
            if (typeInfo instanceof PrimitiveTypeInfo) {
                PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
                AbstractPrimitiveJavaObjectInspector oi =
                        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
                columnOIs.add(oi);
            } else {
                throw new SerDeException(getClass().getName()
                        + " doesn't allow column [" + c + "] named "
                        + columnNames.get(c) + " with type " + columnTypes.get(c));
            }
        }

        // StandardStruct uses ArrayList to store the row.
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs, null);

        row = new ArrayList<Object>(numColumns);
        // Constructing the row object, etc, which will be reused for all rows.
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }
        outputFields = new Object[numColumns];
        outputRowText = new Text();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    private String extractValue(String input) {
        String rightToken = input.substring(input.indexOf(':') + 1, input.length());
        return rightToken.substring(rightToken.indexOf('"') + 1, rightToken.lastIndexOf('"'));
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        Map<String, String> values = decomposeRow(rowText.toString());

        for (int c = 0; c < numColumns; c++) {
            try {
                String t = values.get(columnNames.get(c));
                TypeInfo typeInfo = columnTypes.get(c);

                // Convert the column to the correct type when needed and set in row obj
                PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
                switch (pti.getPrimitiveCategory()) {
                    case STRING:
                        row.set(c, t);
                        break;
                    case BYTE:
                        Byte b;
                        b = Byte.valueOf(t);
                        row.set(c,b);
                        break;
                    case SHORT:
                        Short s;
                        s = Short.valueOf(t);
                        row.set(c,s);
                        break;
                    case INT:
                        Integer i;
                        i = Integer.valueOf(t);
                        row.set(c, i);
                        break;
                    case LONG:
                        Long l;
                        l = Long.valueOf(t);
                        row.set(c, l);
                        break;
                    case FLOAT:
                        Float f;
                        f = Float.valueOf(t);
                        row.set(c,f);
                        break;
                    case DOUBLE:
                        Double d;
                        d = Double.valueOf(t);
                        row.set(c,d);
                        break;
                    case BOOLEAN:
                        Boolean bool;
                        bool = Boolean.valueOf(t);
                        row.set(c, bool);
                        break;
                    case TIMESTAMP:
                        Timestamp ts;
                        ts = Timestamp.valueOf(t);
                        row.set(c, ts);
                        break;
                    case DATE:
                        Date date;
                        date = Date.valueOf(t);
                        row.set(c, date);
                        break;
                    case DECIMAL:
                        HiveDecimal bd = HiveDecimal.create(t);
                        row.set(c, bd);
                        break;
                    case CHAR:
                        HiveChar hc = new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
                        row.set(c, hc);
                        break;
                    case VARCHAR:
                        HiveVarchar hv = new HiveVarchar(t, ((VarcharTypeInfo)typeInfo).getLength());
                        row.set(c, hv);
                        break;
                    default:
                        throw new SerDeException("Unsupported type " + typeInfo);
                }
            } catch (RuntimeException e) {
                row.set(c, null);
            }
        }
        return row;
    }

    private Map<String, String> decomposeRow(String rowText) {
        Map<String, String> values = new HashMap<String, String>();
        StringBuilder headerBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();

        boolean insideHeader = true;
        for (char c : rowText.toCharArray()) {
            if (insideHeader) {
                if (c == ETX) {
                    insideHeader = false;
                } else {
                    headerBuilder.append(c);
                }
            } else {
                if (c == STX || c == '\n') {
                    values.put(headerBuilder.toString(), extractValue(valueBuilder.toString()));
                    headerBuilder = new StringBuilder();
                    valueBuilder = new StringBuilder();
                    insideHeader = true;
                } else {
                    valueBuilder.append(c);
                }
            }
        }

        if (!valueBuilder.toString().isEmpty()) {
            values.put(headerBuilder.toString(), extractValue(valueBuilder.toString()));
        }

        return values;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
            throws SerDeException {
        throw new UnsupportedOperationException(
                "DynamoDb SerDe doesn't support the serialize() method");
    }

    @Override
    public SerDeStats getSerDeStats() {
        // no support for statistics
        return null;
    }
}
