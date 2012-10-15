package com.mongodb.hadoop.hive;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

public class BSONSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void TestDeserializeNestedStruct() throws SerDeException {

        // The BSONWritable instance to deserialize

        BSONObject structInDoc = new BasicBSONObject();
        structInDoc.put("strField", "str");
        structInDoc.put("intField", 1);

        BSONObject doc = new BasicBSONObject();
        doc.put("myStr", "str");
        doc.put("myInt", 1);
        doc.put("myStruct", structInDoc);

        BSONWritable input = new BSONWritable(doc);

        // The resulting List<Object> instance

        List<Object> valuesInStruct = new ArrayList<Object>();
        valuesInStruct.add("str");
        valuesInStruct.add(1);

        List<Object> expected = new ArrayList<Object>();
        expected.add("str");
        expected.add(1);
        expected.add(valuesInStruct);

        // Configurations for BSONSerde

        Configuration sysProps = new Configuration();

        Properties tblProps = new Properties();
        tblProps.put(Constants.LIST_COLUMNS, "myStr,myInt,myStruct");
        tblProps.put(Constants.LIST_COLUMN_TYPES, "string,int,struct<strField:string,intField:int>");

        // Test

        BSONSerde bsonSerde = new BSONSerde();

        bsonSerde.initialize(sysProps, tblProps);

        List<Object> actual = (List<Object>) bsonSerde.deserialize(input);

        assertEquals(expected, actual);
    }

}
