package com.atguigu.function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;

/**
 * @author mlstart
 * @create 2020-10-15 9:47
 */
public class MyUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size()!= 1) {
            throw new UDFArgumentLengthException("输入参数个数不为1...");
        }
        if (!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())){
            throw new UDFArgumentTypeException(0,"参数的类型不正确...");
        }
        ArrayList<String> fieldName = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIS = new ArrayList<>();
        fieldName.add("aaa");
        fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldOIS);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String jsonArr = objects[0].toString();
        JSONArray actions = new JSONArray(jsonArr);
        for (int i = 0; i < actions.length(); i++) {
            String action = actions.getString(i);
            String[] s = new String[1];
            s[0] = action;
            forward(s);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
