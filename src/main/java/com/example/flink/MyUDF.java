package com.example.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Date 5/14/2020 2:36 PM
 * @Created by 
 */
public class MyUDF extends ScalarFunction {
    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return BasicTypeInfo.STRING_TYPE_INFO;
        //return super.getResultType(signature);
    }

    public String eval(int date){
        String s = String.valueOf(date);
        if (s.length()!=8){
            throw new RuntimeException("arkDateToStr UDF's params should be yyyyMMdd. Actual is "+date);
        }
        return s.substring(0,4)+"-"+s.substring(4,6)+"-"+s.substring(6,8);
    }


}
