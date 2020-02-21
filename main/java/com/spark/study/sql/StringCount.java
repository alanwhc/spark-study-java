package com.spark.study.sql;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StringCount extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = 1L;

	@Override
	public StructType inputSchema() {
		return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("str", DataTypes.StringType, true)));
	}

	@Override
	public StructType bufferSchema() {
		return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("count", DataTypes.IntegerType, true)));
	}

	@Override
	public DataType dataType() {
		return DataTypes.IntegerType;
	}

	@Override
	public boolean deterministic() {
		return true;
	}
	
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, 0);
 	}
	
	@Override
	public Object evaluate(Row buffer) {
		return buffer.getAs(0);
	}
	

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		if(input.isNullAt(0)) return;
		buffer.update(0, buffer.getInt(0) + 1);
	}

}
