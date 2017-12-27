package mycom;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

@SuppressWarnings("hiding")
public class MyMapFunction<Row, String> implements MapFunction<Row, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Row row) throws Exception {
		return null;
	}

}
