package lyd.ai.dataflow.exercises.table.stream;

import lyd.ai.dataflow.exercises.datastream.utils.GeoUtils;
import lyd.ai.dataflow.exercises.table.sources.TaxiRideTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopularPlacesSqlJ {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a TableEnvironment
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		// register TaxiRideTableSource as table "TaxiRides"
		tEnv.registerTableSource(
				"TaxiRides",
				new TaxiRideTableSource(
						input,
						maxEventDelay,
						servingSpeedFactor));

		// register user-defined functions
		tEnv.registerFunction("isInNYC", new GeoUtils.IsInNYC());
		tEnv.registerFunction("toCellId", new GeoUtils.ToCellId());
		tEnv.registerFunction("toCoords", new GeoUtils.ToCoords());

		Table results = tEnv.sqlQuery(
			"SELECT " +
				"toCoords(cell), wstart, wend, isStart, popCnt " +
			"FROM " +
				"(SELECT " +
					"cell, " +
					"isStart, " +
					"HOP_START(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart, " +
					"HOP_END(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend, " +
					"COUNT(isStart) AS popCnt " +
				"FROM " +
					"(SELECT " +
						"eventTime, " +
						"isStart, " +
						"CASE WHEN isStart THEN toCellId(startLon, startLat) ELSE toCellId(endLon, endLat) END AS cell " +
					"FROM TaxiRides " +
					"WHERE isInNYC(startLon, startLat) AND isInNYC(endLon, endLat)) " +
				"GROUP BY cell, isStart, HOP(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)) " +
			"WHERE popCnt > 20"
			);

		// convert Table into an append stream and print it
		// (if instead we needed a retraction stream we would use tEnv.toRetractStream)
		tEnv.toAppendStream(results, Row.class).print();

		// execute query
		env.execute();
	}

}
