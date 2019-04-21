package com.bits.sparkstreamingproject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/* This class computes the Simple Moving Average for the closing price */
public class ComputeSimpleMovingAverage {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SimpleMovingAverage");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaDStream<String> inputString = jssc
				.textFileStream(args[1]);
	
		String outputPath = args[2];
		
		// Collecting the events and creating StockData DStream
		JavaDStream<StockData> streams = inputString.window(Durations.seconds(600), Durations.seconds(300))
				.flatMap(rdd -> {
					java.util.List<StockData> stockDataList = new ArrayList<StockData>();
					JSONParser parser = new JSONParser();
					JSONArray jArray = (JSONArray) parser.parse(rdd);

					for (Object o : jArray) {
						StockData stockData = new StockData();
						JSONObject jo = (JSONObject) o;
						String symbol = (String) jo.get("symbol");
						stockData.setStockName(symbol);
						
						Map priceData = ((Map) jo.get("priceData"));
						Iterator<Map.Entry> itr1 = priceData.entrySet().iterator();
						while (itr1.hasNext()) {
							Map.Entry pair = itr1.next();
							if (pair.getKey().equals("volume")) {
								stockData.setVolume(Double.parseDouble(pair.getValue().toString()));
							}

							if (pair.getKey().equals("high")) {
								stockData.setHighPrice(Double.parseDouble(pair.getValue().toString()));
							}

							if (pair.getKey().equals("low")) {
								stockData.setLowPrice(Double.parseDouble(pair.getValue().toString()));
							}

							if (pair.getKey().equals("close")) {
								stockData.setClosingPrice(Double.parseDouble(pair.getValue().toString()));
							}

							if (pair.getKey().equals("open")) {
								stockData.setOpeningPrice(Double.parseDouble(pair.getValue().toString()));
							}
						}
						stockDataList.add(stockData);
					}
					return stockDataList.iterator();
				});

		// Creating JavaPairDStream with Key as "StockName" and Value containing tuple
		// of Closing StockPrice, 1 (e.g "Google", Tuple<1900.00, 1>)
		JavaPairDStream<String, Tuple2<Double, Long>> pairDStreamOfClosingPrice = streams
				// .window(Durations.seconds(600), Durations.seconds(300))
				.mapToPair(new PairFunction<StockData, String, Tuple2<Double, Long>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(StockData stockData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(stockData.getStockName(),
								new Tuple2<Double, Long>(stockData.getClosingPrice(), (long) 1));
					}

				});

		// Computing Sum of Closing Price for each stock. The resultant PairDStream will
		// be Key as "StockName"
		// and Value containing tuple of Sum of <Closing StockPrice, Sum of elements>
		// for each
		// stock (e.g "Google", Tuple<1900.00, 5>)
		JavaPairDStream<String, Tuple2<Double, Long>> sumOfClosingPrice = pairDStreamOfClosingPrice
				// .window(Durations.seconds(600), Durations.seconds(300))
				.reduceByKey(
						(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		// Computing the average based on the sum of closing stock price / no. of
		// elements using function "getAverageByKey"
		// The resultant JavaPairDStream will be <Stock Name, Average Closing Price>
		// (e.g "Google", 1900.00)
		JavaPairDStream<String, Double> averageStockPrice = sumOfClosingPrice
				// .window(Durations.seconds(600), Durations.seconds(300))
				.mapToPair(getAverageByKey);

		// Printing the average. A folder named "MovingAverage" + current time in
		// milliseconds e.g. "MovingAverage1537371900962"
		// will be created inside "C:\temp\SimpleMovingAverage\" for each window and the
		// output file "part-00000" will be available
		averageStockPrice.foreachRDD(rdd -> {
			String messageId = String.valueOf(System.currentTimeMillis());
			rdd.coalesce(1).saveAsTextFile(outputPath + messageId);
		});

		// Start the Java Spark Context
		jssc.start();

		// Awaiting termination
		jssc.awaitTermination();

		// Spark Context terminated
		jssc.close();
	}

	// Helper Function to compute the average
	private static PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double> getAverageByKey = (tuple) -> {
		Tuple2<Double, Long> val = tuple._2;
		Double total = val._1;
		Long count = val._2;
		Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, (Double) (total / count));
		return averagePair;
	};
}
