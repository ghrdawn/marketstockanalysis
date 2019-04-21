package com.bits.sparkstreamingproject;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/* This class computes the sum of trading volume for each stock and computes the highest trading volume amongst all the stocks */
public class ComputeTradingVolume {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ComputeTradingVolume");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaDStream<String> inputString = jssc.textFileStream(args[1]);

		String outputPath = args[2];
		createDirectoryIfNotExists(outputPath);

		// Collecting the Json Files, parsing the json files and creating StockData
		// DStream
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
						}
						stockDataList.add(stockData);
					}
					return stockDataList.iterator();
				});

		// Creating JavaPairDStream with Key as "StockName" and Value containing Trading
		// Volume (e.g "Google", 1900.00)
		JavaPairDStream<String, Double> pairDStreamOfVolume = streams
				.mapToPair(new PairFunction<StockData, String, Double>() {

					@Override
					public Tuple2<String, Double> call(StockData stockData) throws Exception {
						return new Tuple2<String, Double>(stockData.getStockName(), stockData.getVolume());
					}
				});

		// Computing Sum of trading volume for each stock. The resultant PairDStream
		// will be Key as "StockName" and Value containing Sum of trading volume
		// (e.g "Google", 8800.00)
		JavaPairDStream<String, Double> sumOfTradingVolume = pairDStreamOfVolume
				// .window(Durations.seconds(240), Durations.seconds(120))
				.reduceByKey((x, y) -> (x + y));

		/*
		 * As Java RDD does contain any in built function for sorting based the key,
		 * swap the pair of StockName, TradingVolume. After swap operation it will
		 * become TradingVolume, StockName
		 */
		JavaPairDStream<Double, String> swappedPair = sumOfTradingVolume.mapToPair(m -> m.swap());

		// Sort by descending order based on the key i.e. Trading Volume so that the
		// highest trading volume comes first
		JavaPairDStream<Double, String> sortedValuesOfTradingVolume = swappedPair
				.transformToPair(s -> s.sortByKey(false));

		// Once the trading volume is sorted in descending order, loop through the RDD,
		// take
		// the first element and print it. The files will be created under
		// "C:\\temp\\OutputFiles\\TradingVolume\\"
		// and the file name will be "HighestTradingVolume" + "current time in
		// milliseconds"
		// e.g. "HighestTradingVolume1537377121311.txt"
		sortedValuesOfTradingVolume.foreachRDD(rdd -> {
			String highestTradingVolume;
			Tuple2<Double, String> val = rdd.take(1).get(0);

			highestTradingVolume = "Stock: -> " + val._2() + " , Highest Trading Volume: -> " + val._1();

			String messageId = String.valueOf(System.currentTimeMillis());
			String fileName = "HighestTradingVolume" + messageId + ".txt";
			
			try (PrintWriter out = new PrintWriter(outputPath + "//" + fileName)) {
				out.println(highestTradingVolume);
			}
		});

		// Start the Java Spark Context
		jssc.start();

		// Awaiting termination
		jssc.awaitTermination();

		// Spark Context terminated
		jssc.close();
	}
	
		// Helper method to create an output directory if it does not exists. This is used since PrintWriter is used to
		// create the output files instead of SaveAsTextFile()
		private static void createDirectoryIfNotExists(String outputPath)
		{
			File directory = new File(String.valueOf(outputPath));
			if(!directory.exists())
			{
				boolean result = directory.mkdirs();
			}
		}
}
