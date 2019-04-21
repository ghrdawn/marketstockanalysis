package com.bits.sparkstreamingproject;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;
import scala.Tuple4;

/* This class computes the average closing price and average opening price for each stock.
 * Based on this, it computes the maximum profit out of four stocks */
public class ComputeMaximumProfitOfStocks {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ComputeMaximumProfitOfStocks");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaDStream<String> inputString = jssc
				.textFileStream(args[1]);

		String outputPath = args[2];
		
		createDirectoryIfNotExists(outputPath);
		
		// Collecting the JSon Files, parsing the json files and creating StockData
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
		// of ClosingStockPrice, 1 (e.g "Google", Tuple<1900.00, 1>)
		JavaPairDStream<String, Tuple2<Double, Long>> pairDStreamOfClosingPrice = streams
				.mapToPair(new PairFunction<StockData, String, Tuple2<Double, Long>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(StockData stockData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(stockData.getStockName(),
								new Tuple2<Double, Long>(stockData.getClosingPrice(), (long) 1));
					}

				});

		
		// Creating JavaPairDStream with Key as "StockName" and Value containing tuple
		// of OpeningStockPrice, 1 (e.g "Google", Tuple<1900.00, 1>)
		JavaPairDStream<String, Tuple2<Double, Long>> pairDStreamOfOpeningPrice = streams
				.mapToPair(new PairFunction<StockData, String, Tuple2<Double, Long>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Long>> call(StockData stockData) throws Exception {
						return new Tuple2<String, Tuple2<Double, Long>>(stockData.getStockName(),
								new Tuple2<Double, Long>(stockData.getOpeningPrice(), (long) 1));
					}

				});

		
		// Computing Sum of Closing Price for each stock. The resultant PairDStream will
		// be Key as "StockName"
		// and Value containing tuple of Sum of StockPrice, Sum of elements for each
		// stock (e.g "Google", Tuple<1900.00, 5>)
		JavaPairDStream<String, Tuple2<Double, Long>> sumOfClosingPrice = pairDStreamOfClosingPrice.reduceByKey(
				(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
		
		// Computing Sum of Opening Price for each stock. The resultant PairDStream will
		// be Key as "StockName"
		// and Value containing tuple of Sum of StockPrice, Sum of elements for each
		// stock (e.g "Google", Tuple<1900.00, 5>)
		JavaPairDStream<String, Tuple2<Double, Long>> sumOfOpeningPrice = pairDStreamOfOpeningPrice.reduceByKey(
				(tuple1, tuple2) -> new Tuple2<Double, Long>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
		
		// Computing the average for Closing Stock Price based on the PairDStream
		// obtained above. The
		// resultant PairDStream will be StockName, Average Closing Stock Price
		JavaPairDStream<String, Double> averageClosingStockPrice = sumOfClosingPrice
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double>() {

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Long>> tuple) throws Exception {
						Tuple2<Double, Long> val = tuple._2;
						Double total = val._1;
						Long count = val._2;
						Double average = (Double) (total / count);
						return new Tuple2<String, Double>(tuple._1, average);
					}
				});
		
		// Computing the average for Opening Stock based on the PairDStream obtained
		// above. The
		// resultant PairDStream will be StockName, Average Opening Stock Price
		JavaPairDStream<String, Double> averageOpeningStockPrice = sumOfOpeningPrice
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Long>>, String, Double>() {

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Long>> tuple) throws Exception {
						Tuple2<Double, Long> val = tuple._2;
						Double total = val._1;
						Long count = val._2;
						Double average = (Double) (total / count);
						return new Tuple2<String, Double>(tuple._1, average);
					}
				});
		
		// Join the Average Closing Price and Average Opening price, The resultant
		// PairDStream will contain StockName,
		// (AverageClosingPrice, AverageOpeningPrice)
		JavaPairDStream<String, Tuple2<Double, Double>> joinedDStream = averageClosingStockPrice
				.join(averageOpeningStockPrice);
		
		// Subtract AverageClosingPrice - AverageOpeningPrice for each stock. If the
		// profit < 0, 0.00 is added into the stream.
		// The resultant JavaPairDStream will be StockName, Net profit
		JavaPairDStream<String, Double> stockProfit = joinedDStream
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
						Tuple2<Double, Double> val = tuple._2;
						Double profit = val._1 - val._2;
						Double netProfit = profit > 0 ? profit : 0.00;

						return new Tuple2<String, Double>(tuple._1, netProfit);
					}
				});
		
		// Finally to find out the maximum profit out of four stocks, swap the stock
		// name and net profit so that the
		// net profit can be sorted by key in the descending order
		JavaPairDStream<Double, String> sortedProfit = stockProfit.mapToPair(m -> m.swap())
				.transformToPair(t -> t.sortByKey(false));
		
		// Once the net profit is sorted in descending order, loop through the RDD, take
		// the first element and print it
		// If all the stocks contain "0.0" then
		// "No profitable stock found during last 10 minutes" will be printed in the
		// output file.
		// The files will be created under
		// "C:\\temp\\OutputFiles\\MaximumProfit\\"
		// and the file name will be "MaximumProfit" + "current time in
		// milliseconds"
		// e.g. "MaximumProfit1537381201596.txt".
		sortedProfit.foreachRDD(rdd -> {
			String profit;
			Tuple2<Double, String> val = rdd.take(1).get(0);
			
			String message = "No profitable stock found during last 10 minutes";
			profit = (val._1 > 0) ? "Stock: -> " + val._2() + " , Profit: -> " + val._1() : message;

			String messageId = String.valueOf(System.currentTimeMillis());
			String fileName = "MaximumProfit" + messageId + ".txt";
			
			try (PrintWriter out = new PrintWriter(outputPath + "\\" + fileName)) {
				out.println(profit);
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
