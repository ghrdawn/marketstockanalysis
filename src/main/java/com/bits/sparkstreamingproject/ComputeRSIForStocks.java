package com.bits.sparkstreamingproject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

/* This class computes the Relative Strength Index for each stock. */
public class ComputeRSIForStocks {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("ComputeRSIForStocks");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		JavaDStream<String> inputString = jssc.textFileStream(args[1]);
		
		//Output Path
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


		//Create a PairDStream with Closing Price - OpeningPrice for each stock
		JavaPairDStream<String, Double> stockProfitRDD = streams
				.mapToPair(f -> new Tuple2(f.getStockName(), (f.getClosingPrice() - f.getOpeningPrice())));
		
		// Filter the gains into one pairDStream 
		JavaPairDStream<String, Double> gains = stockProfitRDD.filter(f -> f._2 > 0).reduceByKey((x, y) -> x + y);
		
		// Filter the loses into one pairDStream
		JavaPairDStream<String, Double> loses = stockProfitRDD.filter(f -> f._2 < 0)
				.reduceByKey((x, y) -> Math.abs(x) + Math.abs(y));
		
		// Join the gains with loses
		JavaPairDStream<String, Tuple2<Double, Double>> avgProfitAndLoses = gains.join(loses);
		
		/*
		 * RSI Calculation
		 * First the average gain and loss is computed by dividing by 10. Then relative strength is calculated if average loss 
		 * is not equal to 0. Then the RSI calculation formula 100 - (100 / (1 + RS)) is applied to calculate the RSI.
		 */
		JavaPairDStream<String, Double> stockProfit = avgProfitAndLoses
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>() {

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
						Tuple2<Double, Double> val = tuple._2;
						Double RS = 0.0;
						Double avgGain = val._1 / 10;
						Double avgLoss = val._2 / 10;
						
						if (avgLoss != 0) {
							RS = avgGain / avgLoss;
						}
						
						Double RSI = 100 - (100 / (1 + RS));

						return new Tuple2<String, Double>(tuple._1, RSI);
					}
				});

		// Printing the RSI. A folder named "RSICalculation" + current time in
		// milliseconds e.g. "RSICalculation1538077081922"
		// will be created inside "C:\temp\OutputFiles\RSICalculationForStocks\" for each window and the
		// output file "part-00000" will be available
		stockProfit.foreachRDD(rdd -> { 
			String messageId = String.valueOf(System.currentTimeMillis());
		  rdd.coalesce(1).saveAsTextFile(outputPath + messageId); 
		  }
		);

		// Start the Java Spark Context
		jssc.start();

		// Awaiting termination
		jssc.awaitTermination();

		// Spark Context terminated
		jssc.close();
	}
	
}
