package com.bits.sparkstreamingproject;

import java.io.Serializable;

/* This class is a custom class for storing the Stock data related properties */
public class StockData implements Serializable {

	private String stockName;
	private Double volume;
	private Double highPrice;
	private Double lowPrice;
	private Double closingPrice;
	private Double openingPrice;
	
	@Override
	public String toString()
	{
		return stockName + "," + volume + "," + highPrice + "," + lowPrice + ","
				+ closingPrice + "," + openingPrice;
	}
	
	public void setStockName(String stockName){
	    this.stockName = stockName;
	}

	public String getStockName(){
	    return this.stockName;
	}

	public void setVolume(Double volume){
	    this.volume = volume;
	}

	public Double getVolume(){
	    return this.volume;
	}
	
	public void setHighPrice(Double highPrice){
	    this.highPrice = highPrice;
	}

	public Double getHighPrice(){
	    return this.highPrice;
	}
	
	public void setLowPrice(Double lowPrice){
	    this.lowPrice = lowPrice;
	}

	public Double getLowPrice(){
	    return this.lowPrice;
	}
	
	public void setClosingPrice(Double closingPrice){
	    this.closingPrice = closingPrice;
	}

	public Double getClosingPrice(){
	    return this.closingPrice;
	}
	
	public void setOpeningPrice(Double openingPrice){
	    this.openingPrice = openingPrice;
	}

	public Double getOpeningPrice(){
	    return this.openingPrice;
	}
}
