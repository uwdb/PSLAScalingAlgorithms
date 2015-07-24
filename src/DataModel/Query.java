package DataModel;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;

public class Query {
	private JsonObject json;
	private String queryName;
	private double expectedRuntime; // In seconds
	private double actualRuntime; // In seconds
	private int ranOnConfigSize;
	private int queryID;
	
	//new variables
	private int small_that_worked = 0;
	private int large_that_missed = 0;
	
	private Map<Integer, Double> runtimes = new HashMap<Integer, Double>();
	
	public Query(JsonObject json) {
		this.json = json;
	}
	
	public JsonObject getJson() {
		return json;
	}
	
	public double getExpectedRuntime() {
		return expectedRuntime;
	}
	
	public void setExpectedRuntime(double time) {
		this.expectedRuntime = time;
	}
	
	public double getActualRuntime() {
		return actualRuntime;
	}
	
	public void setRanOnConfigSize(int size) {
		this.ranOnConfigSize = size;
	}
	
	public double getRanOnConfigSize() {
		return ranOnConfigSize;
	}
	
	public void setActualRuntime(double time) {
		this.actualRuntime = time;
	}
	
	public String getQueryName() {
		return queryName;
	}
	
	public void setQueryName(String name) {
		this.queryName = name;
	}
	
	public void setQueryID(int id) {
		this.queryID = id;
	}
	
	public int getQueryID() {
		return queryID;
	}
	
	public void setTime(Integer config, Double time) {
		this.runtimes.put(config,  time);
	}
	
	public Double getTime(Integer config) {
		return this.runtimes.get(config);
	}
	
	public void setSmallestWorkingConfig(int s){
		small_that_worked = s;
	}
	
	public int getSmallestWorkingConfig(){
		return small_that_worked;
	}
	
	public int getLargestMissedConfig(){
		return large_that_missed;
	}
	
	public void setLargestMissedConfig(int l){
		large_that_missed = l;
	}
}