package DataModel;

import java.util.ArrayList;
import java.util.List;

public class Workload {
	private List<Query> queries;
	private String name;
	
	public Workload(String name) {
		queries = new ArrayList<Query>();
		this.name = name;
	}
	
	public void addQuery(Query q) {
		queries.add(q);
	}
	
	public List<Query> getQueries() {
		return queries;
	}
	
	public String getName() {
		return name;
	}
}