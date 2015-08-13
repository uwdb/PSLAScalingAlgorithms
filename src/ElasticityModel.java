import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Utils.FileReaderUtils;
import DataModel.Cluster;
import DataModel.Query;
import DataModel.Workload;

public interface ElasticityModel {	
	// Add a finished query to the model to adapt the model
	public void addNewDataPoint(Query q);

	// Returns a negative number if the cluster should scale down
	// Returns a positive number if the cluster should scale up
	// Returns 0 if the cluster should not scale
	public double shouldScaleCluster();

	public void outputData();

	public String getName();
}

class Tuple<Q, D> { 
	public final Q q; 
	public final D score; 

	public Tuple(Q q, D score) { 
		this.q = q; 
		this.score = score; 
	} 
} 

/***
 * See paper
 */
class SimpleElasticity implements ElasticityModel {
	private double score = 0.0;
	public List<Tuple<Query, Double>> queries = new ArrayList<Tuple<Query, Double>>();

	public String getName() {
		return "Basic Scalability";
	}

	public void addNewDataPoint(Query q) {
		if (q.getExpectedRuntime() > q.getActualRuntime()) {
			// Query is running fast
			score = -1.0;
		} else {
			// Query is running slow
			score = 1.0;
		}

		queries.add(new Tuple<Query, Double>(q, score));
	}

	public double shouldScaleCluster() {
		return score;
	}

	public void outputData() {
		System.out.println("expected,actual,score");
		for (Tuple<Query, Double> tuple: queries) {
			System.out.println(tuple.q.getQueryName() + "," +
					tuple.q.getExpectedRuntime() + ","
					+ tuple.q.getActualRuntime() + ","
					+ tuple.score);
		}
	}
}

/***
 */
class WindowedElasticity implements ElasticityModel {
	private double score = 0.0;
	public List<Tuple<Query, Double>> queries = new ArrayList<Tuple<Query, Double>>();
	public List<Query> window = new ArrayList<Query>();

	int windowSize = 5;
	int hitQueries = 0;
	double delta = 0.1;
	double P = 0.8;

	public String getName() {
		return "Windowed Elasticity";
	}

	public void addNewDataPoint(Query q) {
		// usually, the score will be 0.0, only at the end of
		// a window is a possible for the cluster to scale
		score = 0.0;

		window.add(q);
		if (q.getExpectedRuntime() >= q.getActualRuntime()) {
			hitQueries++;
		}

		// Only evaluate at the end of a window
		if (window.size() == windowSize) {
			double currentP = hitQueries / (double) windowSize;
			if (currentP > (P + delta)) {
				score = -1.0; // Try scaling down
			} else if (currentP < P) {
				// We aren't hitting our target, scale up
				score = 1.0;
			} else {
				score = 0.0;
			}

			window = new ArrayList<Query>();
			hitQueries = 0;
		}

		// For data output at the end
		queries.add(new Tuple<Query, Double>(q, score));
	}

	public double shouldScaleCluster() {
		return score;
	}

	public void outputData() {
		System.out.println("expected,actual,score");
		for (Tuple<Query, Double> tuple: queries) {
			System.out.println(tuple.q.getQueryName() + "," +
					tuple.q.getExpectedRuntime() + ","
					+ tuple.q.getActualRuntime() + ","
					+ tuple.score);
		}
	}
}

/***
 * See paper
 */
class WeightedWindowedElasticity implements ElasticityModel {
	private double score = 0.0;
	public List<Tuple<Query, Double>> queries = new ArrayList<Tuple<Query, Double>>();
	public List<Query> window = new ArrayList<Query>();

	int s = 10000;
	int windowSize = 10;
	int hitQueries = 0;
	double delta = 0.1;
	double P = 0.8;

	public String getName() {
		return "Weighted Window Elasticity";
	}

	public void addNewDataPoint(Query q) {
		// usually, the score will be 0.0, only at the end of
		// a window is a possible for the cluster to scale
		score = 0.0;

		if (q.getActualRuntime() > s) {
			window.add(q);
			if (q.getExpectedRuntime() >= q.getActualRuntime()) {
				hitQueries++;
			}
		}

		// Only evaluate at the end of a window
		if (window.size() == windowSize) {
			double currentP = hitQueries / (double) windowSize;
			if (currentP > (P + delta)) {
				score = -1.0; // Try scaling down
			} else if (currentP < P) {
				// We aren't hitting our target, scale up
				score = 1.0;
			} else {
				score = 0.0;
			}

			window = new ArrayList<Query>();
		}

		// For data output at the end
		queries.add(new Tuple<Query, Double>(q, score));
	}


	public double shouldScaleCluster() {
		return score;
	}

	public void outputData() {
		System.out.println("expected,actual,score");
		for (Tuple<Query, Double> tuple: queries) {
			System.out.println(tuple.q.getQueryName() + "," +
					tuple.q.getExpectedRuntime() + ","
					+ tuple.q.getActualRuntime() + ","
					+ tuple.score);
		}
	}
}

/***
 * See paper
 */
class EWMAElasticity implements ElasticityModel {
	private double score = 0.0;
	public List<Query> queries = new ArrayList<Query>();

	int queryCount = 0;
	int windowSize = 5;
	double Z = 0.0;
	double lambda = 0.3;
	double lowerBound = 0.3;
	double upperBound = 0.6;
	boolean haveHit = false;

	public String getName() {
		return "EWNA Elasticity";
	}

	public void addNewDataPoint(Query q) {
//		if (!haveHit) {
//			if (q.getExpectedRuntime() > q.getActualRuntime()) {
//				haveHit = true;
//				score = 0.0;
//			} else {
//				score = 1.0;
//			}
//		} else {
			queries.add(q);
			queryCount++;

			if (queryCount == windowSize) {
				queryCount = 0;

				Z = findZ(queries.size() - 1);

				if (Z < lowerBound) { //scale up
					score = 1.0;
				} else if (Z > upperBound) { //scale down
					score = -1.0;
				} else {
					score = 0.0;
				}
			}
			else {
				score = 0.0;
			}
		//}
	}


	private double findZ(int i) {
		Query current = queries.get(i);
		double v_i = 0.0;
		if (current.getExpectedRuntime() > current.getActualRuntime()) {
			v_i = 1.0;
		}

		if (i > 0) {
			return (lambda)*v_i + (1 - lambda)*(findZ(i - 1));
		} else {
			return v_i;
		}
	}

	public double shouldScaleCluster() {
		return score;
	}

	public void outputData() {
		System.out.println("Not implemented");
	}
}


/***
 * See Paper
 */
class DirectHopElasticity {	
	public List<Query> completedWorkload = new ArrayList<Query>();
	public List<Integer> configs = new ArrayList<Integer>();
	int scaleTo = 0;
	double P = 0.9;
	int queryCount = 0;
	int windowSize = 1;

	public DirectHopElasticity(Cluster cluster) {
		this.configs = new ArrayList<Integer>();
		int skipFactor = cluster.getSkipFactor();
		for (int i = cluster.getMinInstances(); i <= cluster.getMaxInstances(); i+=skipFactor) {
			this.configs.add(i);
		}
	}

	public String getName() {
		return "Direct Hop Elasticity";
	}

	Map<Integer, List<Double>> M1= new HashMap<Integer, List<Double>>();
	Map<Integer, List<Double>> M2= new HashMap<Integer, List<Double>>();
	
	public void addNewDataPoint(Query q, int currentClusterSize, boolean staticTimes, String keyFile) {

		//on higher folder
		M1 = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/queries_" + keyFile + "estimated_estimated_m1.csv");
		
		//iterative predictions
		if(!staticTimes) {
			M2.clear();
			M2 = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/predictions/model_M2/_M2_prediction_results_selected.csv");
		}
		
	
		completedWorkload.add(q);
		q.setRanOnConfigSize(currentClusterSize);
		queryCount++;

		if (queryCount == windowSize) {
			System.out.print("\t \t || EWMAs for workload after this query || ");
			
			int bestConfig = 0;
			double bestConfigScore = 0;
			
			for (Integer config : configs) {
				scaleTo = config;

				Workload estimatedWorkload = new Workload("temp");
				int qNum = 0;
				for (Query query : completedWorkload) {
					Query estimatedQuery = new Query(query.getJson());
					//for linear
					//double estimatedTime = query.getActualRuntime() * (query.getRanOnConfigSize() / config);
					
					double estimatedTime_M1 = M1.get(qNum).get((config - 4) / 2);
					

					if(query.getRanOnConfigSize() == config) {
						
						estimatedQuery.setActualRuntime(query.getActualRuntime());
					}
					else {
						if(!staticTimes) {
								double estimatedTime_M2 = M2.get(qNum).get((config - 4) / 2);
								estimatedQuery.setActualRuntime(estimatedTime_M2); //we have information about this
								
								if(estimatedTime_M1 != estimatedTime_M2){
									//System.out.print("------ DIFF " + estimatedTime_M1 + " "+ estimatedTime_M2);
								}
							}
						else{
							estimatedQuery.setActualRuntime(estimatedTime_M1);
						}
						
					}
					
					estimatedQuery.setExpectedRuntime(query.getExpectedRuntime());
					estimatedWorkload.addQuery(estimatedQuery);
					
					if(config == 4 && qNum == completedWorkload.size()-1){ 
						//System.out.print(" +++ Running on 4 for: " +  estimatedQuery.getActualRuntime() + " ++++ ");
					}
					
					//extra info
					estimatedQuery.setRanOnConfigSize((int) query.getRanOnConfigSize());
					estimatedQuery.setActualRuntimeOnConfig(query.getActualRuntime()); //which could be actual or estimated for other configs
					estimatedQuery.setSLATime(query.getExpectedRuntime());
					
					qNum++;
				}
				
				System.out.print(config + ": ");
				
				double currentScore = OptimizationFunction.workloadMeetsRequirements(estimatedWorkload, P, config);
					
				
				if(currentScore > bestConfigScore){
					bestConfigScore = currentScore;
					bestConfig = config;
				}
				
				if(currentScore >= P){
					// We have found the smallest suitable cluster
					queryCount= 0;
					System.out.println("");
					return;
				}
				
				if(config == 12) { //last chance, but back track if there was a better score
					queryCount = 0;
					scaleTo = bestConfig;
					System.out.println("");
					return;
				}
				
				
				System.out.print(", ");
			}
		} else {
			scaleTo = currentClusterSize;
		}
		System.out.println("");
		queryCount= 0;
		// If we finished the loop, scaleTo is set to the largest possible cluster size
	}

	public int scaleTo() {
		return scaleTo;
	}
}