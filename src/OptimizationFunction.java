import java.util.List;

import DataModel.Query;
import DataModel.Workload;

public class OptimizationFunction {		
	public static double lambda = 0.3;
	public static double epsilon = 0.0;
	
	public static boolean workloadMeetsRequirements(Workload w, double P, Integer config) {
		double movingAverage = getEWMA(w.getQueries(), config, w.getQueries().size() - 1);
		System.out.printf("%.2f",movingAverage);
		return (movingAverage >= P);
	}

	private static double getEWMA(List<Query> queries, int config, int i) {
		Query current = queries.get(i);
		double v_i = 0.0;
		if (current.getExpectedRuntime() + epsilon > current.getActualRuntime()) {
			v_i = 1.0;
		}

		if (i > 0) {
			return (lambda)*v_i + (1 - lambda)*(getEWMA(queries, config, i - 1));
		} else {
			return v_i;
		}
	}	
}