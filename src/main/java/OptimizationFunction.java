import java.util.List;

import DataModel.Query;
import DataModel.Workload;

public class OptimizationFunction {		
	public static double lambda = 0.3;
	
	public static boolean workloadMeetsRequirements(Workload w, double P) {
		double movingAverage = getEWMA(w.getQueries(), w.getQueries().size() - 1);
		return (movingAverage >= P);
	}

	private static double getEWMA(List<Query> queries, int i) {
		Query current = queries.get(i);
		double v_i = 0.0;
		if (current.getExpectedRuntime() > current.getActualRuntime()) {
			v_i = 1.0;
		}

		if (i > 0) {
			return (lambda)*v_i + (1 - lambda)*(getEWMA(queries, i - 1));
		} else {
			return v_i;
		}
	}	
}