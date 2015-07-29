import java.util.List;

import DataModel.Query;
import DataModel.Workload;

public class OptimizationFunction {		
	public static double lambda = 0.7;
	public static double z = .01;
	public static double y = .9;
	
	public static boolean workloadMeetsRequirements(Workload w, double P, Integer config) {
		double movingAverage = getEWMA(w.getQueries(), config, w.getQueries().size() - 1);
		System.out.printf("%.8f",movingAverage);
		return (movingAverage >= P);
	}

	private static double getEWMA(List<Query> queries, int config, int i) {
		Query current = queries.get(i);
		double v_i = 0.0;
		
	
		if(current.getExpectedRuntime() > current.getActualRuntime()){
			v_i = 1.0;
		}
		else //equal or missed
		{
			v_i = current.getExpectedRuntime()/current.getActualRuntime();	
		}
	
		//special conditions

		
		if(config < current.getRanOnConfigSize() && current.getSLARuntime() < current.getActualRuntimeOnConfig()){ //this config is less and missed at original config
			double score = current.getExpectedRuntime()/current.getActualRuntime();
			double penalty = z;
			v_i = score*penalty;
		}
		else if	(config != current.getRanOnConfigSize()){
			v_i *=y;
		}
	

		
		if (i > 0) {
			return (lambda)*v_i + (1 - lambda)*(getEWMA(queries, config, i - 1));
		} else {
			return v_i;
		}
	}	
}