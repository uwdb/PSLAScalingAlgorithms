import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import DataModel.Cluster;
import DataModel.Query;
import DataModel.Workload;
import Utils.AWSClusterUtils;
import Utils.FileReaderUtils;

public class Main {
	private static Workload originalWorkload;
	private static Cluster cluster;	
	private static String masterDNS = "localhost";
	private static int startingClusterSize = 4;
	private static int maxClusterSize = 12;
	private static boolean clusterLaunched = true;

	public static void main(String[] args) {
		Scanner in = new Scanner(System.in);

		cluster = Cluster.getInstance();
		cluster.setMasterDNS(masterDNS);
		cluster.setInstanceBounds(startingClusterSize, maxClusterSize);
		originalWorkload = FileReaderUtils.readRandomQueries(startingClusterSize, "Four Node Workload");

		Usage();
		while (true) {
			System.out.print("Enter command: ");

			try {
				int option = in.nextInt();

				switch (option) {
				case 1:
					if (clusterLaunched) {
						System.out.println("Cluster already running");
						break;
					}

					FileReaderUtils.updateStarClusterFiles(maxClusterSize);

					AWSClusterUtils.launchCluster(cluster.getMaxInstances());
					System.out.println("Finished Launching Myria on AWS");

					clusterLaunched = true;
					System.exit(0);
				case 2:			
					System.out.println("Uploading and ingesting data into myria");
					AWSClusterUtils.uploadAndIngestData();
					break;
				case 3:					
					cluster.sendTestQuery(originalWorkload.getQueries().get(0));
					break;
				case 4:
					cluster.checkNumWorkersAlive();
					break;
				case 5:
					cluster.startWorker();
					break;
				case 6:
					cluster.stopWorker();
					break;
				case 7:
					//utilityExperiment(100);
					//runSingleQueryExperiment(1, 5);
					//directHopExperiment();
					//timingRun();
					//convergenceTimingTest();
					//convergenceTimingBATCH();
					convergenceTimingDirectBATCH();
					//convergenceTimingTestDirect();
					//simulatedExperiment();
					//simulatedExperimentDirect();
					break;
				case 8:
					System.out.println("Goodbye");
					in.close();
					System.exit(0);
				default:
					Usage();
				}

			} catch (Exception e) {
				in.close();
				e.printStackTrace();;
				System.exit(1);
			}
		}
	}

	@SuppressWarnings("unused")
	private static void directHopExperiment() {
		cluster.verbose = false;
		int sampleSize = 10;

		// Read in workloads. We have separate workloads for each configuration to
		// utilize data on each machine.
		ArrayList<Workload> slowWorkloads = new ArrayList<Workload>();
		slowWorkloads.add(FileReaderUtils.readRandomQueries(4, "4 Node Slow"));
		slowWorkloads.add(FileReaderUtils.readRandomQueries(5, "5 Node Slow"));
		slowWorkloads.add(FileReaderUtils.readRandomQueries(6, "6 Node Slow"));
		slowWorkloads.add(FileReaderUtils.readRandomQueries(7, "7 Node Slow"));
		slowWorkloads.add(FileReaderUtils.readRandomQueries(8, "8 Node Slow"));

		ArrayList<Workload> fastWorkloads = new ArrayList<Workload>();
		fastWorkloads.add(FileReaderUtils.readRandomQueries(4, "4 Node Slow"));
		fastWorkloads.add(FileReaderUtils.readRandomQueries(5, "5 Node Slow"));
		fastWorkloads.add(FileReaderUtils.readRandomQueries(6, "6 Node Slow"));
		fastWorkloads.add(FileReaderUtils.readRandomQueries(7, "7 Node Slow"));
		fastWorkloads.add(FileReaderUtils.readRandomQueries(8, "8 Node Slow"));

		PrintWriter writer = null;
		try {
			cluster.scaleToMinSize();
			Thread.sleep(5000);

			writer = new PrintWriter("outputDirect.csv", "UTF-8");

			AWSClusterUtils.clearCache();
			cluster.runCacheWarmer();

			// First, get the expected time of each query by running the query
			// multiple times on the 4 node cluster and averaging the result'
			writer.println("----------------Timing run----------------");
			System.out.println("----------------Timing run----------------");
			int counter = 0;
			for (int i = 0; i < sampleSize; i++) {
				Query q = originalWorkload.getQueries().get(i);
				double elapsedRuntime1 = cluster.executeQuery(q);
				double elapsedRuntime2 = cluster.executeQuery(q);
				double elapsedRuntime3 = cluster.executeQuery(q);

				double slowRuntime = (elapsedRuntime1 + elapsedRuntime2 + elapsedRuntime3) / 5;
				double fastRuntime = (elapsedRuntime1 + elapsedRuntime2 + elapsedRuntime3) / 3;

				for (Workload slowWorkload : slowWorkloads) {
					slowWorkload.getQueries().get(i).setExpectedRuntime(slowRuntime);
				}

				for (Workload fastWorkload : fastWorkloads) {
					fastWorkload.getQueries().get(i).setExpectedRuntime(fastRuntime);
				}

				System.out.println("Finished timing query number " + String.valueOf(counter));
				counter++;
			}

			int queryNum = 1;
			DirectHopElasticity directModel = new DirectHopElasticity(cluster);
			writer.println("queryNum,expected,actual,score,workers");

			// Slow run
			for (int queryIndex = 0; queryIndex < 2; queryIndex++) {
				int startingNumWorkers = cluster.getNumWorkersAlive();
				int workloadIndex = (cluster.getNumWorkersAlive() - 4);
				Query q = slowWorkloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = cluster.executeQuery(q);
				q.setActualRuntime(elapsedRuntime);

				//directModel.addNewDataPoint(q, startingNumWorkers, queryIndex);
				int scaleTo = directModel.scaleTo();

				int x = startingNumWorkers;
				if (scaleTo > startingNumWorkers) {
					while (x < scaleTo) {
						cluster.startWorker();
						Thread.sleep(5000);
						x++;
					}

					cluster.runCacheWarmer();
				} else if (scaleTo < startingNumWorkers){
					while (x > scaleTo) {
						cluster.stopWorker();
						Thread.sleep(5000);
						x--;
					}

					cluster.runCacheWarmer();
				}

				writer.println(String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(scaleTo) + "," + 
						String.valueOf(startingNumWorkers));

				queryNum++;
			}

			// Fast run
			for (int k = 0; k < 2; k++) {
				for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
					int startingNumWorkers = cluster.getNumWorkersAlive();
					int workloadIndex = (cluster.getNumWorkersAlive() - 4);
					Query q = fastWorkloads.get(workloadIndex).getQueries().get(queryIndex);

					double elapsedRuntime = cluster.executeQuery(q);
					q.setActualRuntime(elapsedRuntime);

					//directModel.addNewDataPoint(q, startingNumWorkers, queryIndex);
					int scaleTo = directModel.scaleTo();

					int x = startingNumWorkers;
					if (scaleTo > startingNumWorkers) {
						while (x < scaleTo) {
							cluster.startWorker();
							Thread.sleep(5000);
							x++;
						}

						cluster.runCacheWarmer();
					} else if (scaleTo < startingNumWorkers){
						while (x > scaleTo) {
							cluster.stopWorker();
							Thread.sleep(5000);
							x--;
						}

						cluster.runCacheWarmer();
					}

					writer.println(String.valueOf(queryNum) + "," + 
							String.valueOf(q.getExpectedRuntime()) + "," +
							String.valueOf(elapsedRuntime) + "," +
							String.valueOf(scaleTo) + "," + 
							String.valueOf(startingNumWorkers));

					queryNum++;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			writer.close();
		}

		cluster.verbose = true;		
	}

	@SuppressWarnings("unused")
	private static void runSingleQueryExperiment(int queryNum, int scaleFactor) {		
		cluster.verbose = false;
		int sampleSize = 50;

		PrintWriter writer = null;
		try {
			cluster.scaleToMinSize();
			Thread.sleep(5000);

			writer = new PrintWriter("output" + String.valueOf(queryNum) + ".csv", "UTF-8");

			// First, get the expected time of each query by running the query
			// multiple times on the 4 node cluster and averaging the result'
			writer.println("----------------Timing run----------------");
			System.out.println("----------------Timing run----------------");

			String queryJSONPath = "/Users/jortiz16/Documents/myriascalabilityengine/queries/4" 
					+ "/json"
					+ queryNum
					+ ".json";

			AWSClusterUtils.clearCache();
			cluster.runCacheWarmer();

			Query originalQuery = new Query(FileReaderUtils.readJSON(queryJSONPath));

			double elapsedRuntime1 = cluster.executeQuery(originalQuery);
			double elapsedRuntime2 = cluster.executeQuery(originalQuery);
			double elapsedRuntime3 = cluster.executeQuery(originalQuery);

			double avgRuntime = (elapsedRuntime1 + elapsedRuntime2 + elapsedRuntime3) / scaleFactor;
			originalQuery.setExpectedRuntime(avgRuntime);

			writer.println("Timed query: " + avgRuntime);

			ArrayList<ElasticityModel> models = new ArrayList<ElasticityModel>();
			//models.add(new SimpleElasticity());
			models.add(new WindowedElasticity());
			models.add(new EWMAElasticity());

			int queryCount = 1;
			for (ElasticityModel model : models) {
				writer.println("\n-------------" + model.getName() + "--------------");
				System.out.println("\n-------------" + model.getName() + "--------------");
				writer.println("queryNum,expected,actual,score,workers");

				for (int i = 0; i < sampleSize; i++) {
					int startingNumWorkers = cluster.getNumWorkersAlive();
					String currentJsonPath = "/Users/jortiz16/Documents/myriascalabilityengine/queries/"
							+ String.valueOf(startingNumWorkers)
							+ "/json"
							+ String.valueOf(queryNum)
							+ ".json";

					Query currentQuery = new Query(FileReaderUtils.readJSON(currentJsonPath));
					double elapsedRuntime = cluster.executeQuery(currentQuery);

					currentQuery.setExpectedRuntime(originalQuery.getExpectedRuntime());
					currentQuery.setActualRuntime(elapsedRuntime);

					model.addNewDataPoint(currentQuery);

					if (model.shouldScaleCluster() > 0.0) {
						cluster.startWorker();
						cluster.startWorker();						
						Thread.sleep(5000); // wait for worker to start
						cluster.runCacheWarmer();
					} else if (model.shouldScaleCluster() < 0.0) {
						cluster.stopWorker();
						cluster.stopWorker();
						Thread.sleep(5000); // wait for worker to stop
						cluster.runCacheWarmer();
					} else {
						// System has no need to scale at this time
					}

					writer.println(String.valueOf(queryCount) + "," + 
							String.valueOf(currentQuery.getExpectedRuntime()) + "," +
							String.valueOf(elapsedRuntime) + "," +
							String.valueOf(model.shouldScaleCluster()) + "," + 
							String.valueOf(startingNumWorkers));

					queryCount++;
				}

				// Scale back down for next model
				while (cluster.getNumWorkersAlive() > cluster.getMinInstances()) {
					cluster.stopWorker();
					Thread.sleep(5000);
				}

				queryCount = 1;
			}

			// Finally test the direct hop model, which doens't use same interface
			cluster.runCacheWarmer();
			DirectHopElasticity directModel = new DirectHopElasticity(cluster);
			writer.println("queryNum,expected,actual,scaleTo,workers");

			writer.println("\n-------------Direct Hop--------------");
			System.out.println("\n-------------Direct Hop--------------");
			for (int i = 0; i < sampleSize; i++) {
				int startingNumWorkers = cluster.getNumWorkersAlive();
				String currentJsonPath = "/Users/jortiz16/Documents/myriascalabilityengine/queries/"
						+ String.valueOf(startingNumWorkers)
						+ "/json"
						+ String.valueOf(queryNum)
						+ ".json";

				Query currentQuery = new Query(FileReaderUtils.readJSON(currentJsonPath));
				double elapsedRuntime = cluster.executeQuery(currentQuery);

				currentQuery.setExpectedRuntime(originalQuery.getExpectedRuntime());
				currentQuery.setActualRuntime(elapsedRuntime);

				//directModel.addNewDataPoint(currentQuery, startingNumWorkers, i);
				int scaleTo = directModel.scaleTo();

				int x = startingNumWorkers;
				if (scaleTo > startingNumWorkers) {
					while (x < scaleTo) {
						cluster.startWorker();
						x++;
					}

					// Wait for cluster to get to the right size
					Thread.sleep(5000);
					cluster.runCacheWarmer();
				} else if (scaleTo < startingNumWorkers){
					while (x > scaleTo) {
						cluster.stopWorker();
						x--;
					}

					// Wait for cluster to get to the right size
					Thread.sleep(5000);
					cluster.runCacheWarmer();
				}

				System.out.println(String.valueOf(queryCount) + "," + 
						String.valueOf(currentQuery.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(scaleTo) + "," + 
						String.valueOf(startingNumWorkers));
				writer.println(String.valueOf(queryCount) + "," + 
						String.valueOf(currentQuery.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(scaleTo) + "," + 
						String.valueOf(startingNumWorkers));

				queryCount++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			writer.close();
		}

		cluster.verbose = true;
	}

	@SuppressWarnings("unused")
	private static void utilityExperiment(int sampleSize) {
		cluster.verbose = false;

		// Read in workloads. We have separate workloads for each configuration to
		// utilize data on each machine.
		ArrayList<Workload> workloads = new ArrayList<Workload>();
		workloads.add(FileReaderUtils.readRandomQueries(4, "Four Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(6, "Six Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(8, "Eight Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(10, "Ten Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(12, "Twelve Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(14, "Fourteen Node Workload"));
		workloads.add(FileReaderUtils.readRandomQueries(16, "Sixteen Node Workload"));

		PrintWriter writer = null;
		try {
			cluster.scaleToMinSize();
			Thread.sleep(5000);

			writer = new PrintWriter("output.csv", "UTF-8");

			AWSClusterUtils.clearCache();
			cluster.runCacheWarmer();

			// First, get the expected time of each query by running the query
			// multiple times on the 4 node cluster and averaging the result'
			writer.println("----------------Timing run----------------");
			System.out.println("----------------Timing run----------------");
			int counter = 0;
			for (int i = 0; i < sampleSize; i++) {
				Query q = originalWorkload.getQueries().get(i);
				double elapsedRuntime1 = cluster.executeQuery(q);
				double elapsedRuntime2 = cluster.executeQuery(q);
				double elapsedRuntime3 = cluster.executeQuery(q);

				double avgRuntime = (elapsedRuntime1 + elapsedRuntime2 + elapsedRuntime3) / 6;
				q.setExpectedRuntime(avgRuntime);

				writer.println("Timed query: " + q.getQueryName() + ", " + avgRuntime);
				System.out.println("Finished timing query number " + String.valueOf(counter));
				counter++;
			}


			ArrayList<ElasticityModel> models = new ArrayList<ElasticityModel>();
			//models.add(new SimpleElasticity());
			models.add(new WindowedElasticity());
			models.add(new WeightedWindowedElasticity());
			models.add(new EWMAElasticity());

			writer.println("\n\n----------------Scalabiltiy Experiment------------------");
			System.out.println("\n\n----------------Scalabiltiy Experiment------------------");

			int queryNum = 1;
			for (ElasticityModel model : models) {
				writer.println("\n-------------" + model.getName() + "--------------");
				System.out.println("\n-------------" + model.getName() + "--------------");
				writer.println("expected,actual,score,workers");

				for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
					int startingNumWorkers = cluster.getNumWorkersAlive();
					int workloadIndex = (cluster.getNumWorkersAlive() - 4) / 2;
					Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);
					Query originalQuery = originalWorkload.getQueries().get(queryIndex);

					double elapsedRuntime = cluster.executeQuery(q);

					q.setExpectedRuntime(originalQuery.getExpectedRuntime());
					q.setActualRuntime(elapsedRuntime);

					model.addNewDataPoint(q);

					if (model.shouldScaleCluster() > 0.0) {
						cluster.startWorker();
						cluster.startWorker();
						Thread.sleep(5000); // wait for worker to start
						cluster.runCacheWarmer();
					} else if (model.shouldScaleCluster() < 0.0) {
						cluster.stopWorker();
						cluster.stopWorker();
						Thread.sleep(5000); // wait for worker to stop
						cluster.runCacheWarmer();
					} else {
						// System has no need to scale at this time
					}

					writer.println(String.valueOf(queryNum) + "," + 
							String.valueOf(q.getExpectedRuntime()) + "," +
							String.valueOf(elapsedRuntime) + "," +
							String.valueOf(model.shouldScaleCluster()) + "," + 
							String.valueOf(startingNumWorkers));

					queryNum++;
				}

				// Scale back down for next model
				while (cluster.getNumWorkersAlive() > cluster.getMinInstances()) {
					cluster.stopWorker();
					Thread.sleep(5000);
				}

				cluster.runCacheWarmer();
				queryNum = 1;
			}

			// Finally test the direct hop model, which doens't use same interface
			DirectHopElasticity directModel = new DirectHopElasticity(cluster);
			writer.println("\n-------------"+ directModel.getName() + "--------------");
			System.out.println("\n-------------" + directModel.getName() + "--------------");
			writer.println("expected,actual,score,workers");

			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int startingNumWorkers = cluster.getNumWorkersAlive();
				int workloadIndex = (cluster.getNumWorkersAlive() - 4) / 2;
				Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);
				Query originalQuery = originalWorkload.getQueries().get(queryIndex);

				double elapsedRuntime = cluster.executeQuery(q);

				q.setExpectedRuntime(originalQuery.getExpectedRuntime());
				q.setActualRuntime(elapsedRuntime);

				//directModel.addNewDataPoint(q, startingNumWorkers, queryIndex);
				int scaleTo = directModel.scaleTo();

				int x = startingNumWorkers;
				if (scaleTo > startingNumWorkers) {
					while (x < scaleTo) {
						cluster.startWorker();
						Thread.sleep(5000);
						x++;
					}

					cluster.runCacheWarmer();
				} else if (scaleTo < startingNumWorkers){
					while (x > scaleTo) {
						cluster.stopWorker();
						Thread.sleep(5000);
						x--;
					}

					cluster.runCacheWarmer();
				}

				writer.println(String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(scaleTo) + ", " + 
						String.valueOf(startingNumWorkers));

				queryNum++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			writer.close();
		}

		cluster.verbose = true;
	}

	@SuppressWarnings("unused")
	private static void timingRun() {
		cluster.verbose = false;

		// Read in workloads. We have separate workloads for each configuration to
		// utilize data on each machine.
		ArrayList<Workload> workloads = new ArrayList<Workload>();
		workloads.add(FileReaderUtils.readRandomQueries(4, "4 Node"));
		workloads.add(FileReaderUtils.readRandomQueries(5, "5 Node"));
		workloads.add(FileReaderUtils.readRandomQueries(6, "6 Node"));
		workloads.add(FileReaderUtils.readRandomQueries(7, "7 Node"));
		workloads.add(FileReaderUtils.readRandomQueries(8, "8 Node"));

		PrintWriter writer = null;
		try {
			cluster.scaleToMinSize();
			AWSClusterUtils.clearCache();
			Thread.sleep(10000);

			writer = new PrintWriter("/Users/jortiz16/Documents/myriascalabilityengine/runtimes/Type3b/runtimes.csv", "UTF-8");

			int counter = 0;
			for (Workload workload : workloads) {
				for (int i = 0; i < 10; i++) {
					Query q = workload.getQueries().get(i);
					double time1 = cluster.executeQuery(q);
					AWSClusterUtils.clearCache();
					double time2 = cluster.executeQuery(q);
					AWSClusterUtils.clearCache();
					double time3 = cluster.executeQuery(q);
					AWSClusterUtils.clearCache();

					double runtime = (time1+time2+time3) / 2.0;
					counter++;
					System.out.println(String.valueOf(counter) + "," + String.valueOf(runtime));
					writer.println(String.valueOf(counter) + "," + String.valueOf(runtime));
				}

				cluster.startWorker();
				Thread.sleep(10000);

				counter = 0;
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			writer.close();
		}

		cluster.verbose = true;		
	}

	private static void Usage() {
		System.out.println("\nMyria Elasticity Engine");
		System.out.println("\t1: Launch Cluster on Amazon");
		System.out.println("\t2: Upload data to cluster");
		System.out.println("\t3: Test query");
		System.out.println("\t4: Check workers alive");
		System.out.println("\t5: Start a worker");
		System.out.println("\t6: Stop a worker");
		System.out.println("\t7: Run experiment");	
		System.out.println("\t8: To quit\n");		
	}

	// Does a simulated experiment that helps debug models without
	// running the actual queries.  Uses pre-timed queries
	
	@SuppressWarnings("unused")
	private static void simulatedExperimentDirect() {
		cluster.verbose = false;
		int sampleSize = 10;
		int clusterSize = 4;
		double overestimate = 2.0; 
		double underestimate = 0.45; 

		// Read in workloads. We have separate workloads for each configuration to
		// utilize data on each machine.
		ArrayList<Workload> slowWorkloads = new ArrayList<Workload>();
		slowWorkloads.add(new Workload("4 Node Slow"));
		slowWorkloads.add(new Workload("6 Node Slow"));
		slowWorkloads.add(new Workload("8 Node Slow"));
		slowWorkloads.add(new Workload("10 Node Slow"));
		slowWorkloads.add(new Workload("12 Node Slow"));	

		for (Workload workload : slowWorkloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}

		ArrayList<Workload> fastWorkloads = new ArrayList<Workload>();
		fastWorkloads.add(new Workload("4 Node Fast"));
		fastWorkloads.add(new Workload("6 Node Fast"));
		fastWorkloads.add(new Workload("8 Node Fast"));
		fastWorkloads.add(new Workload("10 Node Fast"));
		fastWorkloads.add(new Workload("12 Node Fast"));	

		for (Workload workload : fastWorkloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}
		
		Map<Integer, List<Double>> timeMap = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/ActualRuntimes.csv");

		for (int i = 0; i < sampleSize; i++) {
			double underestimatedRuntime = timeMap.get(i).get(0) * underestimate;
			double overestimatedRuntime = timeMap.get(i).get(0) * overestimate;

			for (Workload slowWorkload : slowWorkloads) {
				slowWorkload.getQueries().get(i).setExpectedRuntime(underestimatedRuntime);
			}

			for (Workload fastWorkload : fastWorkloads) {
				fastWorkload.getQueries().get(i).setExpectedRuntime(overestimatedRuntime);
			}
		}

		int queryNum = 1;
		DirectHopElasticity directModel = new DirectHopElasticity(cluster);
		
		System.out.println("type,queryNum,expected,actual,workers");
		
		// Slow run
		for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
			int workloadIndex = (clusterSize - 4) / 2;
			Query q = slowWorkloads.get(workloadIndex).getQueries().get(queryIndex);

			double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
			q.setActualRuntime(elapsedRuntime);

			int startingNumWorkers = clusterSize;

			//directModel.addNewDataPoint(q, clusterSize, queryIndex);
			int scaleTo = directModel.scaleTo();
			clusterSize = scaleTo;

			System.out.println("direct," + String.valueOf(queryNum) + "," + 
					String.valueOf(q.getExpectedRuntime()) + "," +
					String.valueOf(elapsedRuntime) + "," +
					String.valueOf(startingNumWorkers));

			queryNum++;
		}

		// Fast run
		for (int k = 0; k < 4; k++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = fastWorkloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
				q.setActualRuntime(elapsedRuntime);

				int startingNumWorkers = clusterSize;

				//directModel.addNewDataPoint(q, clusterSize, queryIndex);
				int scaleTo = directModel.scaleTo();
				clusterSize = scaleTo;

				System.out.println("direct," + String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				queryNum++;
			}
		}
		// Second slow run
		for (int k = 0; k < 5; k++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = slowWorkloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
				q.setActualRuntime(elapsedRuntime);

				int startingNumWorkers = clusterSize;

				//directModel.addNewDataPoint(q, clusterSize, queryIndex);
				int scaleTo = directModel.scaleTo();
				clusterSize = scaleTo;

				System.out.println("direct," + String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				queryNum++;
			}
		}
	}

	
	@SuppressWarnings("unused")
	private static void simulatedExperiment() {
		cluster.verbose = false;
		int sampleSize = 10;
		int clusterSize = 4;
		double overestimate = 2.0; 
		double underestimate = 0.45; 

		// Read in workloads. We have separate workloads for each configuration to
		// utilize data on each machine.
		ArrayList<Workload> slowWorkloads = new ArrayList<Workload>();
		slowWorkloads.add(new Workload("4 Node Slow"));
		slowWorkloads.add(new Workload("6 Node Slow"));
		slowWorkloads.add(new Workload("8 Node Slow"));
		slowWorkloads.add(new Workload("10 Node Slow"));
		slowWorkloads.add(new Workload("12 Node Slow"));	

		for (Workload workload : slowWorkloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}

		ArrayList<Workload> fastWorkloads = new ArrayList<Workload>();
		fastWorkloads.add(new Workload("4 Node Fast"));
		fastWorkloads.add(new Workload("6 Node Fast"));
		fastWorkloads.add(new Workload("8 Node Fast"));
		fastWorkloads.add(new Workload("10 Node Fast"));
		fastWorkloads.add(new Workload("12 Node Fast"));	

		for (Workload workload : fastWorkloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}
		
		Map<Integer, List<Double>> timeMap = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/ActualRuntimes.csv");
		
		for (int i = 0; i < sampleSize; i++) {
			double underestimatedRuntime = timeMap.get(i).get(0) * underestimate;
			double overestimatedRuntime = timeMap.get(i).get(0) * overestimate;

			for (Workload slowWorkload : slowWorkloads) {
				slowWorkload.getQueries().get(i).setExpectedRuntime(underestimatedRuntime);
			}

			for (Workload fastWorkload : fastWorkloads) {
				fastWorkload.getQueries().get(i).setExpectedRuntime(overestimatedRuntime);
			}
		}

		int queryNum = 1;
		ElasticityModel model = new EWMAElasticity();
		String name = "ewma";
		
		System.out.println("type,queryNum,expected,actual,workers");
		
		// Slow run
		for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
			int workloadIndex = (clusterSize - 4) / 2;
			Query q = slowWorkloads.get(workloadIndex).getQueries().get(queryIndex);

			double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
			q.setActualRuntime(elapsedRuntime);

			int startingNumWorkers = clusterSize;

			model.addNewDataPoint(q);
			if (model.shouldScaleCluster() > 0) {
				if (clusterSize < cluster.getMaxInstances()) {
					clusterSize += cluster.getSkipFactor();
				}
			} else if (model.shouldScaleCluster() < 0) {
				if (clusterSize > cluster.getMinInstances()) {
					clusterSize -= cluster.getSkipFactor();
				}
			}

			System.out.println(name + "," + String.valueOf(queryNum) + "," + 
					String.valueOf(q.getExpectedRuntime()) + "," +
					String.valueOf(elapsedRuntime) + "," +
					String.valueOf(startingNumWorkers));

			queryNum++;
		}

		// Fast run
		for (int k = 0; k < 4; k++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = fastWorkloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
				q.setActualRuntime(elapsedRuntime);

				int startingNumWorkers = clusterSize;

				model.addNewDataPoint(q);
				if (model.shouldScaleCluster() > 0) {
					if (clusterSize < cluster.getMaxInstances()) {
						clusterSize += cluster.getSkipFactor();
					}
				} else if (model.shouldScaleCluster() < 0) {
					if (clusterSize > cluster.getMinInstances()) {
						clusterSize -= cluster.getSkipFactor();
					}
				}

				System.out.println(name + "," + String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				queryNum++;
			}
		}
		// Second slow run
		for (int k = 0; k < 5; k++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = slowWorkloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
				q.setActualRuntime(elapsedRuntime);

				int startingNumWorkers = clusterSize;

				model.addNewDataPoint(q);
				if (model.shouldScaleCluster() > 0) {
					if (clusterSize < cluster.getMaxInstances()) {
						clusterSize += cluster.getSkipFactor();
					}
				} else if (model.shouldScaleCluster() < 0) {
					if (clusterSize > cluster.getMinInstances()) {
						clusterSize -= cluster.getSkipFactor();
					}
				}

				System.out.println(name + "," + String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				queryNum++;
			}
		}
	}
	
	private static void printPerfect() {
		for (int i = 1; i <= 10; i++) {
			System.out.println("ideal," + String.valueOf(i) + ",0.0,0.0,12");
		}
		for (int i = 11; i <= 50; i++) {
			System.out.println("ideal," + String.valueOf(i) + ",0.0,0.0,4");
		}
		for (int i = 51; i <= 100; i++) {
			System.out.println("ideal," + String.valueOf(i) + ",0.0,0.0,12");
		}
	}
	
	@SuppressWarnings("unused")
	private static void convergenceTimingTest() {
		boolean singleQuery = true;
		boolean scaleDown = true;

		int sampleSize = 10;
		int clusterSize = 4;
		double overestimate = 1.2; 
		double underestimate = 0.8;
		int repeatCount = 25;
		double runningTime = 0.0;

		if (singleQuery) {
			sampleSize = 1;
		}

		if (scaleDown) {
			clusterSize = 12;
		}

		ArrayList<Workload> workloads = new ArrayList<Workload>();

		workloads.add(new Workload("4 Node"));
		workloads.add(new Workload("6 Node"));
		workloads.add(new Workload("8 Node"));
		workloads.add(new Workload("10 Node"));
		workloads.add(new Workload("12 Node"));	

		for (Workload workload : workloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}

		Map<Integer, List<Double>> timeMap = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/JoinQueries.csv");

		for (int i = 0; i < sampleSize; i++) {
			double runtime = timeMap.get(i).get(0) * underestimate;

			// The first query in the workload is too small for a single
			// query experiment so we just grab the second one
			if (singleQuery) {
				runtime = timeMap.get(1).get(0) * underestimate; 
			}

			if (scaleDown) {
				runtime = timeMap.get(i).get(0) * overestimate; 
			}
			
			if (scaleDown && singleQuery) {
				runtime = timeMap.get(1).get(0) * overestimate; 
			}

			for (Workload workload : workloads) {
				workload.getQueries().get(i).setExpectedRuntime(runtime);
			}
		}

		int queryNum = 1;
		ElasticityModel model = new EWMAElasticity();
		
		System.out.println("queryNum,expected,actual,workers");

		for (int i = 0; i < repeatCount; i++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);

				// The first query in the workload is too small for a single
				// query experiment so we just grab the second one
				if (singleQuery) {
					elapsedRuntime = timeMap.get(1).get(workloadIndex); 
				}

				q.setActualRuntime(elapsedRuntime);
				runningTime += elapsedRuntime;

				int startingNumWorkers = clusterSize;
				model.addNewDataPoint(q);
				if (model.shouldScaleCluster() > 0) {
					if (clusterSize < cluster.getMaxInstances()) {
						clusterSize += cluster.getSkipFactor();
					}
				} else if (model.shouldScaleCluster() < 0) {
					if (clusterSize > cluster.getMinInstances()) {
						clusterSize -= cluster.getSkipFactor();
					}
				}

				System.out.println(String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				System.out.print("Running Time: ");
				System.out.println(runningTime);

				queryNum++;
			}
		}

		cluster.verbose = true;
	}
	
	//added to run batch of over or underpredicted queries
	private static void convergenceTimingBATCH() {
		int batchNumber = 5;
		boolean scaleDown = true;
		int clusterSize = 4;
		
		double runningTime = 0.0;

		//if we're scaling down, start the cluster at 12 (the max)
		if (scaleDown) {
			clusterSize = 12;
		}

		ArrayList<Workload> workloads = new ArrayList<Workload>();

		workloads.add(new Workload("4 Node"));
		workloads.add(new Workload("6 Node"));
		workloads.add(new Workload("8 Node"));
		workloads.add(new Workload("10 Node"));
		workloads.add(new Workload("12 Node"));	
		
		
		Map<Integer, List<Double>> timeMap_expected = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/RandomOrder/queries_overestimated_estimated_rand1.csv");
		Map<Integer, List<Double>> timeMap_actual = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/RandomOrder/queries_overestimated_actual_rand1.csv");
		
		int qlistSize = timeMap_actual.size();

		//for each workload add a query
		for(int j = 0; j < batchNumber; j++){
		for (int i = 0; i < qlistSize; i++) {
		for (Workload workload : workloads) {
				workload.addQuery(new Query(null));
		}
		}
		}

		//for each query found in the map, set the expected time for the smallest config
		for(int j = 0; j < batchNumber; j++){
		for (int i = 0; i < qlistSize; i++) {
			for (Workload workload : workloads) {
				//int index = (!scaleDown) ? 0 : 4;
				workload.getQueries().get(i).setExpectedRuntime(timeMap_expected.get(i).get(0) + timeMap_expected.get(i).get(0)*.1);
			}
		}
		}

		int queryNum = 1;
		ElasticityModel model = new WindowedElasticity();
		
		System.out.println("queryNum,expected,actual,workers");
		
		//start the model
		for(int j = 0; j < 5; j++){
		for (int queryIndex = 0; queryIndex < qlistSize; queryIndex++) {
			int workloadIndex = (clusterSize - 4) / 2;
			Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);

			double elapsedRuntime = timeMap_actual.get(queryIndex).get(workloadIndex);

			q.setActualRuntime(elapsedRuntime);
			runningTime += elapsedRuntime;

			int startingNumWorkers = clusterSize;
			model.addNewDataPoint(q);
			if (model.shouldScaleCluster() > 0) {
				if (clusterSize < cluster.getMaxInstances()) {
					clusterSize += cluster.getSkipFactor();
				}
			} else if (model.shouldScaleCluster() < 0) {
				if (clusterSize > cluster.getMinInstances()) {
					clusterSize -= cluster.getSkipFactor();
				}
			}
//			
//			if(q.getExpectedRuntime() - q.getActualRuntime() < 0)
//			{
//				System.out.print("SLOWER,");
//			}

			System.out.println(String.valueOf(queryNum) + "," + 
					String.valueOf(q.getExpectedRuntime()) + "," +
					String.valueOf(elapsedRuntime) + "," +
					String.valueOf(startingNumWorkers));
		
			//System.out.print("Running Time: ");
			//System.out.println(runningTime);

			queryNum++;
		}
		}
		

		cluster.verbose = true;
	}

	
	private static void convergenceTimingDirectBATCH() throws IOException, InterruptedException{
		//algorithm parameters
		int batchNumber = 5;
		boolean scaleDown = false;
		int clusterSize = 4;
		boolean usePredictions = true;
		
		//file parameters
		String masterPath = "/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/";
		String keyFile = "under";
		
		//double currentRunningTime = 0.0;
		
		//if we're scaling down, start the cluster at 12 (the max)
		if (scaleDown) {
			clusterSize = 12;
		}

		ArrayList<Workload> workloads = new ArrayList<Workload>();

		workloads.add(new Workload("4 Node"));
		workloads.add(new Workload("6 Node"));
		workloads.add(new Workload("8 Node"));
		workloads.add(new Workload("10 Node"));
		workloads.add(new Workload("12 Node"));	
		
		
		Map<Integer, List<Double>> timeMap_expected = FileReaderUtils.readTimeMap(masterPath + "queries_" + keyFile + "estimated_estimated.csv");
		Map<Integer, List<Double>> timeMap_actual = FileReaderUtils.readTimeMap(masterPath + "queries_" + keyFile + "estimated_actual.csv");
		
		int qlistSize = timeMap_actual.size();
		
		//populate query IDs from file (for testing queries)
		FileReader testing_queriesIDsFile = new FileReader( new File(masterPath + "predictions/testing_ids.txt"));
		BufferedReader testing_queriesIDsBuffer = new BufferedReader(testing_queriesIDsFile);
		String currentReadID = "";
		ArrayList<Integer> queryIDs = new ArrayList<Integer>();
		while((currentReadID=testing_queriesIDsBuffer.readLine())!=null) {
			queryIDs.add(Integer.valueOf(currentReadID));
		}
	
		//for each workload add a query and set the sla time for 4 workers
		for(int j = 0; j < batchNumber; j++){
			for (int i = 0; i < qlistSize; i++) {
				for (Workload workload : workloads) {
				workload.addQuery(new Query(null));
				workload.getQueries().get(i).setExpectedRuntime(timeMap_expected.get(i).get(0));
				}
			}
		}

		DirectHopElasticity directModel = new DirectHopElasticity(cluster);
		System.out.println("queryNum,expected,actual,workers");
		
		int queryNum = 1;
		int totalQueriesMissed = 0;
		int totalMissed = 0;
		
		//only for predictions if used
		Map<String, Integer> actualCardinalities  = new HashMap<String, Integer>();
		
		//start the model over the batch of queries
		for (int queryIndex = 0; queryIndex < qlistSize; queryIndex++) {
			int workloadIndex = (clusterSize - 4) / 2;
			int startingNumWorkers = clusterSize;
			
			//get the first query and "run" it
			Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);
			q.setQueryID(queryIDs.get(queryIndex));
			
			double elapsedRuntime = timeMap_actual.get(queryIndex).get(workloadIndex);
			q.setActualRuntime(elapsedRuntime);

			
			System.out.printf("%-1d, %2f, %2f, %d \t", queryNum,  
					q.getExpectedRuntime(),
					elapsedRuntime,
					startingNumWorkers);
			
			
			//*****modify predictions
			if(usePredictions){	
				
			//STEP 1: modify test file
			
			if (queryNum == 1) { //only do once
				//make a copy
				Process makeArffCopy = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "cd " + masterPath + "predictions/; cp testing-small.arff testing-small-cp.arff;"});
				makeArffCopy.waitFor();
				
				//read actual cardinalities
				FileReader actual_cardinalitiesFile = new FileReader( new File(masterPath + "predictions/testing_actual_cardinalities.txt"));
				BufferedReader actual_cardinalitiesBuffer = new BufferedReader(actual_cardinalitiesFile);
				String currentLine = "";
				while((currentLine=actual_cardinalitiesBuffer.readLine())!=null) {
					String[] currentListSplit = currentLine.split(",");
					String generatedKey = currentListSplit[0] + "-" + currentListSplit[1];
					actualCardinalities.put(generatedKey, Integer.valueOf(currentListSplit[2]));
				}
				
			}
			PrintWriter newTestArffFile = new PrintWriter(masterPath + "predictions/testing-small-mod.arff", "UTF-8");
			
			FileReader currentPredictionsFile = new FileReader( new File(masterPath + "predictions/testing-small-cp.arff"));
			BufferedReader currentPredictionsBuffer = new BufferedReader(currentPredictionsFile);
			String currentLine = null;
			
			//read/write the header
			for(int l = 0 ; l < 13; l++){
				String beginningLine = currentPredictionsBuffer.readLine();
				newTestArffFile.write(beginningLine + "\n");
			}
			//read/write the rest of the file
			while((currentLine=currentPredictionsBuffer.readLine())!=null)
			{
				String[] currentListSplit = currentLine.split(",");
				Integer currentQueryID = Integer.valueOf(currentListSplit[0]);
				Integer currentConfig = Integer.valueOf(currentListSplit[7]);
				if(currentQueryID == q.getQueryID() && currentConfig == clusterSize){
					for(int i = 0; i< 9; i++){
						switch(i) {
							case 4: //actual time
								newTestArffFile.write(q.getActualRuntime() + ",");
								break;
							case 5: //actual rows -- find it in the populated list
								for(Map.Entry<String, Integer>  entry: actualCardinalities.entrySet()){
									String[] parseKey = entry.getKey().split("-");

									if(Integer.valueOf(parseKey[0]).equals(currentQueryID) && Integer.valueOf(parseKey[1]).equals(currentConfig)){
										newTestArffFile.write(entry.getValue() + ",");
									}
								}
								break;
							default: 
								newTestArffFile.write(currentListSplit[i] + ",");
								break;
						}
					}
					newTestArffFile.write("\n");
				}
				else
				{
					newTestArffFile.write(currentLine + "\n");
				}
			}
			newTestArffFile.close();
			
			//STEP 2: re-run predictions
			//clear results
			Process clearResults = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "> " + masterPath + "predictions/prediction_results.txt"});
		    clearResults.waitFor();
		    
		    //remove the ID attribute for training and testing
		    String removeIDCommand_training = String.format("java weka.filters.unsupervised.attribute.Remove -R %s -i %s -o %s",
					"1",
					masterPath + "predictions/training-small.arff",
					masterPath + "predictions/training-small-noID.arff");
		    Process runRemove_training = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "export CLASSPATH=$CLASSPATH:/Users/jortiz16/Desktop/weka-3-6-12/weka.jar;" + removeIDCommand_training});
		    runRemove_training.waitFor();
		    
		    String removeIDCommand_testing = String.format("java weka.filters.unsupervised.attribute.Remove -R %s -i %s -o %s",
					"1",
					masterPath + "predictions/testing-small-mod.arff",
					masterPath + "predictions/testing-small-noID.arff");
		    Process runRemove_testing = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "export CLASSPATH=$CLASSPATH:/Users/jortiz16/Desktop/weka-3-6-12/weka.jar;" + removeIDCommand_testing});
		    runRemove_testing.waitFor();
		 
			String command = String.format("java weka.classifiers.functions.GaussianProcesses -L 1.0 -N 0 -K \"weka.classifiers.functions.supportVector.RBFKernel -C 250007 -G 1.0\" -t %s  -T %s -p 0 > %s",
													masterPath + "predictions/training-small-noID.arff",
													masterPath + "predictions/testing-small-noID.arff",
													masterPath + "predictions/prediction_results.txt");
			
			Process getResults = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", "export CLASSPATH=$CLASSPATH:/Users/jortiz16/Desktop/weka-3-6-12/weka.jar;" + command});
			getResults.waitFor();
			
			//STEP 3: Parse the prediction results
			ArrayList<Double> workersTimes4 = new ArrayList<Double>();
			ArrayList<Double> workersTimes6 = new ArrayList<Double>();
			ArrayList<Double> workersTimes8 = new ArrayList<Double>();
			ArrayList<Double> workersTimes10 = new ArrayList<Double>();
			ArrayList<Double> workersTimes12 = new ArrayList<Double>();
			
			FileReader predictionFile = new FileReader( new File(masterPath + "predictions/prediction_results.txt"));
			BufferedReader predictionsBuffer = new BufferedReader(predictionFile);
			
			//skip header lines 
			for(int headerLine = 0 ; headerLine < 5; headerLine++){
				predictionsBuffer.readLine();
			}
			//read the rest of the file
			for(int currentReadConfig = 0; currentReadConfig < 5; currentReadConfig++){
				for(int j = 0; j < 100; j++){ //100 lines for each config

					//there should be a way to make the output nicer
					String[] pieces = predictionsBuffer.readLine().split("    ");
					double predictionReadFromFile;
					try{
						predictionReadFromFile =  Double.valueOf(pieces[pieces.length-2].trim());
					}
					catch(Exception e){
						predictionReadFromFile =  Double.valueOf(pieces[pieces.length-3].trim());
					}
					
					switch(currentReadConfig){
						case 0: 
							workersTimes4.add(predictionReadFromFile);
							break;
						case 1: 
							workersTimes6.add(predictionReadFromFile);
							break;
						case 2: 
							workersTimes8.add(predictionReadFromFile);
							break;
						case 3: 
							workersTimes10.add(predictionReadFromFile);
							break;
						case 4: 
							workersTimes12.add(predictionReadFromFile);
							break;
						
					}
					
				}
			}
			predictionsBuffer.close();
			
			//this last part outputs only the queries I'm working with at the moment
			ArrayList<Integer> idsNeeded = new ArrayList<Integer>();
			FileReader idFile = new FileReader( new File(masterPath + "queries_" + keyFile + "estimated_estimated.csv"));
			BufferedReader idFileBuffer = new BufferedReader(idFile);
			String currentID = null;
			while((currentLine=idFileBuffer.readLine())!=null)
			{
				idsNeeded.add(Integer.valueOf(currentLine.split(",")[0]));
			}
			idFileBuffer.close();
			
			//arrange the runtimes into a file
			PrintWriter writePredictionsForModel = new PrintWriter(masterPath + "predictions/prediction_results-selected.csv", "UTF-8");
			for(int i = 0; i < 100; i++){
				if(idsNeeded.contains(queryIDs.get(i))){
					writePredictionsForModel.println(queryIDs.get(i) + "," 
													+ workersTimes4.get(i) + "," 
													+  workersTimes6.get(i) + ","  
													+ workersTimes8.get(i) + ","   
													+ workersTimes10.get(i) + ","  
													+ workersTimes12.get(i));
				}
			}
			writePredictionsForModel.close();
			
			//back to the model
			directModel.addNewDataPoint(q, clusterSize, queryNum-1);
			int scaleTo = directModel.scaleTo();
			clusterSize = scaleTo;
			
			if(q.getExpectedRuntime() - q.getActualRuntime() < 0)
			{
				totalMissed++;
			}

			queryNum++;
		}
		}
	
		System.out.println("Total Missed " + totalMissed);
		
		cluster.verbose = true;
	}

	
	
	// Does a simulated experiment that helps debug models without
	// running the actual queries.  Uses pre-timed queries
	@SuppressWarnings("unused")
	private static void convergenceTimingTestDirect() {
		boolean singleQuery = true;
		boolean scaleDown = true;

		cluster.verbose = false;
		int sampleSize = 10;
		int clusterSize = 4;
		double underestimate = 0.7; 
		double overestimate = 1.4;
		int repeatCount = 10;
		double runningTime = 0.0;

		if (singleQuery) {
			sampleSize = 1;
		}

		if (scaleDown) {
			clusterSize = 12;
		}

		ArrayList<Workload> workloads = new ArrayList<Workload>();

		workloads.add(new Workload("4 Node"));
		workloads.add(new Workload("6 Node"));
		workloads.add(new Workload("8 Node"));
		workloads.add(new Workload("10 Node"));
		workloads.add(new Workload("12 Node"));	

		for (Workload workload : workloads) {
			for (int i = 0; i < sampleSize; i++) {
				workload.addQuery(new Query(null));
			}
		}

		Map<Integer, List<Double>> timeMap = FileReaderUtils.readTimeMap("/Users/jortiz16/Documents/myriascalabilityengine/timing/LargeToSmall/JoinQueries.csv");

		for (int i = 0; i < sampleSize; i++) {
			double runtime = timeMap.get(i).get(0) * underestimate;

			// The first query in the workload is too small for a single
			// query experiment so we just grab the second one
			if (singleQuery) {
				runtime = timeMap.get(1).get(0) * underestimate; 
			}

			if (scaleDown) {
				runtime = timeMap.get(i).get(0) * overestimate; 
			}
			
			if (scaleDown && singleQuery) {
				runtime = timeMap.get(1).get(0) * overestimate; 
			}

			for (Workload workload : workloads) {
				workload.getQueries().get(i).setExpectedRuntime(runtime);
			}
		}

		int queryNum = 1;
		DirectHopElasticity directModel = new DirectHopElasticity(cluster);
		System.out.println("queryNum,expected,actual,workers");

		for (int i = 0; i < repeatCount; i++) {
			for (int queryIndex = 0; queryIndex < sampleSize; queryIndex++) {
				int workloadIndex = (clusterSize - 4) / 2;
				Query q = workloads.get(workloadIndex).getQueries().get(queryIndex);

				double elapsedRuntime = timeMap.get(queryIndex).get(workloadIndex);
				// The first query in the workload is too small for a single
				// query experiment so we just grab the second one
				if (singleQuery) {
					elapsedRuntime = timeMap.get(1).get(workloadIndex); 
				}

				q.setActualRuntime(elapsedRuntime);
				runningTime += elapsedRuntime;

				int startingNumWorkers = clusterSize;
				//directModel.addNewDataPoint(q, clusterSize, queryIndex);
				int scaleTo = directModel.scaleTo();
				clusterSize = scaleTo;

				System.out.println(String.valueOf(queryNum) + "," + 
						String.valueOf(q.getExpectedRuntime()) + "," +
						String.valueOf(elapsedRuntime) + "," +
						String.valueOf(startingNumWorkers));
				System.out.print("Running Time: ");
				System.out.println(runningTime);

				queryNum++;
			}
		}

		cluster.verbose = true;		
	}
}
