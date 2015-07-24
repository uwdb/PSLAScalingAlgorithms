package Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import DataModel.Query;
import DataModel.Workload;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FileReaderUtils {

	public static Workload readfullWorkload(int workerCount, String name) {
		// TODO: Read actual data from output of PSLA engine
		Workload W = new Workload(name);

		JsonObject workloadJSON = readJSON("/Users/jortiz16/Documents/myriascalabilityengine/pslaData/FinalQueries.json");
		String workerCountAsString = String.valueOf(workerCount);

		try {
			JsonArray configs = workloadJSON.getAsJsonArray("QueryInformation");
			for (int i = 0; i < configs.size(); i++) {
				JsonObject config = configs.get(i).getAsJsonObject();
				String configWorkerCount = config.get("NumWorkers").getAsString();
				if (configWorkerCount.equals(workerCountAsString)) {

					JsonArray queries = config.getAsJsonArray("Queries");
					for (int j = 0; j < queries.size(); j++) {
						JsonObject query = queries.get(j).getAsJsonObject();

						// Now grab each query json
						String queryJSONPath = "/Users/jortiz16/Documents/myriascalabilityengine/queries/" 
								+ workerCountAsString
								+ "/json"
								+ String.valueOf(j)
								+ ".json";
						Query q = new Query(readJSON(queryJSONPath));
						double time = query.get("QueryPredictedRuntime").getAsDouble();
						q.setExpectedRuntime(Math.round(time * 1000.0)); // To milliseconds
						q.setQueryName("json" + String.valueOf(j) + ".json");

						W.addQuery(q);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return W;
	}

	public static Workload readRandomQueries(int workerCount, String name) {
		// TODO: Read actual data from output of PSLA engine
		Workload W = new Workload(name);
		int maxQueries = 10;

		try {
			for (int j = 6; j < maxQueries + 6; j++) {
				String workerCountAsString = String.valueOf(workerCount);
				String queryNumberAsString = String.valueOf(j);

				String queryJSONPath = "/Users/jortiz16/Documents/myriascalabilityengine/sampleQueries/Type2/" 
						+ workerCountAsString
						+ "/"
						+ queryNumberAsString
						+ ".json";

				Query q = new Query(readJSON(queryJSONPath));
				q.setQueryName("json" + String.valueOf(j) + ".json");
				W.addQuery(q);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return W;
	}

	public static JsonObject readJSON(String path) {
		JsonParser parser = new JsonParser();
		JsonObject jsonObject = null;
		try {
			jsonObject = (JsonObject)parser.parse(new FileReader(path));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jsonObject;
	}

	public static void updateStarClusterFiles(int maxSize) {
		String outputFile = "/Users/brendan/.starcluster/myriacluster.config";
		String inputFile = "/Users/jortiz16/Documents/myriascalabilityengine/starclusterfiles/myriacluster.config";

		Path inputPath = Paths.get(inputFile);
		Path outputPath = Paths.get(outputFile);
		Charset charset = StandardCharsets.UTF_8;

		String content;
		try {
			content = new String(Files.readAllBytes(inputPath), charset);
			content = content.replaceAll("\\{SIZE\\}", String.valueOf(maxSize + 1));
			Files.write(outputPath, content.getBytes(charset));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Map<Integer, List<Double>> readTimeMap(String inputFile) {
		Map<Integer, List<Double>> timeMap = new HashMap<Integer, List<Double>>();
		try {
			FileInputStream fstream = new FileInputStream(inputFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;
			Integer queryIndex = 0;
			while ((strLine = br.readLine()) != null)   {
				String[] tokens = strLine.split(",");
				List<Double> timeList = new ArrayList<Double>();
				for (int i = 1; i < tokens.length; i++) {
					timeList.add(Double.valueOf(tokens[i]));
				}
				
				timeMap.put(queryIndex, timeList);
				queryIndex += 1;
			}

			br.close();
		} catch(Exception e) {
			e.printStackTrace();
		}

		return timeMap;
	}
}