package DataModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import Utils.FileReaderUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Cluster {
	private static Cluster sharedInstance = null;
	private static HttpClient client;

	private String masterDNS = "";
	private String masterPort = ":8753";
	private String protocol = "http://";

	private int numTotalInstances;
	private int numInstancesAlive;
	private int numMinNeededInstances;
	
	// If 4 to 16 node, skipFactor is 2 (4, 6 ... 16)
	private int skipFactor = 2;

	public Boolean verbose = true;
	
	protected Cluster() {
		// To enforce singleton
	}

	public static Cluster getInstance() {
		if (sharedInstance == null) {
			sharedInstance = new Cluster();

			client = HttpClientBuilder.create().build();
		}

		return sharedInstance;
	}

	public void setInstanceBounds(int minSize, int maxSize) {
		this.numTotalInstances = maxSize;
		this.numInstancesAlive = minSize;
		this.numMinNeededInstances = minSize;
		if (maxSize == 16) {
			skipFactor = 2;
		}
	}

	public void setMasterDNS(String masterDNS) {
		this.masterDNS = masterDNS;
	}

	public String getMasterDNS() {
		return this.masterDNS;
	}

	public int getMinInstances() {
		return this.numMinNeededInstances;
	}
	
	public int getMaxInstances() {
		return numTotalInstances;
	}

	public boolean canStartWorker() {
		return (this.numInstancesAlive < this.numTotalInstances);
	}

	public void startWorker() {
		if (canStartWorker()) {
			System.out.println("Starting worker");

			Integer workerToStart = this.numInstancesAlive + 1;
			sendPost("/workers/start/worker-" + workerToStart.toString(), null);
			this.numInstancesAlive++;
		} else {
			System.out.println("System cannot scale up any further");

		}
	}

	public boolean canStopWorker() {
		return (this.numInstancesAlive > numMinNeededInstances);
	}

	public void stopWorker() {
		if (canStopWorker()) {
			System.out.println("Stopping worker");


			Integer workerToStop = this.numInstancesAlive;
			sendPost("/workers/stop/worker-" + workerToStop.toString(), null);
			this.numInstancesAlive--;
		} else {
			System.out.println("System cannot scale down any further");

		}
	}

	public void checkNumWorkersAlive() {
		sendGet("/workers/alive");
	}
	
	public int getNumWorkersAlive() {
		return numInstancesAlive;
	}


	public void sendTestQuery(Query q) {
		executeQuery(q);
	}

	public long executeQuery(Query q) {
		String responseString = sendPost("/query",q.getJson());		
		JsonObject responseJson = parseResponse(responseString);
		Long qId = Long.valueOf(responseJson.get("queryId").getAsInt());

		boolean queryComplete = false;
		while (!queryComplete) {
			responseString = sendGet("/query/query-" + qId.toString());
			responseJson = parseResponse(responseString);
			if (responseJson.get("status").getAsString().equals("SUCCESS")) {
				queryComplete = true;
			}
		}

		long elapsedTime = responseJson.get("elapsedNanos").getAsLong();
		return elapsedTime/ 1000000; // To milliseconds
	}

	private JsonObject parseResponse(String responseString) {
		JsonParser parser = new JsonParser();
		JsonObject jsonObject = null;
		try {
			Object obj = parser.parse(responseString);
			jsonObject = (JsonObject) obj; 
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jsonObject;
	}

	// HTTP GET request
	private String sendGet(String path) {
		HttpEntity responseEntity = null;
		try {
			String url = this.protocol + this.masterDNS + this.masterPort + path;

			HttpGet request = new HttpGet(url);	 
			HttpResponse response = client.execute(request);

			if (verbose) {
				System.out.println("\nSending 'GET' request to URL : " + url);
				System.out.println("Response Code : " + 
					response.getStatusLine().getStatusCode());
			}

			responseEntity = response.getEntity();
			BufferedReader rd = new BufferedReader(
					new InputStreamReader(responseEntity.getContent()));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}

			if (verbose) {
				System.out.println(result.toString());
			}

			return result.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				EntityUtils.consume(responseEntity);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}

	// HTTP POST request
	private String sendPost(String path, JsonObject json) {
		HttpEntity responseEntity = null;
		try {
			String url = this.protocol + this.masterDNS + this.masterPort + path;
			HttpPost post = new HttpPost(url);

			if (json != null) {
				StringEntity params = new StringEntity(json.toString());
				post.addHeader("content-type", "application/json");
				post.setEntity(params);
			}

			HttpResponse response = client.execute(post);

			if (verbose) {
				System.out.println("\nSending 'POST' request to URL : " + url);
				System.out.println("Post parameters : " + post.getEntity());
				System.out.println("Response Code : " + 
					response.getStatusLine().getStatusCode());
			}

			responseEntity = response.getEntity();
			BufferedReader rd = new BufferedReader(
					new InputStreamReader(responseEntity.getContent()));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			if (verbose) {
				System.out.println(result.toString());
			}
			
			return result.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				EntityUtils.consume(responseEntity);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void scaleToMinSize() {
		for (int i = 5; i <= 16; i++) {
			sendPost("/workers/stop/worker-" + String.valueOf(i), null);
		}
	}

	public void runCacheWarmer() {
		String queryJSONPath = "./queries/"
				+ numInstancesAlive
				+ "/cacheWarmer.json";
		
		Query q = new Query(FileReaderUtils.readJSON(queryJSONPath));
		this.executeQuery(q);
	}

	public int getSkipFactor() {
		return skipFactor;
	}
}