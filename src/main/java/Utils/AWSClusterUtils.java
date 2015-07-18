package Utils;

import java.io.*;

public class AWSClusterUtils {
	private static boolean debug = true;
	
	public static String launchCluster(int numInstances) {
		String python = "python";
		String program = "./starclusterfiles/launcher.py";
		
		runScript(python, program);
		
		String masterDNS = "";
		return  masterDNS;
	}

	public static void uploadAndIngestData() {
		String python = "python";
		
		String uploadProgram = "./starclusterfiles/dataIngest.py";
		runScript(python, uploadProgram);		

		String ingestProgram = "./starclusterfiles/dataIngest.py";
		runScript(python, ingestProgram);		
		
		String replicateProgram = "./starclusterfiles/dataReplicate.py";
		runScript(python, replicateProgram);		
	}
	
	public static void clearCache() {
		String python = "python";
		String program = "./starclusterfiles/clearCache.py";
		
		runScript(python, program);
	}
	
	public static void terminateCluster() {
		String python = "python";
		String program = "./starclusterfiles/terminator.py";
		
		runScript(python, program);
	}
	
	private static void runScript(String python, String program) {
		String[] cmd = new String[2];
		cmd[0] = python;
		cmd[1] = program;
		
		Runtime rt = Runtime.getRuntime();
		
		try {
			Process p = rt.exec(cmd);
			BufferedReader bfr = new BufferedReader(new InputStreamReader(p.getInputStream()));
			
			String output = "";
			while ((output = bfr.readLine()) != null) {
				System.out.println(output);
			}
			
			// For debugging
			if (debug) {
				BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				String error = "";
				while ((error = errorStream.readLine()) != null) {
					System.err.println(error);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
