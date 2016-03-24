package neu.mr6240.utils;

/**
 * This class gets the parses the parameters from the command line arguments
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class CommandLineParser {
	
	public static final int INPUT_DIR_INDEX = 1;
	public static final int OUTPUT_DIR_INDEX = 2;
	public static final int SCHEDULED_FLIGHT_TIME_INDEX = 3;
	
	public static final String CMD_SEPERATOR = "=";

	private String[] args;
	
	private String inputDir;
	private String outputDir;
	private String time;

	public CommandLineParser(String[] args) {
		this.args = args;
		setInputDir(args[INPUT_DIR_INDEX]);
		setOutputDir(args[OUTPUT_DIR_INDEX]);
		setTime(args[SCHEDULED_FLIGHT_TIME_INDEX]);
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public String getInputDir() {
		return inputDir;
	}

	/**
	 * Set the argument input parameter
	 * @param inputDir
	 */
	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}

	public String getOutputDir() {
		return outputDir;
	}

	/**
	 * Set the argument output parameter
	 * @param outputDir
	 */
	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getTime() {
		return time;
	}

	/**
	 * Set the argument for parameter "time="
	 * @param time
	 */
	public void setTime(String time) {		
		this.time = time.substring(time.indexOf(CMD_SEPERATOR) + 1);
	}

}
