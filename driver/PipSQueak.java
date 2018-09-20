import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/* Not implemented: Meant to allow special command lines to be executed in event of exception
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
 */
import org.apache.drill.jdbc.DrillResultSet;

import oadd.org.apache.commons.lang.time.StopWatch;

/**
 * @author kkhatua
 *
 */
public class PipSQueak {
	private static Logger log;
	private static JDBCDriver jdbcDriver;
	private static boolean isDrillDriver;
	private String driverClass;
	private LinkedList<String> alterations;
	private StopWatch stopWatch;
	private Long[] timings;
	/** ----- **/
	private static final String EOL = "\n";
	private static final String DELIMITER = "|";
	public static final String PRE_CANCEL = "preCancel";

	private String query;
	private String connURL;
	private String userName;
	private String userPassword;
	private Connection connection;
	private Statement statement;
	private ResultSet resultSet;
	private long rowsRead;
	private File queryFile;
	private int timeout;
	private boolean skipRowFetchDueToFailure;
	private String explainPlan;
	private Boolean showResults = false;
	private String explainPlanPrefix = "";
	private QueryTimer queryTimer;
	private boolean isCancelled = false;

	private String outputFileName;
	private PrintStream outputStream;
	private String explainVerbosePlanPrefix;
	private boolean mustPrepareStatement;
	private PreparedStatement prepStatement;

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args)  {
		//Speed up on SysOut
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));

		try {
			PipSQueak pipSQueak = new PipSQueak(args[0]);
			pipSQueak.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	/**
	 * Constructor for Test Driver
	 * @throws FileNotFoundException
	 */
	public PipSQueak(String fileName) throws FileNotFoundException {
		//Init Logger
		initLogger();
		
		//Timings
		this.stopWatch = new StopWatch();
		this.timings = new Long[Timing.values().length];
		//Query
		this.queryFile = new File(fileName); 
		System.out.println(this.queryFile.getAbsolutePath());
		if (!queryFile.canRead()) 
			throw new FileNotFoundException(fileName);
		//Database
		initJdbcParams();
		checkForExplanation();
		initSessionParams();

		//Output
		initOutputFormat();
	}

	@SuppressWarnings("unused")
	private void initLogger() {
		 log = Logger.getLogger(PipSQueak.class.getCanonicalName());
		 log.setLevel(Level.WARNING);
		 /*
		 ConsoleHandler handler = new ConsoleHandler();
		 handler.setFormatter(new SimpleFormatter());
		 handler.setLevel(Level.WARNING);
		 log.addHandler(handler);
		 */
		 log.addHandler(new StreamHandler(System.out, new SimpleFormatter()));
		 log.fine("hello world");
		
	}

	/**
	 * Registers the driver for Database access
	 * @throws ClassNotFoundException 
	 */
	private void registerDriver() throws ClassNotFoundException {
		stopWatch.start();
		Class.forName(this.driverClass); //.newInstance();
		stopWatch.stop();
		timings[Timing.REGISTER_DRIVER.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
		System.out.println("[INFO] Registered the database driver! ["+this.driverClass+"]");
		System.out.flush();
	}

	//Initialize Sessions Alterations
	private void initSessionParams() {
		alterations = new LinkedList<String>();
		//Loading Alterations
		File alterationsFile = new File(System.getProperty("alter", ""));
		if (alterationsFile.canRead()) {
			try {
				BufferedReader fileRdr = new BufferedReader(new FileReader(alterationsFile));
				String lineRead = null;
				while ((lineRead=fileRdr.readLine()) != null) {
					if (!lineRead.startsWith("--") && lineRead.trim().length() > 0) {
						if (lineRead.trim().endsWith(";")) {
							lineRead = lineRead.replaceFirst(";", " ");
						}
						alterations.add(lineRead);
					}
				}
				fileRdr.close();
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}		
	}

	// Initialize output (screen:SysOut / fileName:file)
	private void initOutputFormat() {
		this.outputFileName = System.getProperty("output", null);
		if (outputFileName != null) {
			this.showResults = true;
			if (outputFileName.equalsIgnoreCase("screen")) 
				outputStream = System.out;
			else {
				try {
					outputStream = new PrintStream(new FileOutputStream(outputFileName), true);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
	}

	//Initialize JDBC Parameters
	private void initJdbcParams() {
		this.driverClass = System.getProperty("driver", JDBCDriver.DRILL.getDriverClass());
		this.connURL = System.getProperty("conn", "jdbc:drill:zk=localhost:5181");
		this.userName = System.getProperty("user", "admin");
		this.userPassword = System.getProperty("password", "admin");
		this.timeout = Integer.valueOf(System.getProperty("timeout", "10"));
		if (driverClass.equalsIgnoreCase(JDBCDriver.DRILL.getDriverClass())) {
			isDrillDriver = true;
		}
		this.mustPrepareStatement = Boolean.valueOf(System.getProperty("prepare", "false"));
	}

	//Captures Explanation
	private void checkForExplanation() {
		this.explainPlan = System.getProperty("explain", null);
		if (explainPlan != null) {
			//Drill
			if (jdbcDriver == JDBCDriver.DRILL) {
				explainVerbosePlanPrefix = "explain plan including all attributes for ";
				explainPlanPrefix = "explain plan for ";
			} else { //TODO: Impala?
				explainPlanPrefix = "explain "; 
			}
		}
	}

	private void loadQuery() throws IOException {
		System.out.println("[INFO] Loading query from file: " + queryFile.getName());
		System.out.flush();
		stopWatch.start();
	
		StringBuilder tmpQuery = new StringBuilder();
		BufferedReader fileRdr = new BufferedReader(new FileReader(queryFile));
		String lineRead = null;
		while ((lineRead=fileRdr.readLine()) != null) {
			if (!lineRead.startsWith("--")) {
				if (lineRead.trim().endsWith(";"))
					lineRead = lineRead.replaceFirst(";", " ");
				tmpQuery.append(lineRead.trim()+ " ");
			}
		}
		fileRdr.close();
		this.query = tmpQuery.toString();
		if (explainPlan != null) {
			if (explainPlan.equalsIgnoreCase("verbose")) 
				this.query = explainVerbosePlanPrefix  + tmpQuery.toString();
			else
				this.query = explainPlanPrefix  + tmpQuery.toString();
		}
		//"select * from region_par100;";
		stopWatch.stop();
		timings[Timing.LOADING_QUERY.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	//Runs the actual test
	private void runTest() throws SQLException, IOException, ClassNotFoundException {
		/* All Steps are Timed!
		 * 1. Load Driver
		 * 2. Load Query from File
		 * 2. Connect to DB
		 * 3. Execute Query
		 * 4. Fetch Rows (Time 1st Row)
		 * 5. Close Connection
		 */
		loadQuery();
		System.out.println("Running Query:: " + this.queryFile.getName());
		System.out.flush();
		registerDriver();
		connectToDB();

		alterSession();

		prepareStmt();
		executeQuery();
		if (explainPlan == null) //Dont need Query IDs for ExplainPlans
			getQueryID();

		if (explainPlan != null)
			showPlan();
		else if (showResults)
			displayRows();
		else
			fetchRows();
		disconnect();
		printSummary();
	}

	// Get Query ID for Drill Drivers
	private String getQueryID() {
		String queryId = null;
		stopWatch.start();

		//Incompatible with Impala: if (this.resultSet instanceof DrillResultSet) {
		if (isDrillDriver) {
			DrillResultSet rs = (DrillResultSet) this.resultSet;
			if (rs != null) {
				try {
					queryId = ((DrillResultSet)this.resultSet).getQueryId();
				} catch (SQLException e) { e.printStackTrace(); }
				if (queryId != null) {
					System.out.println("[QUERYID] "+queryId);
					System.out.flush();
				}
			}
		}
		stopWatch.stop();
		timings[Timing.GET_QUERY_ID.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
		return queryId;
	}

	private void connectToDB() throws SQLException {
		System.out.println("[INFO] Connecting to DB");
		System.out.println("[INFO] Connection: " + connURL);
		System.out.flush();
		stopWatch.start();
	
		connection = DriverManager.getConnection(
				this.connURL,
				this.userName, this.userPassword);
		stopWatch.stop();
		timings[Timing.CONNECT.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	private void alterSession() throws IOException, SQLException {
		System.out.println("[INFO] Altering Session");
		System.out.flush();
		stopWatch.start();
		for (String alteration : alterations) {
			Statement alterStmt = connection.createStatement();
			System.out.println("[WARN] Applying Alteration: " + alteration);
			alterStmt.execute(alteration);
			alterStmt.close();
		}
		stopWatch.stop();
		timings[Timing.ALTER_SESSION.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
	}

	@Deprecated
	private void prepareStmt() {
		System.out.println("[INFO] Preparing Statement");
		System.out.flush();

		stopWatch.start();		
		try {
			if (mustPrepareStatement) {
				prepStatement = connection.prepareStatement(query);
			} else {
				statement = connection.createStatement();
			}	
		} catch (SQLException e) {
			e.printStackTrace();
		}
		stopWatch.stop();
		timings[Timing.PREP_STMT.ordinal()] = stopWatch.getTime();
		stopWatch.reset();

		try {
			if (mustPrepareStatement) {
				prepStatement.setQueryTimeout(timeout);
			} else {
				statement.setQueryTimeout(timeout);
			}
		} catch (SQLException noTimeout) {
			noTimeout.printStackTrace(System.err);
			queryTimer = new QueryTimer();
		}
		System.out.println("[WARN] Setting timeout as "+timeout+"sec");
		System.out.flush();

	}

	//Execute the query
	private void executeQuery() {
		skipRowFetchDueToFailure = false;
		if (queryTimer != null)
			queryTimer.start();
		System.out.println("[INFO] Executing query...");
		System.out.flush();
		stopWatch.start();
	
		try {
			if (mustPrepareStatement) {
				resultSet = prepStatement.executeQuery();
			} else {
				resultSet = statement.executeQuery(query);
			}
		} catch (SQLException e) {
			skipRowFetchDueToFailure = true;
			e.printStackTrace();
			System.err.println("[ERROR] Unable to execute " + retrieveCause(e));
			System.err.flush();
		}
		stopWatch.stop();
		timings[Timing.EXECUTE.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	private void fetchRows() {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) {
			System.err.println("[ERROR] Skipping fetch() due to execute() failure");
			System.err.flush();
			return;
		}
		System.out.println("[INFO] Fetching rows...");
		System.out.flush();
		stopWatch.start();
	
		@SuppressWarnings("unused")
		String dummyColValue;
		//Fetching 1st n-rows
		try {
			//Fetching and Clocking 1st Row
			this.resultSet.next();
			stopWatch.split();
			timings[Timing.FETCH_FIRST_DONE.ordinal()] = stopWatch.getSplitTime();
			
			rowsRead++;
			//Fetching remaining rows
			while (this.resultSet.next()) {
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println("[ERROR] Unable to fetch all rows (got only "+rowsRead+") " + retrieveCause(e));
			System.err.flush();
			return;
		} finally {
			stopWatch.stop();
			timings[Timing.FETCH_ALL_ROWS.ordinal()] = stopWatch.getTime();
			stopWatch.reset();
			
		}
	}

	private void displayRows() throws SQLException {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) {
			System.err.println("[ERROR] Skipping fetch() due to execute() failure");
			System.err.flush();
			return;
		}
		int columnCount = this.resultSet.getMetaData().getColumnCount();
		System.out.println("[INFO] Displaying rows for "+ columnCount +" columns...");
		System.out.flush();
		stopWatch.start();
		@SuppressWarnings("unused")
		String dummyColValue;
		//Fetching 1st n-rows
		try {
			boolean isFirst = true;
			//Fetching remaining rows
			while (this.resultSet.next()) {
				if (isFirst) {
					//Fetching and Clocking 1st Row
					stopWatch.split();
					timings[Timing.FETCH_FIRST_DONE.ordinal()] = stopWatch.getSplitTime();
					
					isFirst = false;
				}
				for (int i = 1; i <= columnCount; i++) {
					outputStream.print(resultSet.getString(i)+ ( i == columnCount ? EOL : DELIMITER));
				}
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (!outputFileName.equalsIgnoreCase("screen"))
				outputStream.close();
		}
		stopWatch.stop();
		timings[Timing.FETCH_ALL_ROWS.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	private void showPlan() {
		rowsRead = 0L;
		if (skipRowFetchDueToFailure) return;
		System.out.println("[INFO] So the plan is...");
		System.out.flush();
		stopWatch.start();
	
		@SuppressWarnings("unused")
		String dummyColValue;
	
		//Fetching 1st n-rows
		try {
			//Fetching and Clocking 1st Row
			this.resultSet.next();
			System.out.println(this.resultSet.getString(1));
			System.out.flush();
			stopWatch.split();
			timings[Timing.FETCH_FIRST_DONE.ordinal()] = stopWatch.getSplitTime();
			
			rowsRead++;
	
			//Fetching remaining rows
			while (this.resultSet.next()) {
				System.out.println(this.resultSet.getString(1));
				System.out.flush();
				rowsRead++;		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		stopWatch.stop();
		timings[Timing.FETCH_ALL_ROWS.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	private void disconnect() {
		stopWatch.start();
	
		//Closing ResultSet 
		try {
			if (this.resultSet != null ) {
				this.resultSet.close();
				System.out.println("[INFO] Releasing ResultSet resources");
				System.out.flush();
			}
		}
		catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		//Closing SQLStatement Object 
		try {
			if (this.statement != null ) {
				if (!this.statement.isClosed())
					this.statement.close();
				System.out.println("[INFO] Releasing JDBC (Statement) resources");
				System.out.flush();
			}	
		} catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		//Closing Connection Handle 
		try {
			if (this.connection != null ) {
				this.connection.close();
				System.out.println("[INFO] Closed connection ");
				System.out.flush();
			}	
		} catch (SQLException sqlExcp) {	 
			System.out.println("[FATAL (DATABASE : )]: "+sqlExcp.getLocalizedMessage());
			sqlExcp.printStackTrace(); //[NotExitingForNow] System.exit(2);
		}
		stopWatch.stop();
		timings[Timing.DISCONNECT.ordinal()] = stopWatch.getTime();
		stopWatch.reset();
		
	}

	private void printSummary() {
		System.out.println("[STAT] Query : \n\t" + query);
		System.out.println("[STAT] Rows Fetched : " + rowsRead);
		for (Timing timing : Timing.values()) {
			System.out.println("[STAT] Time "+timing.name()+": " + timings[timing.ordinal()] + " msec" );
		}
		/** delMe? 
		System.out.println("[STAT] Time to load queries : " + loadingQueryTime + " msec" );
		System.out.println("[STAT] Time to register Driver : " + registerDriverTime + " msec");
		System.out.println("[STAT] Time to connect : " + connectTime + " msec");
		System.out.println("[STAT] Time to alter session : " + alterSessionTime + " msec");
		System.out.println("[STAT] Time to prep Statement  : " + prepStmtTime + " msec");
		System.out.println("[STAT] Time to execute query : " + executeTime + " msec");
		System.out.println("[STAT] Time to get query ID : " + getQueryIdTime + " msec");
		System.out.println("[STAT] Time to fetch 1st Row : " + fetchFirstDone + " msec");
		System.out.println("[STAT] Time to fetch All Rows : " + fetchAllRowsTime + " msec");
		System.out.println("[STAT] Time to disconnect : " + disconnectTime + " msec");
		**/
		System.out.println("[STAT] TOTAL TIME : " 
		+ (timings[Timing.EXECUTE.ordinal()] + timings[Timing.FETCH_ALL_ROWS.ordinal()]) + " msec");
		System.out.flush();
	}

	//Retrieves the Root Cause in the stackTrace
	private String retrieveCause(SQLException e) {
		String causeText = "";
		Throwable cause = e;
		while(cause != null) {
			causeText = cause.toString();
			cause = cause.getCause();
		}
		return causeText;
	}
	
	/**
	 * Timer class (if underlying database lacks timer support)
	 */
	class QueryTimer extends Thread {
		public QueryTimer() {
			isCancelled = false;
			System.out.println("[INFO] Using QueryTimer thread to timeout in "+timeout+" sec");
			System.out.flush();
		}
		@Override
		public void run() {
			try {
				sleep(TimeUnit.SECONDS.toMillis(timeout));
				log.info("[TIME OUT] Query took more than "+timeout+" sec.");
				System.err.println("[TIME OUT] Query took more than "+timeout+" sec.");				
				System.out.flush();
				cancelQuery();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			isCancelled = true;
		}

		public void cancelQuery() {
			try {
				//Cancelling Query
				statement.cancel();
				if (!statement.isClosed())
					statement.close();		
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * JDBC Driver List
	 * @author kkhatua
	 */
	enum JDBCDriver {
		DRILL("org.apache.drill.jdbc.Driver"),
		IMPALA("com.cloudera.impala.jdbc41.Driver"),
		HIVE("org.apache.hive.jdbc.HiveDriver"),
		DERBY("org.apache.derby.jdbc.ClientDriver");

		private String driverClass;

		private JDBCDriver(String className) {
			this.driverClass = className;
		}

		public String getDriverClass() {
			return driverClass;
		};
	}
	
	/**
	 * Timing List 
	 * @author kkhatua
	 *
	 */
	enum Timing {
		REGISTER_DRIVER("RegisterDriver"),
		LOADING_QUERY("LoadingQuery"),
		CONNECT("Connect"),
		ALTER_SESSION("AlterSession"),		
		EXECUTE("Execute"),
		GET_QUERY_ID("GetQueryId"),
		FETCH_FIRST_DONE("FetchFirstDone"),
		FETCH_ALL_ROWS("FetchAllRows"),
		DISCONNECT("Disconnect"),
		PREP_STMT("PrepareStatement");
		
		private String name;
		
		private Timing(String description) {
			this.name = description;
		}
	}
}


