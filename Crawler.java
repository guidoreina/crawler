import java.util.logging.Level;
import java.net.URL;

public class Crawler {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Constants.                                                           ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private static final String DEFAULT_DATABASE_NAME = "urlsDB";
  private static final String DEFAULT_TEMP_DIRECTORY = "tmpdata";
  private static final String DEFAULT_FINAL_DIRECTORY = "data";

  private static final String DEFAULT_HTTP_USER_AGENT =
            "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 " +
            "Firefox/38.0 Iceweasel/38.7.1";

  private static final String DEFAULT_LOG_FILENAME = "crawler.log";
  private static final Level DEFAULT_LOG_LEVEL = Level.FINEST;

  private static final long CHECK_INTERVAL = 500; // Milliseconds.


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Thread mainThread = null;

  private Log log = null;
  private Database database = null;
  private Downloader downloader = null;
  private UrlsExtractor urlsExtractor = null;

  private String tempDir = null;
  private String finalDir = null;
  private String httpUserAgent = null;
  private String logFilename = null;
  private Level logLevel = null;

  private boolean running = true;


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Methods.                                                             ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  // Method: Constructor
  // Description: sets the data members.
  // Parameters:
  //   - tempDir: temporary directory where to download the files.
  //   - finalDir: final directory where to save the downloaded files.
  //   - httpUserAgent: user agent to be used in the HTTP requests.
  //   - logFilename: name of the log file.
  //   - logLevel: log level to be used for logging.
  //
  // Returns: nothing.
  private Crawler(String tempDir,
                  String finalDir,
                  String httpUserAgent,
                  String logFilename,
                  Level logLevel)
  {
    this.mainThread = Thread.currentThread();

    this.tempDir = tempDir;
    this.finalDir = finalDir;
    this.httpUserAgent = httpUserAgent;
    this.logFilename = logFilename;
    this.logLevel = logLevel;
  }


  // Method: initialize
  // Description: initializes the crawler:
  //                - Creates and initializes the logger object.
  //                - Creates and initializes the database object.
  //                - Creates and initializes the downloader object.
  //                - Creates and initializes the URLs extractor object.
  //
  // Parameters:
  //   - host: name of the host containing the database server;
  //           null if the embedded driver should be used.
  //   - port: port in which the database server is listening;
  //           -1 if the embedded driver should be used.
  //   - databaseName: name of the database.
  //
  // Returns: true: the crawler could be initialized; false: otherwise.
  private boolean initialize(String host, int port, String databaseName)
  {
    // Create logger object.
    log = new Log();

    // Initialize logger.
    if (log.initialize(logFilename, logLevel)) {
      // Create database object.
      database = new Database(log);

      // Initialize database.
      boolean initialized = (host != null) ?
                              database.initialize(host, port, databaseName) :
                              database.initialize(databaseName);

      if (initialized) {
        // Create downloader object.
        downloader = new Downloader(database,
                                    tempDir,
                                    finalDir,
                                    httpUserAgent,
                                    log);

        // Initialize downloader.
        if (downloader.initialize()) {
          // Create URLs extractor object.
          urlsExtractor = new UrlsExtractor(database, log);

          // Initialize URLs extractor.
          if (urlsExtractor.initialize()) {
            return true;
          } else {
            // Shutdown database.
            database.shutdown();
          }
        } else {
          // Shutdown database.
          database.shutdown();
        }
      }
    }

    return false;
  }


  // Method: run
  // Description: main loop:
  //                - Gets the next URL to be visited.
  //                  If some URL can be visited:
  //                    - Makes an HTTP request and saves the response in a
  //                      data file.
  //                    - If the data file should be processed (the Content-Type
  //                      is "text/html"):
  //                        - Extracts the URLs from the data file and saves
  //                          them in the database.
  //
  // Parameters: none.
  // Returns: nothing.
  private void run()
  {
    MutableLong wait = new MutableLong();
    MutableBoolean process = new MutableBoolean();
    StringBuilder filename = new StringBuilder();

    do {
      // Get from the database the next URL to visit.
      URL url;
      if ((url = database.getNextUrlToVisit(wait)) != null) {
        process.value = false;
        filename.setLength(0);

        // Download file.
        if (downloader.request(url, 0, process, filename)) {
          // If the file should be processed...
          if (process.value) {
            urlsExtractor.processFile(filename.toString());
          }
        }

        // Remove URL from the list of URLs to visit.
        database.removeUrlToVisit(url);
      } else {
        long ms = (wait.value > 0) ? Math.min(wait.value, CHECK_INTERVAL) :
                                     CHECK_INTERVAL;

        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
      }
    } while (running);
  }


  // Method: help
  // Description: shows the usage.
  // Parameters: none.
  // Returns: nothing.
  private static void help()
  {
    System.out.println("Options:");
    System.out.println("\t--host <host>");
    System.out.println("\t--port <port>");

    System.out.println("\t--database-name <database-name> (default: " +
                       DEFAULT_DATABASE_NAME +
                       ").");

    System.out.println("\t--temp-dir <directory> (default: " +
                       DEFAULT_TEMP_DIRECTORY +
                       ").");

    System.out.println("\t--final-dir <directory> (default: " +
                       DEFAULT_FINAL_DIRECTORY +
                       ").");

    System.out.println("\t--user-agent <user-agent> (default: " +
                       DEFAULT_HTTP_USER_AGENT +
                       ").");

    System.out.println("\t--log-filename <log-filename> (default: " +
                       DEFAULT_LOG_FILENAME +
                       ").");

    System.out.println("\t--log-level <log-level> (default: " +
                       DEFAULT_LOG_LEVEL.getName() +
                       ").");

    System.out.println();
  }


  // Method: main
  // Description: checks the command-line arguments, creates and initializes a
  //              crawler object and calls its run() method.
  //
  // Parameters: array of command-line arguments.
  // Returns: nothing.
  public static void main(String[] args)
  {
    String host = null;
    int port = -1;
    String databaseName = DEFAULT_DATABASE_NAME;
    String tempDir = DEFAULT_TEMP_DIRECTORY;
    String finalDir = DEFAULT_FINAL_DIRECTORY;
    String httpUserAgent = DEFAULT_HTTP_USER_AGENT;
    String logFilename = DEFAULT_LOG_FILENAME;
    Level logLevel = DEFAULT_LOG_LEVEL;

    // Check arguments.
    int i = 0;
    while (i < args.length) {
      if (args[i].equals("--host")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        host = args[i + 1];

        i += 2;
      } else if (args[i].equals("--port")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        try {
          port = Integer.parseInt(args[i + 1]);

          if ((port < 1) || (port > 65535)) {
            System.out.println("Invalid port '" + args[i + 1] + "'.");
            return;
          }
        } catch (NumberFormatException e) {
          System.out.println("Invalid port '" + args[i + 1] + "'.");
          return;
        }

        i += 2;
      } else if (args[i].equals("--database-name")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        databaseName = args[i + 1];

        i += 2;
      } else if (args[i].equals("--temp-dir")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        tempDir = args[i + 1];

        i += 2;
      } else if (args[i].equals("--final-dir")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        finalDir = args[i + 1];

        i += 2;
      } else if (args[i].equals("--user-agent")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        httpUserAgent = args[i + 1];

        i += 2;
      } else if (args[i].equals("--log-filename")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        logFilename = args[i + 1];

        i += 2;
      } else if (args[i].equals("--log-level")) {
        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        try {
          logLevel = Level.parse(args[i + 1]);
        } catch (IllegalArgumentException e) {
          System.out.println("Invalid level '" + args[i + 1] + "'.");
          return;
        }

        i += 2;
      } else {
        help();
        return;
      }
    }

    if (host != null) {
      if (port == -1) {
        System.out.println("A host has been specified but no port.");
        return;
      }
    } else {
      if (port != -1) {
        System.out.println("A port has been specified but no host.");
        return;
      }
    }

    // Create crawler object.
    Crawler crawler = new Crawler(tempDir,
                                  finalDir,
                                  httpUserAgent,
                                  logFilename,
                                  logLevel);

    // Initialize crawler.
    if (crawler.initialize(host, port, databaseName)) {
      // Add shutdown hook.
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run()
        {
          crawler.running = false;

          crawler.log.log(Level.INFO, "Exiting...");

          try {
            crawler.mainThread.join();
          } catch (InterruptedException e) {
          }
        }
      });

      crawler.run();

      // Shutdown database.
      crawler.database.shutdown();
    }
  }
}
