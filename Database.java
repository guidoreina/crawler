import java.util.Date;
import java.util.logging.Level;
import java.net.URI;
import java.net.URL;
import java.net.URISyntaxException;
import java.net.MalformedURLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.SQLException;

public class Database {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Constants.                                                           ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private static final int URL_MAX_LEN = 2048;
  private static final int HOST_MAX_LEN = 255;
  private static final int SERVER_MAX_LEN = 255;
  private static final int FILENAME_MAX_LEN = 255;
  private static final long HOST_VISIT_INTERVAL = 5000; // Milliseconds.

  private static final String EMBEDDED_DRIVER =
                              "org.apache.derby.jdbc.EmbeddedDriver";

  private static final String CLIENT_DRIVER =
                              "org.apache.derby.jdbc.ClientDriver";

  private static final String FORMAT_EMBEDDED_CONNECTION_URL =
                              "jdbc:derby:%s;create=true";

  private static final String FORMAT_CLIENT_CONNECTION_URL =
                              "jdbc:derby://%s:%d/%s;create=true";

  private static final String VISITED_URLS = "VISITED_URLS";

  private static final String CREATE_VISITED_URLS =
                              "CREATE TABLE " +
                              VISITED_URLS +
                              " (URL VARCHAR(" +
                              URL_MAX_LEN +
                              ") NOT NULL, TIMESTAMP TIMESTAMP NOT NULL, " +
                              "FILENAME VARCHAR(" +
                              FILENAME_MAX_LEN +
                              ") NOT NULL, PRIMARY KEY (URL))";

  private static final String VISITED_HOSTS = "VISITED_HOSTS";

  private static final String CREATE_VISITED_HOSTS =
                              "CREATE TABLE " +
                              VISITED_HOSTS +
                              " (HOST VARCHAR(" +
                              HOST_MAX_LEN +
                              ") NOT NULL, TIMESTAMP TIMESTAMP NOT NULL, " +
                              "SERVER VARCHAR(" +
                              SERVER_MAX_LEN +
                              "), PRIMARY KEY (HOST))";

  private static final String URLS_TO_VISIT = "URLS_TO_VISIT";

  private static final String CREATE_URLS_TO_VISIT =
                              "CREATE TABLE " +
                              URLS_TO_VISIT +
                              " (URL VARCHAR(" +
                              URL_MAX_LEN +
                              ") NOT NULL, HOST VARCHAR(" +
                              HOST_MAX_LEN +
                              ") NOT NULL, WHEN TIMESTAMP NOT NULL, " +
                              "PRIMARY KEY (URL))";


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Type declarations.                                                   ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private enum Action {
    NONE,
    VIEW_TABLES,
    VIEW_TABLE_VISITED_URLS,
    VIEW_TABLE_VISITED_HOSTS,
    VIEW_TABLE_URLS_TO_VISIT,
    DROP_TABLES,
    DROP_TABLE_VISITED_URLS,
    DROP_TABLE_VISITED_HOSTS,
    DROP_TABLE_URLS_TO_VISIT,
    ADD_URL_TO_VISIT,
    REMOVE_URL_TO_VISIT
  };


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Connection conn = null;

  private Log log = null;


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Methods.                                                             ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  // Method: Constructor
  // Description: sets the data member "log".
  // Parameters:
  //   - log: logger object.
  //
  // Returns: nothing.
  public Database(Log log)
  {
    this.log = log;
  }


  // Method: initialize
  // Description: initializes the database using the embedded driver.
  // Parameters: none.
  // Returns: true: the database could be initialized; false: otherwise.
  public boolean initialize(String databaseName)
  {
    return initialize(EMBEDDED_DRIVER,
                      String.format(FORMAT_EMBEDDED_CONNECTION_URL,
                                    databaseName));
  }


  // Method: initialize
  // Description: initializes the database using the client driver.
  // Parameters: none.
  // Returns: true: the database could be initialized; false: otherwise.
  public boolean initialize(String host, int port, String databaseName)
  {
    return initialize(CLIENT_DRIVER,
                      String.format(FORMAT_CLIENT_CONNECTION_URL,
                                    host,
                                    port,
                                    databaseName));
  }


  // Method: initialize
  // Description: initializes the database:
  //                - Starts the derby engine.
  //                - Connects to the database.
  //                - Creates tables (if not already done).
  //
  // Parameters:
  //   - driver: name of the database driver to be used.
  //   - url: a database URL of the form jdbc:<subprotocol>:<subname>
  //
  // Returns: true: the database could be initialized; false: otherwise.
  private boolean initialize(String driver, String url)
  {
    try {
      // Start the derby engine.
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.log(Level.SEVERE,
              "Cannot start the derby engine: '" + e.toString() + "'.");

      return false;
    }

    try {
      // Connect to the database.
      conn = DriverManager.getConnection(url);

      // Create tables.
      return createTable(VISITED_URLS, CREATE_VISITED_URLS) &&
             createTable(VISITED_HOSTS, CREATE_VISITED_HOSTS) &&
             createTable(URLS_TO_VISIT, CREATE_URLS_TO_VISIT);
    } catch (SQLException e) {
      log.log(Level.SEVERE, "Database error: '" + e.toString() + "'.");
    }

    return false;
  }


  // Method: shutdown
  // Description: shutdowns the database.
  // Parameters: none.
  // Returns: true: the database could be shut down; false: otherwise.
  public boolean shutdown()
  {
    try {
      // Shutdown database.
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    } catch (SQLException e) {
      if (e.getSQLState().equals("XJ015")) {
        log.log(Level.INFO, "The database has been shut down.");
        return true;
      }

      log.log(Level.WARNING,
              "Error shutting down database (" + e.toString() + ").");
    }

    return false;
  }


  // Method: dropTable
  // Description: drops a table from the database.
  // Parameters:
  //   - table: name of the table to be dropped.
  //
  // Returns: true: the table could be dropped or doesn't exist;
  //          false: otherwise.
  private boolean dropTable(String table)
  {
    PreparedStatement statement = null;

    try {
      // Drop table.
      statement = conn.prepareStatement("DROP TABLE " + table);
      statement.executeUpdate();

      log.log(Level.FINE, "The table " + table + " has been dropped.");
      return true;
    } catch (SQLException e) {
      if (e.getSQLState().equals("42Y55")) {
        // '<value>' cannot be performed on '<value>' because it does not exist.
        log.log(Level.FINE,
                "The table " +
                table +
                " could not be dropped because it doesn't exist.");

        return true;
      }

      log.log(Level.WARNING,
              "Error dropping table " + table + " (" + e.toString() + ").");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: addVisitedUrl
  // Description: adds a URL to the table of visited URLs.
  //              If the URL is not in the table of visited URLs:
  //                - Adds the URL to the table of visited URLs with the
  //                  timestamp when the URL was visited and the name of
  //                  the data file.
  //                - Extracts the hostname from the URL and calls the method
  //                  addVisitedHost() to add the hostname, timestamp and the
  //                  HTTP header "Server" to the table of visited hosts.
  //
  //              If the URL is already in the table of visited URLs:
  //                - Returns success.
  //
  // Parameters:
  //   - url: URL which has been visited.
  //   - server: HTTP header "Server".
  //   - filename: name of the data file.
  //
  // Returns: true:
  //            - The URL could be added to the table of visited URLs and the
  //              hostname to the table of visited hosts.
  //            or:
  //              - The URL was already in the table of visited URLs.
  //          false: otherwise.
  public boolean addVisitedUrl(URL url, String server, String filename)
  {
    if ((server.length() <= SERVER_MAX_LEN) &&
        (filename.length() <= FILENAME_MAX_LEN)) {
      String host = url.getHost();
      if (host.length() <= HOST_MAX_LEN) {
        String urlStr = url.toString();
        if (urlStr.length() <= URL_MAX_LEN) {
          PreparedStatement statement = null;

          try {
            statement = conn.prepareStatement("INSERT INTO " +
                                              VISITED_URLS +
                                              " (URL, TIMESTAMP, FILENAME) " +
                                              "VALUES (?, ?, ?)");

            statement.setString(1, urlStr);

            Date date = new Date();
            Timestamp timestamp = new Timestamp(date.getTime());

            statement.setTimestamp(2, timestamp);
            statement.setString(3, filename);

            statement.executeUpdate();

            log.log(Level.FINEST,
                    "Added visited URL '" +
                    urlStr +
                    "', timestamp: '" +
                    timestamp.toString() +
                    "', filename: '" +
                    filename +
                    "'.");

            return addVisitedHost(host, timestamp, server);
          } catch (SQLException e) {
            if (e.getSQLState().equals("23505")) {
              // The statement was aborted because it would have caused a
              // duplicate key value in a unique or primary key constraint or
              // unique index identified by '<value>' defined on '<value>'.
              log.log(Level.FINEST,
                      "Visited URL '" + urlStr + "' already added.");

              return true;
            }

            log.log(Level.WARNING,
                    "Error adding visited URL (" + e.toString() + ").");
          } finally {
            if (statement != null) {
              try {
                statement.close();
              } catch (SQLException e) {
                log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
              }
            }
          }
        }
      }
    }

    return false;
  }


  // Method: addUrlToVisit
  // Description: adds a URL to the table of URLs to visit.
  //              If the URL has not been already visited and is not already
  //              in the table of URLs to visit:
  //                - Calculates in the following way the timestamp "when" when
  //                  the URL can be visited:
  //                    - Gets from the table of URLs to visit the latest "when"
  //                      for the same host.
  //                      If found: the URL can be visited, the earliest, at:
  //                        "when" + HOST_VISIT_INTERVAL.
  //                      If not found:
  //                    - Gets from the table of visited hosts the timestamp
  //                      "timestamp" when the host was last visited.
  //                      If found: the URL can be visited, the earliest, at:
  //                        "timestamp" + HOST_VISIT_INTERVAL.
  //                      If not found:
  //                        Sets "when" to current time.
  //                - Adds the URL to visit.
  //
  //              If the URL has been already visited or is already in the
  //              table of URLs to visit:
  //                - Returns success.
  //
  // Parameters:
  //   - url: URL to visit.
  //
  // Returns: true:
  //            - The URL could be added to the table of URLs to visit.
  //            or:
  //              - The URL has been already visited.
  //            or:
  //              - The URL is already in the table of URLs to visit.
  //          false: otherwise.
  public boolean addUrlToVisit(URL url)
  {
    String host = url.getHost();
    if (host.length() <= HOST_MAX_LEN) {
      String urlStr = url.toString();
      if (urlStr.length() <= URL_MAX_LEN) {
        PreparedStatement statement = null;

        try {
          if ((!urlVisited(urlStr)) && (!haveUrlToVisit(urlStr))) {
            // Get the latest 'WHEN' for the host 'host' in the table
            // 'URLS_TO_VISIT'.
            Timestamp when = getLatestWhen(host);

            if (when != null) {
              when.setTime(when.getTime() + HOST_VISIT_INTERVAL);
            } else {
              // Get the timestamp when the host was last visited.
              Timestamp timestamp;
              if ((timestamp = getHostTimestamp(host)) != null) {
                when = new Timestamp(timestamp.getTime() + HOST_VISIT_INTERVAL);
              } else {
                // The host can be visited now.
                Date now = new Date();
                when = new Timestamp(now.getTime());
              }
            }

            statement = conn.prepareStatement("INSERT INTO " +
                                              URLS_TO_VISIT +
                                              " (URL, HOST, WHEN) " +
                                              "VALUES (?, ?, ?)");

            statement.setString(1, urlStr);
            statement.setString(2, host);
            statement.setTimestamp(3, when);

            statement.executeUpdate();

            log.log(Level.FINEST,
                    "Added URL to visit '" +
                    urlStr +
                    "', host: '" +
                    host +
                    "', when: '" +
                    when.toString() +
                    "'.");
          }

          return true;
        } catch (SQLException e) {
          log.log(Level.WARNING,
                  "Error adding URL to visit (" + e.toString() + ").");
        } finally {
          if (statement != null) {
            try {
              statement.close();
            } catch (SQLException e) {
              log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
            }
          }
        }
      }
    }

    return false;
  }


  // Method: removeUrlToVisit
  // Description: removes a URL from the table of URLs to visit.
  // Parameters:
  //   - url: URL to be removed.
  //
  // Returns: true:
  //            - The URL could be removed from the table of URLs to visit.
  //            or:
  //              - The URL is not in the table of URLs to visit.
  //          false: otherwise.
  public boolean removeUrlToVisit(URL url)
  {
    String host = url.getHost();
    if (host.length() <= HOST_MAX_LEN) {
      String urlStr = url.toString();
      if (urlStr.length() <= URL_MAX_LEN) {
        return removeUrlToVisit(urlStr);
      }
    }

    return false;
  }


  // Method: removeUrlToVisit
  // Description: removes a URL from the table of URLs to visit.
  // Parameters:
  //   - urlStr: URL to be removed.
  //
  // Returns: true:
  //            - The URL could be removed from the table of URLs to visit.
  //            or:
  //              - The URL is not in the table of URLs to visit.
  //          false: otherwise.
  private boolean removeUrlToVisit(String urlStr)
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("DELETE FROM " +
                                        URLS_TO_VISIT +
                                        " WHERE URL = ?");

      statement.setString(1, urlStr);
      statement.executeUpdate();

      log.log(Level.FINEST, "Removed URL to visit '" + urlStr + "'.");

      return true;
    } catch (SQLException e) {
      log.log(Level.WARNING,
              "Error removing URL to visit (" + e.toString() + ").");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: getNextUrlToVisit
  // Description: gets the next URL to visit with a timestamp "when" before
  //              current time.
  //              If there is such a URL:
  //                - If the URL is valid:
  //                  - Returns the URL.
  //                - If the URL is invalid:
  //                  - Deletes the URL from the table of URLs to visit.
  //                  - Tries to get the next URL to visit.
  //              If there are URLs which cannot be visited yet:
  //                - Sets the parameter "wait" to the number of milliseconds
  //                  until the next URL can be visited.
  //              If there are no more URLs:
  //                - Sets the parameter "wait" to 0.
  //
  // Parameters:
  //   - wait: if no URL can be visited at the moment, set to the number of
  //           milliseconds until the next URL can be visited. If there are no
  //           URLs to visit, set to 0.
  //
  // Returns: next URL to visit if some URL can be visited now; null: otherwise.
  public URL getNextUrlToVisit(MutableLong wait)
  {
    Date now = new Date();

    do {
      PreparedStatement statement = null;

      try {
        statement = conn.prepareStatement("SELECT URL, WHEN from " +
                                          URLS_TO_VISIT +
                                          " ORDER BY WHEN ASC");

        ResultSet rs = statement.executeQuery();

        if (rs.next()) {
          Timestamp when = rs.getTimestamp("WHEN");

          if (when.before(now)) {
            String urlStr = rs.getString("URL");

            rs.close();

            try {
              // Create URI.
              URI uri = new URI(urlStr);

              // Convert URI to URL.
              return uri.toURL();
            } catch (URISyntaxException | MalformedURLException e) {
              log.log(Level.WARNING,
                      "Invalid URL found (" +
                      urlStr +
                      ") in the table " +
                      URLS_TO_VISIT +
                      ".");

              removeUrlToVisit(urlStr);

              continue;
            }
          } else {
            rs.close();

            wait.value = when.getTime() - now.getTime();
            return null;
          }
        } else {
          rs.close();

          wait.value = 0;
          return null;
        }
      } catch (SQLException e) {
        log.log(Level.WARNING,
                "Error getting next URL to visit (" + e.toString() + ").");
      } finally {
        if (statement != null) {
          try {
            statement.close();
          } catch (SQLException e) {
            log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
          }
        }
      }

      return null;
    } while (true);
  }


  // Method: printVisitedUrls
  // Description: displays the table of visited URLs.
  // Parameters: none.
  // Returns: true: the table of visited URLs could be displayed;
  //          false: otherwise.
  private boolean printVisitedUrls()
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT * from " + VISITED_URLS);
      ResultSet rs = statement.executeQuery();

      System.out.println("Visited URLs:");
      System.out.println("===========================================");

      int count = 0;

      while (rs.next()) {
        count++;

        String url = rs.getString("URL");
        Timestamp timestamp = rs.getTimestamp("TIMESTAMP");
        String filename = rs.getString("FILENAME");

        System.out.println("[" +
                           count +
                           "] URL: '" +
                           url +
                           "', timestamp: '" +
                           timestamp.toString() +
                           "', filename: '" +
                           filename +
                           "'.");
      }

      rs.close();

      System.out.println("===========================================");

      return true;
    } catch (SQLException e) {
      System.out.println("Exception: '" + e.toString() + "'.");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          System.out.println("Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: printVisitedHosts
  // Description: displays the table of visited hosts.
  // Parameters: none.
  // Returns: true: the table of visited hosts could be displayed;
  //          false: otherwise.
  private boolean printVisitedHosts()
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT * from " + VISITED_HOSTS);
      ResultSet rs = statement.executeQuery();

      System.out.println("Visited hosts:");
      System.out.println("===========================================");

      int count = 0;

      while (rs.next()) {
        count++;

        String host = rs.getString("HOST");
        Timestamp timestamp = rs.getTimestamp("TIMESTAMP");
        String server = rs.getString("SERVER");

        System.out.println("[" +
                           count +
                           "] Host: '" +
                           host +
                           "', timestamp: '" +
                           timestamp.toString() +
                           "', server: '" +
                           server +
                           "'.");
      }

      rs.close();

      System.out.println("===========================================");

      return true;
    } catch (SQLException e) {
      System.out.println("Exception: '" + e.toString() + "'.");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          System.out.println("Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: printUrlsToVisit
  // Description: displays the table of URLs to visit.
  // Parameters: none.
  // Returns: true: the table of URLs to visit could be displayed;
  //          false: otherwise.
  private boolean printUrlsToVisit()
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT * from " + URLS_TO_VISIT);
      ResultSet rs = statement.executeQuery();

      System.out.println("URLs to visit:");
      System.out.println("===========================================");

      int count = 0;

      while (rs.next()) {
        count++;

        String url = rs.getString("URL");
        String host = rs.getString("HOST");
        Timestamp when = rs.getTimestamp("WHEN");

        System.out.println("[" +
                           count +
                           "] URL: '" +
                           url +
                           "', host: '" +
                           host +
                           "', when: '" +
                           when.toString() +
                           "'.");
      }

      rs.close();

      System.out.println("===========================================");

      return true;
    } catch (SQLException e) {
      System.out.println("Exception: '" + e.toString() + "'.");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          System.out.println("Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: createTable
  // Description: creates the table "table" using the SQL command "sql".
  // Parameters:
  //   - table: name of the table to be created.
  //   - sql: SQL command to create the table.
  //
  // Returns: true: the table could be created or already exists;
  //          false: otherwise.
  private boolean createTable(String table, String sql)
  {
    PreparedStatement statement = null;

    try {
      // Create table.
      statement = conn.prepareStatement(sql);
      statement.executeUpdate();

      log.log(Level.INFO, "Created table " + table + ".");
      return true;
    } catch (SQLException e) {
      if (e.getSQLState().equals("X0Y32")) {
        // <value> '<value>' already exists in <value> '<value>'.
        log.log(Level.INFO, "Table " + table + " already exists.");
        return true;
      }

      log.log(Level.WARNING, "Error creating table (" + e.toString() + ").");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: addVisitedHost
  // Description: adds a host to the table of visited hosts. If the host is
  //              already in the table of visited hosts, calls the method
  //              updateVisitedHost() to update the host.
  //
  // Parameters:
  //   - host: name of the host to be added.
  //   - timestamp: timestamp when the host was visited.
  //   - server: HTTP header "Server".
  //
  // Returns: true: the host could be added or updated; false: otherwise.
  private boolean addVisitedHost(String host,
                                 Timestamp timestamp,
                                 String server)
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("INSERT INTO " +
                                        VISITED_HOSTS +
                                        " (HOST, TIMESTAMP, SERVER) " +
                                        "VALUES (?, ?, ?)");

      statement.setString(1, host);
      statement.setTimestamp(2, timestamp);
      statement.setString(3, server);

      statement.executeUpdate();

      log.log(Level.FINEST,
              "Added visited host '" +
              host +
              "', timestamp: '" +
              timestamp.toString() +
              "', server: '" +
              server +
              "'.");

      return true;
    } catch (SQLException e) {
      if (e.getSQLState().equals("23505")) {
        // The statement was aborted because it would have caused a
        // duplicate key value in a unique or primary key constraint or
        // unique index identified by '<value>' defined on '<value>'.
        return updateVisitedHost(host, timestamp, server);
      }

      log.log(Level.WARNING,
              "Error adding visited host (" + e.toString() + ").");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: updateVisitedHost
  // Description: updates a host from the table of visited hosts.
  // Parameters:
  //   - host: name of the host to be updated.
  //   - timestamp: timestamp when the host was visited.
  //   - server: HTTP header "Server".
  //
  // Returns: true: the host could be updated; false: otherwise.
  private boolean updateVisitedHost(String host,
                                    Timestamp timestamp,
                                    String server)
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("UPDATE " +
                                        VISITED_HOSTS +
                                        " SET TIMESTAMP = ?, SERVER = ? " +
                                        "WHERE HOST = ?");

      statement.setTimestamp(1, timestamp);
      statement.setString(2, server);
      statement.setString(3, host);

      statement.executeUpdate();

      log.log(Level.FINEST,
              "Updated visited host '" +
              host +
              "', timestamp: '" +
              timestamp.toString() +
              "', server: '" +
              server +
              "'.");

      return true;
    } catch (SQLException e) {
      log.log(Level.WARNING,
              "Error updating visited host (" + e.toString() + ").");
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }

    return false;
  }


  // Method: urlVisited
  // Description: checks whether the URL "url" has been already visited.
  // Parameters:
  //   - url: URL to check.
  //
  // Returns: true: the URL "url" has been already visited; false: otherwise.
  public boolean urlVisited(String url) throws SQLException
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT 1 FROM " +
                                        VISITED_URLS +
                                        " WHERE URL = ?");

      statement.setString(1, url);

      ResultSet rs = statement.executeQuery();

      boolean ret = rs.next();

      rs.close();

      return ret;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }
  }


  // Method: haveUrlToVisit
  // Description: checks whether the URL "url" is already in the table of URLs
  //              to visit.
  //
  // Parameters:
  //   - url: URL to check.
  //
  // Returns: true: the URL "url" is already in the table of URLs to visit;
  //          false: otherwise.
  public boolean haveUrlToVisit(String url) throws SQLException
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT 1 FROM " +
                                        URLS_TO_VISIT +
                                        " WHERE URL = ?");

      statement.setString(1, url);

      ResultSet rs = statement.executeQuery();

      boolean ret = rs.next();

      rs.close();

      return ret;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }
  }


  // Method: getLatestWhen
  // Description: retrieves from the table of URLs to visit the latest "when"
  //              for the host "host".
  //
  // Parameters:
  //   - host: name of the host for which to get the latest "when".
  //
  // Returns: latest "when" for the host "host";
  //          null if the host "host" is not in the table of URLs to visit.
  private Timestamp getLatestWhen(String host) throws SQLException
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT MAX(WHEN) AS WHEN FROM " +
                                        URLS_TO_VISIT +
                                        " WHERE HOST = ?");

      statement.setString(1, host);

      ResultSet rs = statement.executeQuery();

      Timestamp timestamp;

      if (rs.next()) {
        timestamp = rs.getTimestamp("WHEN");
      } else {
        timestamp = null;
      }

      rs.close();

      return timestamp;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }
  }


  // Method: getHostTimestamp
  // Description: retrieves from the table of visited hosts the timestamp of the
  //              host "host".
  //
  // Parameters:
  //   - host: name of the host for which to get the timestamp when it was last
  //           visited.
  //
  // Returns: timestamp when the host "host" was last visited; null if the
  //          host "host" has not been visited.
  private Timestamp getHostTimestamp(String host) throws SQLException
  {
    PreparedStatement statement = null;

    try {
      statement = conn.prepareStatement("SELECT TIMESTAMP FROM " +
                                        VISITED_HOSTS +
                                        " WHERE HOST = ?");

      statement.setString(1, host);

      ResultSet rs = statement.executeQuery();

      Timestamp timestamp;

      if (rs.next()) {
        timestamp = rs.getTimestamp("TIMESTAMP");
      } else {
        timestamp = null;
      }

      rs.close();

      return timestamp;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
        }
      }
    }
  }


  // Method: help
  // Description: shows the usage.
  // Parameters: none.
  // Returns: nothing.
  private static void help()
  {
    System.out.println("Usage: [OPTIONS] <action> [<arguments>]");
    System.out.println();

    System.out.println("Options:");
    System.out.println("\t--host <host>");
    System.out.println("\t--port <port>");
    System.out.println("\t--database-name <database-name>");
    System.out.println();

    System.out.println("Actions:");
    System.out.println("\t--view-tables");
    System.out.println("\t--view-table-visited-urls");
    System.out.println("\t--view-table-visited-hosts");
    System.out.println("\t--view-table-urls-to-visit");
    System.out.println("\t--drop-tables");
    System.out.println("\t--drop-table-visited-urls");
    System.out.println("\t--drop-table-visited-hosts");
    System.out.println("\t--drop-table-urls-to-visit");
    System.out.println("\t--add-url-to-visit <URL>");
    System.out.println("\t--remove-url-to-visit <URL>");
    System.out.println();
  }


  // Method: main
  // Description: checks the command-line arguments and performs the desired
  //              action, which can be one of the following:
  //                - View tables.
  //                - View table of visited URLs.
  //                - View table of visited hosts.
  //                - View table of URLs to visit.
  //                - Drop tables.
  //                - Drop tables of visited URLs.
  //                - Drop tables of visited hosts.
  //                - Drop tables of URLs to visit.
  //                - Add URL to visit.
  //                - Remove URL to visit.
  //
  // Parameters: array of command-line arguments.
  // Returns: nothing.
  public static void main(String[] args)
  {
    if (args.length == 0) {
      help();
      return;
    }

    String host = null;
    int port = -1;
    String databaseName = null;

    Action action = Action.NONE;
    URL url = null;

    // Check arguments.
    int i = 0;
    while (i < args.length) {
      if (args[i].equals("--host")) {
        // Last argument or after the action?
        if ((i + 1 == args.length) || (action != Action.NONE)) {
          help();
          return;
        }

        host = args[i + 1];

        i += 2;
      } else if (args[i].equals("--port")) {
        // Last argument or after the action?
        if ((i + 1 == args.length) || (action != Action.NONE)) {
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
        // Last argument or after the action?
        if ((i + 1 == args.length) || (action != Action.NONE)) {
          help();
          return;
        }

        databaseName = args[i + 1];

        i += 2;
      } else if (args[i].equals("--view-tables")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.VIEW_TABLES;
        i++;
      } else if (args[i].equals("--view-table-visited-urls")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.VIEW_TABLE_VISITED_URLS;
        i++;
      } else if (args[i].equals("--view-table-visited-hosts")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.VIEW_TABLE_VISITED_HOSTS;
        i++;
      } else if (args[i].equals("--view-table-urls-to-visit")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.VIEW_TABLE_URLS_TO_VISIT;
        i++;
      } else if (args[i].equals("--drop-tables")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.DROP_TABLES;
        i++;
      } else if (args[i].equals("--drop-table-visited-urls")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.DROP_TABLE_VISITED_URLS;
        i++;
      } else if (args[i].equals("--drop-table-visited-hosts")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.DROP_TABLE_VISITED_HOSTS;
        i++;
      } else if (args[i].equals("--drop-table-urls-to-visit")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        action = Action.DROP_TABLE_URLS_TO_VISIT;
        i++;
      } else if (args[i].equals("--add-url-to-visit")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        try {
          url = new URL(args[i + 1]);
        } catch (MalformedURLException e) {
          System.out.println("Invalid URL '" + args[i + 1] + "'.");
          return;
        }

        action = Action.ADD_URL_TO_VISIT;

        i += 2;
      } else if (args[i].equals("--remove-url-to-visit")) {
        if (action != Action.NONE) {
          System.out.println("Only one action is allowed.");
          return;
        }

        // Last argument?
        if (i + 1 == args.length) {
          help();
          return;
        }

        try {
          url = new URL(args[i + 1]);
        } catch (MalformedURLException e) {
          System.out.println("Invalid URL '" + args[i + 1] + "'.");
          return;
        }

        action = Action.REMOVE_URL_TO_VISIT;

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

    if (databaseName == null) {
      System.out.println("No database name has been specified.");
      return;
    }

    Log log = new Log();
    if (log.initialize(Level.FINEST)) {
      Database db = new Database(log);

      boolean initialized = (host != null) ?
                              db.initialize(host, port, databaseName) :
                              db.initialize(databaseName);

      if (initialized) {
        switch (action) {
          case VIEW_TABLES:
            db.printVisitedUrls();
            db.printVisitedHosts();
            db.printUrlsToVisit();

            break;
          case VIEW_TABLE_VISITED_URLS:
            db.printVisitedUrls();
            break;
          case VIEW_TABLE_VISITED_HOSTS:
            db.printVisitedHosts();
            break;
          case VIEW_TABLE_URLS_TO_VISIT:
            db.printUrlsToVisit();
            break;
          case DROP_TABLES:
            db.dropTable(VISITED_URLS);
            db.dropTable(VISITED_HOSTS);
            db.dropTable(URLS_TO_VISIT);

            break;
          case DROP_TABLE_VISITED_URLS:
            db.dropTable(VISITED_URLS);
            break;
          case DROP_TABLE_VISITED_HOSTS:
            db.dropTable(VISITED_HOSTS);
            break;
          case DROP_TABLE_URLS_TO_VISIT:
            db.dropTable(URLS_TO_VISIT);
            break;
          case ADD_URL_TO_VISIT:
            db.addUrlToVisit(url);
            break;
          case REMOVE_URL_TO_VISIT:
            db.removeUrlToVisit(url);
            break;
        }

        db.shutdown();
      }
    } else {
      System.out.println("Cannot initialize logger.");
    }
  }
}
