import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.MalformedURLException;
import java.net.CookieManager;
import java.net.CookieHandler;
import java.net.CookiePolicy;
import javax.net.ssl.HttpsURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;

public class Downloader {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Constants.                                                           ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private static final int READ_BUFFER_SIZE = 8 * 1024;

  private static final String HTTP_ACCEPT =
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

  private static final String HTTP_ACCEPT_LANGUAGE = "en-US,en;q=0.5";

  private static final int MAX_REDIRECTS = 3;

  private static final String TEMP_FILENAME = "data.bin";

  private static final String FILENAME_FORMAT = "%06d.bin";


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Database database = null;

  private String tempDir = null;
  private String finalDir = null;
  private String httpUserAgent = null;

  private Log log = null;

  private int dataFileCount = 0;


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
  //   - database: database object.
  //   - tempDir: temporary directory where to download the files.
  //   - finalDir: final directory where to save the downloaded files.
  //   - httpUserAgent: user agent to be used in the HTTP requests.
  //   - log: logger object.
  //
  // Returns: nothing.
  public Downloader(Database database,
                    String tempDir,
                    String finalDir,
                    String httpUserAgent,
                    Log log)
  {
    this.database = database;
    this.tempDir = tempDir;
    this.finalDir = finalDir;
    this.httpUserAgent = httpUserAgent;
    this.log = log;
  }


  // Method: initialize
  // Description: initializes the downloader:
  //                - Creates the temporary directory.
  //                - Creates the final directory.
  //                - Sets the default cookie manager.
  //
  // Parameters: none.
  // Returns: true: the downloader could be initialized; false: otherwise.
  public boolean initialize()
  {
    // Create temporary directory.
    try {
      Files.createDirectories(Paths.get(tempDir));
    } catch (IOException e) {
      log.log(Level.SEVERE,
              "Cannot create temporary directory '" +
              tempDir +
              "' (" +
              e.toString() +
              ").");

      return false;
    }

    log.log(Level.INFO, "Created temporary directory '" + tempDir + "'.");

    // Create final directory.
    try {
      Files.createDirectories(Paths.get(finalDir));
    } catch (IOException e) {
      log.log(Level.SEVERE,
              "Cannot create final directory '" +
              finalDir +
              "' (" +
              e.toString() +
              ").");

      return false;
    }

    log.log(Level.INFO, "Created final directory '" + finalDir + "'.");

    // Set the default cookie manager.
    CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));

    return true;
  }


  // Method: getNextDataFilename
  // Description: returns the name of the next data file, checking that there is
  //              no file with such a name.
  //
  // Parameters: none.
  // Returns: name of the next data file.
  private String getNextDataFilename()
  {
    // Find the name of the next data file.
    do {
      String filename = String.format(FILENAME_FORMAT, dataFileCount++);

      // If the file doesn't exist...
      if (!Files.exists(Paths.get(finalDir + "/" + filename))) {
        return filename;
      }
    } while (true);
  }


  // Method: writeFileHeaders
  // Description: writes the HTTP headers to the output stream "out",
  //              checks the value of the HTTP header "Content-Type" and, if it
  //              is "text/html", sets the output parameter "process" to true,
  //              it also extracts the value of the HTTP header "Server".
  //
  // Parameters:
  //   - urlConnection: HTTP connection containing the HTTP headers.
  //   - out: output stream where to write the headers.
  //   - process: set to true if the "Content-Type" is "text/html".
  //   - server: set to the value of the HTTP header "Server".
  //
  // Returns: nothing.
  private void writeFileHeaders(HttpURLConnection urlConnection,
                                OutputStream out,
                                MutableBoolean process,
                                StringBuilder server) throws IOException
  {
    // Compose headers of the file.
    // Format:
    // URL: <URL>
    // *(<message-header>)
    // <empty-line>
    StringBuilder fileHeaders = new StringBuilder();

    // Add first line.
    fileHeaders.append("URL: ").append(urlConnection.getURL()).append("\r\n");

    // Get HTTP headers.
    Map<String, List<String>> httpHeaders = urlConnection.getHeaderFields();

    // Iterate through the response headers.
    for (Map.Entry<String, List<String>> httpHeader :
                                         httpHeaders.entrySet()) {
      String key;
      if ((key = httpHeader.getKey()) != null) {
        List<String> values = httpHeader.getValue();

        // Iterate through the values of the key 'key'.
        String lastValue = null;
        for (String value : values) {
          fileHeaders.append(key).append(": ").append(value).append("\r\n");

          lastValue = value;
        }

        if (lastValue != null) {
          if (key.equals("Content-Type")) {
            process.value = lastValue.startsWith("text/html");

            log.log(Level.FINEST, "Content-Type: " + lastValue);
          } else if (key.equals("Server")) {
            server.append(lastValue);

            log.log(Level.FINEST, "Server: " + lastValue);
          }
        }
      }
    }

    // Add empty line.
    fileHeaders.append("\r\n");

    // Save file headers.
    out.write(fileHeaders.toString().getBytes(StandardCharsets.US_ASCII));

    log.log(Level.FINEST, "Written HTTP headers.");
  }


  // Method: performRequest
  // Description: performs an HTTP request.
  //              If the Status-Code is success (2XX):
  //                - Saves the response in a data file.
  //                - Adds the URL to the table of visited URLs.
  //
  //              If the Status-Code is redirect (3XX):
  //                - Adds the URL to the table of visited URLs.
  //                - If we haven't performed too many redirections:
  //                    - Extracts the "Location" header.
  //                    - If the redirect URL has not been visited and is not
  //                      in the table of URLs to visit:
  //                        - Makes a new request.
  //                    - If the redirect URL has been already visited or is in
  //                      the table of URLs to visit: returns false.
  //                - If we have performed too many redirections: returns false.
  //
  // Parameters:
  //   - urlConnection: HTTP connection.
  //   - numberRedirects: number of redirections.
  //   - process: set to true if the file should be further processed.
  //   - finalFilename: name of the final data file.
  //
  // Returns: true: the request succeeded; false: otherwise.
  private boolean performRequest(HttpURLConnection urlConnection,
                                 int numberRedirects,
                                 MutableBoolean process,
                                 StringBuilder finalFilename)
  {
    String tempFilename = null;
    OutputStream out = null;

    try {
      // Set request's HTTP headers.
      urlConnection.setRequestProperty("User-Agent", httpUserAgent);
      urlConnection.setRequestProperty("Accept", HTTP_ACCEPT);
      urlConnection.setRequestProperty("Accept-Language", HTTP_ACCEPT_LANGUAGE);

      // Get status code.
      int statusCode = urlConnection.getResponseCode();

      log.log(Level.FINEST, "Status-Code: " + statusCode);

      // Success response?
      if ((statusCode >= 200) && (statusCode < 300)) {
        // Get name of the file where to save the response.
        tempFilename = tempDir + "/" + TEMP_FILENAME;

        // Create temporary file for saving the response.
        out = new FileOutputStream(tempFilename);

        StringBuilder server = new StringBuilder();

        // Write file headers.
        writeFileHeaders(urlConnection, out, process, server);

        byte[] buf = new byte[READ_BUFFER_SIZE];
        int len;

        InputStream in = urlConnection.getInputStream();

        // Read response and write it to the temporary file.
        while ((len = in.read(buf)) != -1) {
          out.write(buf, 0, len);
        }

        // Close temporary file.
        out.close();
        out = null;

        // Close connection.
        in.close();

        String dataFilename = getNextDataFilename();
        finalFilename.append(finalDir).append("/").append(dataFilename);

        // Move temporary file to the final directory.
        Files.move(Paths.get(tempFilename),
                   Paths.get(finalFilename.toString()),
                   StandardCopyOption.REPLACE_EXISTING);

        log.log(Level.FINER,
                "mv " +
                tempFilename +
                " -> " +
                finalFilename.toString());

        // Add visited URL.
        database.addVisitedUrl(urlConnection.getURL(),
                               server.toString(),
                               dataFilename);

        return true;
      } else if ((statusCode >= 300) && (statusCode < 400)) {
        // Add visited URL.
        database.addVisitedUrl(urlConnection.getURL(), "-", "-");

        if (++numberRedirects <= MAX_REDIRECTS) {
          // Get Location header.
          String url = urlConnection.getHeaderField("Location");
          if (url != null) {
            try {
              // If the URL has not been already visited...
              if (!database.urlVisited(url)) {
                // If the URL is not in the table of URLs to visit...
                if (!database.haveUrlToVisit(url)) {
                  log.log(Level.FINE, "Redirecting to: '" + url + "'...");

                  return request(url, numberRedirects, process, finalFilename);
                } else {
                  log.log(Level.FINEST,
                          "Redirection '" +
                          url +
                          "' is already in the table of URLs to visit.");
                }
              } else {
                log.log(Level.FINEST,
                        "Redirection '" + url + "' has been already visited.");
              }
            } catch (SQLException e) {
              log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
            }
          }
        } else {
          log.log(Level.WARNING,
                  "Too many redirects (" + numberRedirects + ").");
        }
      }
    } catch (IOException e1) {
      log.log(Level.WARNING, "Exception: '" + e1.toString() + "'.");

      try {
        if (out != null) {
          // Close file.
          out.close();
        }

        if (tempFilename != null) {
          // Remove file.
          Files.delete(Paths.get(tempFilename));
        }
      } catch (IOException e2) {
      }
    }

    return false;
  }


  // Method: request
  // Description: if the URL passed as parameter is valid, calls the other
  //              method request() with a URL object.
  //
  // Parameters:
  //   - urlStr: URL as string.
  //   - numberRedirects: number of redirections.
  //   - process: set to true if the file should be further processed.
  //   - finalFilename: name of the final data file.
  //
  // Returns: true: if the URL is valid and the other method request()
  //          succeeded; false: otherwise.
  public boolean request(String urlStr,
                         int numberRedirects,
                         MutableBoolean process,
                         StringBuilder finalFilename)
  {
    try {
      // Create URI.
      URI uri = new URI(urlStr);

      return request(uri.toURL(), numberRedirects, process, finalFilename);
    } catch (URISyntaxException | MalformedURLException e) {
      log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
    }

    return false;
  }


  // Method: request
  // Description: checks the URL passed as parameter and, if the protocol
  //              is either HTTP or HTTPS, calls the method performRequest()
  //              to perform an HTTP request.
  //
  // Parameters:
  //   - url: URL.
  //   - numberRedirects: number of redirections.
  //   - process: set to true if the file should be further processed.
  //   - finalFilename: name of the final data file.
  //
  // Returns: true: the protocol is HTTP or HTTPS and the request succeeded;
  //          false: otherwise.
  public boolean request(URL url,
                         int numberRedirects,
                         MutableBoolean process,
                         StringBuilder finalFilename)
  {
    log.log(Level.INFO, "Request: '" + url.toString() + "'.");

    try {
      if (url.getProtocol().equals("http")) {
        return performRequest((HttpURLConnection) url.openConnection(),
                              numberRedirects,
                              process,
                              finalFilename);
      } else if (url.getProtocol().equals("https")) {
        return performRequest((HttpsURLConnection) url.openConnection(),
                              numberRedirects,
                              process,
                              finalFilename);
      } else {
        log.log(Level.INFO, "Unknown protocol '" + url.getProtocol() + "'.");
      }
    } catch (IOException e) {
      log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
    }

    return false;
  }
}
