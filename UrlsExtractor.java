import java.util.logging.Level;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URI;
import java.net.URL;
import javax.swing.text.html.parser.ParserDelegator;

class UrlsExtractor {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Constants.                                                           ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Database database = null;

  private Log log = null;


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
  //   - log: logger object.
  //
  // Returns: nothing.
  public UrlsExtractor(Database database, Log log)
  {
    this.database = database;
    this.log = log;
  }


  // Method: initialize
  // Description: nothing to initialize.
  // Parameters: none.
  // Returns: true.
  public boolean initialize()
  {
    return true;
  }


  // Method: processBody
  // Description: parses the body of the data file, which is HTML, to extract
  //              the URLs.
  //
  // Parameters:
  //   - reader: reader to read the body.
  //   - url: context URL.
  //
  // Returns: true: the body could be parsed; false: otherwise.
  private boolean processBody(Reader reader, URL url)
  {
    try {
      HtmlParser htmlParser = new HtmlParser(database, url, log);
      ParserDelegator parserDelegator = new ParserDelegator();

      parserDelegator.parse(reader, htmlParser, true);

      return true;
    } catch (IOException e) {
      log.log(Level.WARNING,
              "Exception while parsing HTML: '" + e.toString() + "'.");
    }

    return false;
  }


  // Method: processFile
  // Description: processes a file:
  //                - Opens the file "filename".
  //                - Extracts the context URL from the first line.
  //                - Skips the HTTP headers.
  //                - Calls the method processBody() to extract the URLs.
  //
  // Parameters:
  //   - filename: name of the file to be processed.
  //
  // Returns: true: the file could be processed; false: otherwise.
  public boolean processFile(String filename)
  {
    log.log(Level.FINEST, "Processing file '" + filename + "'...");

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename),
                                                         DEFAULT_CHARSET)) {
      // Read first line:
      // Format:
      // URL: <url>
      String line;
      if (((line = reader.readLine()) != null) && (!line.isEmpty())) {
        if (line.startsWith("URL:")) {
          try {
            // Create URI.
            URI uri = new URI(line.substring(4).trim());

            // Convert URI to URL.
            URL url = uri.toURL();

            // Skip headers.
            while (((line = reader.readLine()) != null) && (!line.isEmpty()));

            boolean ret = processBody(reader, url);

            reader.close();

            log.log(Level.FINEST,
                    "Finished processing file '" + filename + "'.");

            return true;
          } catch (Exception e) {
            log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");

            reader.close();
          }
        }
      }
    } catch (IOException e) {
      log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
    }

    log.log(Level.WARNING, "Error processing file '" + filename + "'.");

    return false;
  }
}
