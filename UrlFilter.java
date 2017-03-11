import java.util.logging.Level;

public class UrlFilter {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private UrlMatcher exclude = null;
  private UrlMatcher include = null;

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
  public UrlFilter(Log log)
  {
    this.log = log;
  }


  // Method: initialize
  // Description: creates and initializes the URL matchers "exclude" and
  //              "include".
  //
  // Parameters: none.
  // Returns: true: the URL matchers could be initialized; false: otherwise.
  public boolean initialize()
  {
    exclude = new UrlMatcher(log);
    if (exclude.initialize()) {
      include = new UrlMatcher(log);

      if (include.initialize()) {
        return true;
      }
    }

    log.log(Level.SEVERE, "Error initializing URL filter.");

    return false;
  }


  // Method: load
  // Description: loads the exclude and include patterns.
  // Parameters:
  //   - excludeFilename: name of the file containing the URLs to be excluded.
  //   - includeFilename: name of the file containing the URLs to be included.
  //
  // Returns: true: the files could be processed; false: otherwise.
  public boolean load(String excludeFilename, String includeFilename)
  {
    if (excludeFilename != null) {
      if (!exclude.load(excludeFilename)) {
        return false;
      }
    }

    if (includeFilename != null) {
      if (!include.load(includeFilename)) {
        return false;
      }
    }

    return true;
  }


  // Method: matches
  // Description: checks whether the URL "urlStr" matches one of the
  //              exclude or include patterns.
  //
  // Parameters:
  //   - urlStr: URL to be checked.
  //
  // Returns: true: the URL doesn't match the exclude patterns and either
  //                matches one of the include patterns or there are no
  //                include patterns.
  //          false: otherwise.
  public boolean matches(String urlStr)
  {
    if ((!exclude.matches(urlStr)) &&
        ((include.matches(urlStr)) ||
         (include.isEmpty()))) {
      log.log(Level.FINEST, "URL '" + urlStr + "' matches the URL filter.");

      return true;
    } else {
      log.log(Level.FINEST,
              "URL '" + urlStr + "' doesn't match the URL filter.");

      return false;
    }
  }
}
