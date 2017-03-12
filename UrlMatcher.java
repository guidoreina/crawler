import java.util.logging.Level;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UrlMatcher {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Constants.                                                           ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private static final Charset DEFAULT_CHARSET = StandardCharsets.US_ASCII;


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private String name = null;

  private ArrayList<Pattern> patterns = null;

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
  //   - name: name of the URL matcher (either "exclude" or "include").
  //   - log: logger object.
  //
  // Returns: nothing.
  public UrlMatcher(String name, Log log)
  {
    this.name = name;
    this.log = log;
  }


  // Method: initialize
  // Description: creates an empty list of patterns.
  // Parameters: none.
  // Returns: true.
  public boolean initialize()
  {
    patterns = new ArrayList<Pattern>();

    return true;
  }


  // Method: load
  // Description: loads and compiles the patterns from the file "filename".
  // Parameters:
  //   - filename: name of the file to be processed.
  //
  // Returns: true: the file could be processed; false: otherwise.
  public boolean load(String filename)
  {
    log.log(Level.INFO,
            "[" + name + "] Loading patterns file '" + filename + "'...");

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename),
                                                         DEFAULT_CHARSET)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isEmpty()) {
          String patternStr = line.trim();

          if ((!patternStr.isEmpty()) && (patternStr.charAt(0) != '#')) {
            try {
              // Compile pattern.
              Pattern p = Pattern.compile(patternStr);

              // Add pattern to the list of patterns.
              patterns.add(p);

              log.log(Level.FINEST,
                      "[" + name + "] Added pattern '" + patternStr + "'.");
            } catch (PatternSyntaxException e) {
              log.log(Level.WARNING,
                      "[" +
                      name +
                      "] Ignored invalid pattern '" +
                      patternStr +
                      "'.");
            }
          }
        }
      }

      reader.close();

      return true;
    } catch (IOException e) {
      log.log(Level.SEVERE,
              "[" + name + "] Exception: '" + e.toString() + "'.");
    }

    log.log(Level.SEVERE,
            "[" + name + "] Error processing file '" + filename + "'.");

    return false;
  }


  // Method: matches
  // Description: checks whether the URL "urlStr" matches one of the patterns.
  // Parameters:
  //   - urlStr: URL to be checked.
  //
  // Returns: true: the URL matches one of the patterns; false: otherwise.
  public boolean matches(String urlStr)
  {
    if (!patterns.isEmpty()) {
      for (Pattern p: patterns) {
        Matcher m = p.matcher(urlStr);
        if (m.matches()) {
          log.log(Level.FINEST,
                  "[" +
                  name +
                  "] URL '" +
                  urlStr +
                  "' matches pattern '" +
                  p.toString() +
                  "'.");

          return true;
        }
      }

      log.log(Level.FINEST,
              "[" + name + "] No pattern matches URL '" + urlStr + "'.");
    } else {
      log.log(Level.FINEST,
              "[" + name + "] No patterns to match URL '" + urlStr + "'.");
    }

    return false;
  }


  // Method: isEmpty
  // Description: returns whether the list of patterns is empty.
  // Parameters: none.
  // Returns: true: the list of patterns is empty; false: otherwise.
  public boolean isEmpty()
  {
    return patterns.isEmpty();
  }
}
