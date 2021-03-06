import java.util.logging.Level;
import java.io.IOException;
import java.net.URL;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.HTML;
import javax.swing.text.MutableAttributeSet;

public class HtmlParser extends HTMLEditorKit.ParserCallback {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Database database = null;
  private UrlFilter urlFilter = null;

  private URL contextUrl = null;

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
  //   - urlFilter: URL filter object.
  //   - contextUrl: context URL object.
  //   - log: logger object.
  //
  // Returns: nothing.
  public HtmlParser(Database database,
                    UrlFilter urlFilter,
                    URL contextUrl,
                    Log log)
  {
    this.database = database;
    this.urlFilter = urlFilter;
    this.contextUrl = contextUrl;
    this.log = log;
  }


  // Method: handleStartTag
  // Description: handles a start tag, it just calls the method handleTag().
  // Parameters:
  //   - t: HTML tag.
  //   - a: set of attributes.
  //   - pos: position of the tag.
  //
  // Returns: nothing.
  public void handleStartTag(HTML.Tag t, MutableAttributeSet a, int pos)
  {
    handleTag(t, a, pos);
  }


  // Method: handleSimpleTag
  // Description: handles a simple tag, it just calls the method handleTag().
  // Parameters:
  //   - t: HTML tag.
  //   - a: set of attributes.
  //   - pos: position of the tag.
  //
  // Returns: nothing.
  public void handleSimpleTag(HTML.Tag t, MutableAttributeSet a, int pos)
  {
    handleTag(t, a, pos);
  }


  // Method: handleTag
  // Description: handles a tag:
  //                - If the tag is "a", calls the method addUrl() with
  //                  the attribute "href" to add the URL to the database.
  //                - If the tag is "img", calls the method addUrl() with
  //                  the attribute "src" to add the URL to the database.
  //
  // Parameters:
  //   - t: HTML tag.
  //   - a: set of attributes.
  //   - pos: position of the tag.
  //
  // Returns: nothing.
  private void handleTag(HTML.Tag t, MutableAttributeSet a, int pos)
  {
    if (HTML.Tag.A.equals(t)) {
      addUrl((String) a.getAttribute(HTML.Attribute.HREF));
    } else if (HTML.Tag.IMG.equals(t)) {
      addUrl((String) a.getAttribute(HTML.Attribute.SRC));
    }
  }


  // Method: addUrl
  // Description: adds the URL to the table of URLs to be visited only if the
  //              URL starts with "http://" or "https://" and matches the URL
  //              filter.
  //
  // Parameters:
  //   - urlStr: URL to be added.
  //
  // Returns: true:
  //            - The scheme is "HTTP" or "HTTPS", the URL matches the URL
  //              filter, the URL is valid and could be added to the database
  //              (if not already done);
  //              or:
  //            - The URL has another scheme.
  //              or:
  //            - The URL doesn't match the URL filter.
  //          false: otherwise.
  private boolean addUrl(String urlStr)
  {
    // HTTP or HTTPS and matches the URL filter?
    if ((urlStr != null) &&
        ((urlStr.regionMatches(true, 0, "http://", 0, 7)) ||
         (urlStr.regionMatches(true, 0, "https://", 0, 8))) &&
        (urlFilter.matches(urlStr))) {
      try {
        URL url = new URL(contextUrl, urlStr);

        return database.addUrlToVisit(url);
      } catch (IOException e) {
        log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
      }

      return false;
    }

    return true;
  }
}
