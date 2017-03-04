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
  //   - contextUrl: context URL.
  //   - log: logger object.
  //
  // Returns: nothing.
  public HtmlParser(Database database, URL contextUrl, Log log)
  {
    this.database = database;
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
  // Description: adds the URL to the table of URLs to be visited.
  // Parameters:
  //   - urlStr: URL to be added.
  //
  // Returns: true: the URL is valid and could be added to the database
  //                (if not already done); false: otherwise.
  private boolean addUrl(String urlStr)
  {
    try {
      URL url = new URL(contextUrl, urlStr);

      return database.addUrlToVisit(url);
    } catch (IOException e) {
      log.log(Level.WARNING, "Exception: '" + e.toString() + "'.");
    }

    return false;
  }
}
