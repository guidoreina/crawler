import java.util.logging.Logger;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;
import java.io.IOException;

public class Log {
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Data members.                                                        ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  private Logger logger = null;


  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  ////                                                                      ////
  //// Methods.                                                             ////
  ////                                                                      ////
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  // Method: Constructor
  // Description: creates the logger "Crawler".
  // Parameters: none.
  // Returns: nothing.
  public Log()
  {
    logger = Logger.getLogger("Crawler");
  }


  // Method: initialize
  // Description: initializes the logger as a console logger.
  // Parameters:
  //   - level: level used for logging.
  //
  // Returns: true: the logger could be initialized; false: otherwise.
  public boolean initialize(Level level)
  {
    return initialize(new ConsoleHandler(), level);
  }


  // Method: initialize
  // Description: initializes the logger as a file logger.
  // Parameters:
  //   - filename: name of the log file.
  //   - level: level used for logging.
  //
  // Returns: true: the logger could be initialized; false: otherwise.
  public boolean initialize(String filename, Level level)
  {
    try {
      // Create a file handler where logs will be appended.
      FileHandler handler = new FileHandler(filename, true);

      // Set the simple formatter (the default format is XML).
      handler.setFormatter(new SimpleFormatter());

      return initialize(handler, level);
    } catch (IOException | SecurityException e) {
      System.out.println("Exception: '" + e.toString() + "'.");
    }

    return false;
  }


  // Method: initialize
  // Description: sets the log handler and the log level.
  // Parameters:
  //   - handler: log handler to be set.
  //   - level: level used for logging.
  //
  // Returns: true: the logger could be initialized; false: otherwise.
  private boolean initialize(Handler handler, Level level)
  {
    try {
      handler.setLevel(level);

      logger.setUseParentHandlers(false);

      logger.addHandler(handler);
      logger.setLevel(level);

      return true;
    } catch (SecurityException e) {
      System.out.println("Exception: '" + e.toString() + "'.");
    }

    return false;
  }


  // Method: log
  // Description: generates a log message with level "level" and text "msg".
  // Parameters:
  //   - level: log level.
  //   - msg: log message.
  //
  // Returns: nothing.
  public void log(Level level, String msg)
  {
    logger.log(level, msg);
  }
}
