package net.pincette.letterbox;

import static java.util.logging.Logger.getLogger;
import static net.pincette.config.Util.configValue;

import com.typesafe.config.Config;
import java.util.logging.Logger;

class Common {
  static final Logger LOGGER = getLogger("net.pincette.letterbox");
  private static final String NAMESPACE = "namespace";
  static final String LETTER_BOX = "letter-box";
  static final String VERSION = "1.1.0";

  private Common() {}

  static String namespace(final Config config) {
    return configValue(config::getString, NAMESPACE).orElse(LETTER_BOX);
  }
}
