package chapter4.jukebox;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NetControl provides a text-based TCP protocol for remotely controlling the jukebox
 * application.
 *
 * User Interface:
 *
 * netcat localhost 3000 (or use nc command)
 * /list — Lists the files available for playback
 * /play — Ensures the stream plays
 * /pause — Pauses the stream
 * /schedule file — Appends file at the end of the playlist
 *
 * Each text line can have exactly one command, so the protocol is newline-separated.
 *
 * We concatenate buffers as they arrive, and splits them on newlines, so we have one line
 * per buffer. Instead of manually assembling intermediary buffers, Vert.x offers a handy
 * parsing helper with the RecordParser class. The parser ingests buffers and emits new buffers
 * with parsed data, either by looking for delimiters or by working with chunks of fixed size.
 *
 * The parser is both a read and a write stream, as it functions as an adapter between two
 * streams.
 *
 */
public class NetControl extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger(NetControl.class);

  // --------------------------------------------------------------------------------- //

  @Override
  public void start() {
    logger.info("Start");
    vertx.createNetServer()
      .connectHandler(this::handleClient)
      .listen(3000);
  }

  private void handleClient(NetSocket socket) {
    logger.info("New connection");
    RecordParser.newDelimited("\n", socket) // Parse by looking for new lines
      .handler(buffer -> handleBuffer(socket, buffer)) // Now buffers are lines
      .endHandler(v -> logger.info("Connection ended"));
  }

  // --------------------------------------------------------------------------------- //

  // In the next listing, each buffer is known to be a line, so we can go directly to
  // processing commands
  private void handleBuffer(NetSocket socket, Buffer buffer) {
    String command = buffer.toString(); // Buffer-to-string decoding with the default charset
    switch (command) {
      case "/list":
        listCommand(socket);
        break;
      case "/play":
        vertx.eventBus().send("jukebox.play", "");
        break;
      case "/pause":
        vertx.eventBus().send("jukebox.pause", "");
        break;
      default:
        if (command.startsWith("/schedule ")) {
          schedule(command);
        } else {
          socket.write("Unknown command\n");
        }
    }
  }

  // --------------------------------------------------------------------------------- //

  private void schedule(String command) {
    String track = command.substring(10); // The first 10 characters are for /schedule and a space
    JsonObject json = new JsonObject().put("file", track);
    vertx.eventBus().send("jukebox.schedule", json);
  }

  private void listCommand(NetSocket socket) {
    vertx.eventBus().request("jukebox.list", "", reply -> {
      if (reply.succeeded()) {
        JsonObject data = (JsonObject) reply.result().body();
        data.getJsonArray("files")
          .stream().forEach(name -> socket.write(name + "\n")); // We write each filename to the standard console output
      } else {
        logger.error("/list error", reply.cause());
      }
    });
  }

  // --------------------------------------------------------------------------------- //
}
