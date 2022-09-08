package chapter4.jukebox;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Jukebox provides the main music-streaming logic and HTTP server interface for music
 * players to connect to.
 *
 * There are two types of incoming HTTP requests: either a client wants to directly
 * download a file by name, or it wants to join the audio stream. The processing strategies
 * are very different.
 *
 * In the case of downloading a file, the goal is to perform a direct copy from the file
 * read stream to the HTTP response write stream. This will be done with back-pressure
 * management to avoid excessive buffering.
 *
 * Streaming is a bit more involved, as we need to keep track of all the streamers’ HTTP
 * response write streams. A timer periodically reads data from the current MP3 file, and
 * the data is duplicated and written for each streamer.
 *
 * Usage:
 *
 * Download mp3:
 * curl -o out.mp3 http://localhost:8080/download/intro.mp3
 * GET /download/filename
 *
 * Streaming:
 * GET /
 *
 */
public class Jukebox extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger(Jukebox.class);

  // --------------------------------------------------------------------------------- //
  // the state of the Jukebox verticle class is defined by a play status and a playlist.
  // the Vert.x threading model ensures single-threaded access, so there is no need for
  // concurrent collections and critical sections
  private enum State {PLAYING, PAUSED}

  private State currentMode = State.PAUSED;

  private final Queue<String> playlist = new ArrayDeque<>();

  // --------------------------------------------------------------------------------- //
  // Http response objects for streaming requesters - we track all current streamers in a set of
  // HTTP responses
  private final Set<HttpServerResponse> streamers = new HashSet<>();

  // --------------------------------------------------------------------------------- //
  // The start method needs to configure a few event-bus handlers that correspond to the
  // commands and actions that can be used from the TCP text protocol. The NetControl verticle
  // deals with the inners of the TCP server and sends messages to the event bus.
  @Override
  public void start() {
    logger.info("Start");

    // Setting up the event-bus handlers
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("jukebox.list", this::list);
    eventBus.consumer("jukebox.schedule", this::schedule);
    eventBus.consumer("jukebox.play", this::play);
    eventBus.consumer("jukebox.pause", this::pause);

    vertx.createHttpServer()
      .requestHandler(this::httpHandler)
      .listen(8080);

    // Rate control here is all about ensuring that all players receive data fast enough
    // so that they can play without interruption, but not too quickly so that they do
    // not buffer too much data.
    // streamAudioChunk periodically pushes new MP3 data (100 ms is purely empirical, so
    // feel free to adjust it
    vertx.setPeriodic(100, this::streamAudioChunk);
  }

  // --------------------------------------------------------------------------------- //
  // the play/pause and schedule handlers. These methods directly manipulate the play
  // and playlist state
  private void play(Message<?> request) {
    logger.info("Play");
    currentMode = State.PLAYING;
  }

  private void pause(Message<?> request) {
    logger.info("Pause");
    currentMode = State.PAUSED;
  }

  private void schedule(Message<JsonObject> request) {
    String file = request.body().getString("file");
    logger.info("Scheduling {}", file);
    // allows us to automatically resume playing when no track is playing, and we schedule a new one
    if (playlist.isEmpty() && currentMode == State.PAUSED) {
      currentMode = State.PLAYING;
    }
    playlist.offer(file);
  }

  // --------------------------------------------------------------------------------- //
  // list the available files
  private void list(Message<?> request) {
    // asynchronously get all files ending with .mp3 in the tracks/ folder
    vertx.fileSystem().readDir("tracks", ".*mp3$", ar -> {
      if (ar.succeeded()) {
        List<String> files = ar.result()
          .stream()
          .map(File::new)
          .map(File::getName)
          .collect(Collectors.toList());
        JsonObject json = new JsonObject().put("files", new JsonArray(files));
        request.reply(json);
      } else {
        logger.error("readDir failed", ar.cause());
        request.fail(500, ar.cause().getMessage());
      }
    });
  }

  // --------------------------------------------------------------------------------- //
  // forwards HTTP requests to the openAudioStream and download utility methods, which
  // complete the requests and proceed
  private void httpHandler(HttpServerRequest request) {
    logger.info("{} '{}' {}", request.method(), request.path(), request.remoteAddress());
    if ("/".equals(request.path())) {
      openAudioStream(request);
      return;
    }
    if (request.path().startsWith("/download/")) {
      // prevents malicious attempts to read files from other directories
      String sanitizedPath = request.path().substring(10).replaceAll("/", "");
      download(sanitizedPath, request);
      return;
    }
    // When nothing matches, we give a 404 (not found) response
    request.response().setStatusCode(404).end();
  }

  // --------------------------------------------------------------------------------- //
  // prepares the stream to be in chunking mode, sets the proper content type, and sets the
  // response object aside for later
  private void openAudioStream(HttpServerRequest request) {
    logger.info("New streamer");
    HttpServerResponse response = request.response()
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true);
    streamers.add(response);
    response.endHandler(v -> {
      streamers.remove(response);
      logger.info("A streamer left");
    });
  }

  // --------------------------------------------------------------------------------- //

  private void download(String path, HttpServerRequest request) {
    String file = "tracks/" + path;
    // Unless you are on a networked filesystem, the possible blocking time is marginal,
    // so we avoid a nested callback level.
    if (!vertx.fileSystem().existsBlocking(file)) {
      request.response().setStatusCode(404).end();
      return;
    }
    OpenOptions opts = new OpenOptions().setRead(true);
    // we look for the file, and when it exists, we forward the final download duty
    // to the downloadFile method
    vertx.fileSystem().open(file, opts, ar -> {
      if (ar.succeeded()) {
        downloadFile(ar.result(), request);
      } else {
        logger.error("Read failed", ar.cause());
        request.response().setStatusCode(500).end();
      }
    });
  }

  private void downloadFile(AsyncFile file, HttpServerRequest request) {
    HttpServerResponse response = request.response();
    response.setStatusCode(200)
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true);

    file.handler(buffer -> {
      response.write(buffer);
      if (response.writeQueueFull()) { //  Writing too fast!
        file.pause(); // Back-pressure application by pausing the read stream
        response.drainHandler(v -> file.resume()); // Resuming when drained
      }
    });

    file.endHandler(v -> response.end());
  }

  private void downloadFilePipe(AsyncFile file, HttpServerRequest request) {
    HttpServerResponse response = request.response();
    response.setStatusCode(200)
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true);
    file.pipeTo(response);
  }

  // --------------------------------------------------------------------------------- //

  private AsyncFile currentFile;
  private long positionInFile;

  // Reads blocks of, at most, 4096 bytes. Will always be called 10 times per second, so needs
  // to check whether anything is being played at all. The processReadBuffer method streams data
  // to listeners.
  private void streamAudioChunk(long id) {
    if (currentMode == State.PAUSED) {
      return;
    }
    if (currentFile == null && playlist.isEmpty()) {
      currentMode = State.PAUSED;
      return;
    }
    if (currentFile == null) {
      openNextFile();
    }
    // Buffers cannot be reused across I/O operations, so we need a new one
    currentFile.read(Buffer.buffer(4096), 0, positionInFile, 4096, ar -> {
      if (ar.succeeded()) {
        // This is where data is being copied to all players
        processReadBuffer(ar.result());
      } else {
        logger.error("Read failed", ar.cause());
        closeCurrentFile();
      }
    });
  }

  // --------------------------------------------------------------------------------- //

  private void openNextFile() {
    logger.info("Opening {}", playlist.peek());
    OpenOptions opts = new OpenOptions().setRead(true);
    // we use the blocking variant, but it will rarely be an issue for opening a file
    currentFile = vertx.fileSystem()
      .openBlocking("tracks/" + playlist.poll(), opts);
    positionInFile = 0;
  }

  private void closeCurrentFile() {
    logger.info("Closing file");
    positionInFile = 0;
    currentFile.close();
    currentFile = null;
  }

  // --------------------------------------------------------------------------------- //

  private void processReadBuffer(Buffer buffer) {
    logger.info("Read {} bytes from pos {}", buffer.length(), positionInFile);
    positionInFile += buffer.length();
    // This happens when the end of the file has been reached
    if (buffer.length() == 0) {
      closeCurrentFile();
      return;
    }
    for (HttpServerResponse streamer : streamers) {
      // Back-pressure - when the write queue of a client is full, we simply discard data
      if (!streamer.writeQueueFull()) {
        // Remember, buffers cannot be reused
        streamer.write(buffer.copy());
      }
    }
  }
}
