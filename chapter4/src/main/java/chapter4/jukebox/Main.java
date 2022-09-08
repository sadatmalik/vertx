package chapter4.jukebox;

import io.vertx.core.Vertx;

/**
 * The idea is that the jukebox has a few MP3 files stored locally, and clients can
 * connect over HTTP to listen to the stream. Individual files can also be downloaded
 * over HTTP. In turn, controlling when to play, pause, and schedule a song happens
 * over a simple, text-based TCP protocol. All connected players will be listening to
 * the same audio at the same time, apart from minor delays due to the buffering put
 * in place by the players.
 */
public class Main {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new Jukebox());
    vertx.deployVerticle(new NetControl());
  }
}
