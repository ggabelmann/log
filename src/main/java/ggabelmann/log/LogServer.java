package ggabelmann.log;

import com.google.common.io.ByteStreams;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Headers;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Allows a FileLogService to be accessed with standard REST calls.
 * When a log is created with a POST it is backed by a temporary file.
 * When the server is restarted the log still exists (in the filesystem) but the mapping is gone.
 * This is temporary behavior.
 * 
 * /logs
 * POST - the name of the log to create
 * 
 * /logs/{log}/items
 * GET - the next ID to be created
 * POST - the next item to append to the log
 * 
 * /logs/{log}/items/{id}
 * GET - the data for the item
 * PUT - an "optimistic append"
 * 
 * @author Greg Gabelmann
 */
public class LogServer {
	
	private final Map<String, FileLogService> fileLogServices;
	
	public static void main(final String[] args) {
		final LogServer server = new LogServer();
		server.start();
	}
	
	public LogServer() {
		this.fileLogServices = new ConcurrentHashMap();
	}
	
	private void start() {
		final Undertow server = Undertow.builder()
			.addHttpListener(8090, "localhost")
			.setHandler(Handlers.pathTemplate()
				.add("/logs/{log}/items/{id}", new HandleId())
				.add("/logs/{log}/items", new HandleItems())
				.add("/logs", new HandleLogs())).build();
		server.start();
	}
	
	
	// ===
	
	
	private class HandleId implements HttpHandler {
		
		@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			final String log = exchange.getAttachment(PathTemplateHandler.PATH_TEMPLATE_MATCH).getParameters().get("log");
			final String id = exchange.getAttachment(PathTemplateHandler.PATH_TEMPLATE_MATCH).getParameters().get("id");

			final FileLogService fls = fileLogServices.get(log);
			if (fls == null) {
				exchange.setResponseCode(404);	// Not Found
			}
			else if (exchange.getRequestMethod().equalToString("GET")) {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				fls.getLogItem(Integer.parseInt(id)).copyTo(baos);
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");	// binary instead?
				exchange.getResponseSender().send(baos.toString());
			}
			else if (exchange.getRequestMethod().equalToString("PUT")) {
				exchange.startBlocking();
				if (fls.log(exchange.getInputStream(), Integer.parseInt(id))) {
					exchange.setResponseCode(201);	// Created
				}
				else {
					// Either the ID is too low or too high.
					exchange.setResponseCode(405);	// Method Not Allowed
				}
			}
			else {
				exchange.setResponseCode(400);	// Bad Request
			}
		}
	}
	
	private class HandleItems implements HttpHandler {
		
		@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			final String log = exchange.getAttachment(PathTemplateHandler.PATH_TEMPLATE_MATCH).getParameters().get("log");

			final FileLogService fls = fileLogServices.get(log);
			if (fls == null) {
				exchange.setResponseCode(404);	// Not Found
			}
			else if (exchange.getRequestMethod().equalToString("GET")) {
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
				exchange.getResponseSender().send("{\"nextId\":" + fls.getNextId() + "}");
			}
			else if (exchange.getRequestMethod().equalToString("POST")) {
				exchange.startBlocking();
				final int id = fls.log(exchange.getInputStream());
				exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
				exchange.setResponseCode(201);	// Created
				exchange.getResponseSender().send("{\"id\":" + id + "}");
			}
			else {
				exchange.setResponseCode(400);	// Bad Request
			}
		}
	}
	
	private class HandleLogs implements HttpHandler {
		
		@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			if (exchange.getRequestMethod().equalToString("POST")) {
				exchange.startBlocking();
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ByteStreams.copy(exchange.getInputStream(), baos);
				final String log = baos.toString();

				synchronized (fileLogServices) {
					FileLogService fls = fileLogServices.get(log);

					if (fls == null) {
						try {
							fls = new FileLogService(Files.createTempFile(null, null));
							fls.startAsync();
							fls.awaitRunning();
							fileLogServices.put(log, fls);
						}
						catch (final IOException ex1) {
							throw new UncheckedIOException(ex1);
						}
						exchange.setResponseCode(201);	// Created
					}
					else {
						exchange.setResponseCode(409);	// Conflict
					}
				}
			}
			else {
				exchange.setResponseCode(405);	// Method Not Allowed
			}
		}
	}
	
}
