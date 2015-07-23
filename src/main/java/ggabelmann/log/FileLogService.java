package ggabelmann.log;

import com.google.common.collect.AbstractIterator;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractIdleService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Implements the Log interface as well as the Guava Service interface.
 * This class uses a ReentrantReadWriteLock to control access to the file-based log.
 * 
 * @author Greg Gabelmann
 */
public class FileLogService extends AbstractIdleService implements Log {
	
	private final List<LogEntryMetadata> metadata;
	private int nextId;
	private final Path path;
	private final ReadWriteLock rwl;
	final static int logEntryHeaderLength = 1 + 4 + 32 + 4;
	final static int copyToWindow = 64 * 1024;
	
	/**
	 * Constructs a new FileLogService, but does not initialize a log at the given path or "start" itself.
	 * 
	 * @param path Cannot be null.
	 */
	public FileLogService(final Path path) {
		this.metadata = new ArrayList<>();
		this.nextId = 0;
		this.path = path;
		this.rwl = new ReentrantReadWriteLock();
	}
	
	private void ensureIsRunning() {
		if (isRunning() == false) throw new IllegalStateException();
	}
	
	/**
	 * This method does not grab any locks, so many calls can be done (in parallel or not) and references to the LogItems held.
	 * 
	 * @see FileLogItem#copyTo(java.io.OutputStream) For info on locking.
	 * @throws IllegalStateException If it is not running.
	 */
	@Override
	public LogItem getLogItem(final int id) {
		ensureIsRunning();
		if (id >= 0 && id < metadata.size()) {
			final LogEntryMetadata entry = metadata.get(id);
			return new FileLogItem(entry);
		}
		else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * @throws UnsupportedOperationException Always.
	 */
	public Stream<LogItem> getLogItems(final int startId, final int count) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	/**
	 * An empty log will return 0.
	 * 
	 * @throws IllegalStateException If it is not running.
	 */
	@Override
	public int getNextId() {
		ensureIsRunning();
		return nextId;
	}
	
	/**
	 * Helpful for testing.
	 * 
	 * @return The log's Path (which may not exist yet).
	 */
	Path getPath() {
		return path;
	}
	
	private int getThenIncrementNextId() {
		return nextId++;
	}
	
	/**
	 * Grabs the write lock when it becomes available to perform the log (append).
	 * This prevents all other reads and writes.
	 * 
	 * @param is Not buffered. Cannot be null.
	 * @throws IllegalStateException If it is not running.
	 */
	@Override
	public int log(final InputStream is) {
		ensureIsRunning();
		rwl.writeLock().lock();
		try {
			return logHelper(is);
		}
		catch (final IOException ex1) {
			throw new UncheckedIOException(ex1);
		}
		finally {
			rwl.writeLock().unlock();
		}
	}
	
	private int logHelper(final InputStream is) throws IOException {
		final byte[] bytes = ByteStreams.toByteArray(is);
		final ByteBuffer bb = ByteBuffer.allocate(logEntryHeaderLength);

		bb.put((byte) 0);	// type
		bb.putInt(getNextId());	// id
		bb.put(ByteSource.wrap(bytes).hash(Hashing.sha256()).asBytes());	// sha256
		bb.putInt(bytes.length);	// length
		
		try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC)) {
			ByteStreams.copy(new ByteArrayInputStream(bb.array()), os);
			ByteStreams.copy(new ByteArrayInputStream(bytes), os);
		}
		
		metadata.add(new LogEntryMetadata(bytes.length, Files.size(path) - bytes.length, getNextId()));
		return getThenIncrementNextId();
	}
	
	/**
	 * @see log(InputStream)
	 */
	@Override
	public boolean log(final InputStream is, final int expectedId) {
		ensureIsRunning();
		rwl.writeLock().lock();
		try {
			if (expectedId == getNextId()) {
				// This should always be true unless there is a Bug.
				logHelper(is);
				return true;
			}
			else {
				return false;
			}
		}
		catch (final IOException ex1) {
			throw new UncheckedIOException(ex1);
		}
		finally {
			rwl.writeLock().unlock();
		}
	}
	
	/**
	 * Waits for all reads/writes to finish before grabbing the write lock.
	 * The lock is never released which prevents further activity on this object.
	 * This is per the Guava Service javadoc.
	 */
	@Override
	protected void shutDown() {
		rwl.writeLock().lock();
	}
	
	/**
	 * Grabs the write lock to help prevent any activity (although its state won't be "running" until this method exits).
	 * If the log exists it is replayed to find any errors and record locations of data within the file.
	 * If it does not exist then nothing happens.
	 */
	@Override
	protected void startUp() {
		rwl.writeLock().lock();
		try {
			if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
				try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ)) {
					final FileLogIterator fli = new FileLogIterator(is);
					fli.forEachRemaining((final LogEntryMetadata lemd) -> {
						if (lemd.id == metadata.size()) {
							metadata.add(lemd);
						}
						else {
							throw new IllegalStateException("id does not match position within list.");
						}
					});
				}
				catch (final IOException ex1) {
					throw new UncheckedIOException(ex1);
				}
			}
			else {
				// Nothing to do if the path does not exist.
			}
		}
		finally {
			rwl.writeLock().unlock();
		}
	}
	
	
	// ===
	
	
	/**
	 * Records information about the locations of data within the log's file.
	 * This helps with random reads.
	 */
	private static class LogEntryMetadata {
		
		private final int dataLength;
		private final long dataStart;
		private final int id;
		
		private LogEntryMetadata(final int dataLength, final long dataStart, final int id) {
			this.dataLength = dataLength;
			this.dataStart = dataStart;
			this.id = id;
		}
	}
	
	/**
	 * Implementation of a LogItem.
	 */
	private class FileLogItem implements LogItem {
		
		private final LogEntryMetadata lemd;
		
		private FileLogItem(final LogEntryMetadata lemd) {
			this.lemd = lemd;
		}
		
		/**
		 * The read lock is grabbed prior to reading.
		 * This prevents all other writes to the log from occurring, but multiple reads may occur if the OS/platform allows it.
		 * The given OutputStream is not buffered or closed.
		 */
		@Override
		public void copyTo(final OutputStream os) {
			// Necessary to grab read lock before creating RandomAccessFile.
			// Grabbing the lock outside of try/finally is fine.
			rwl.readLock().lock();
			try (final RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
				int left = lemd.dataLength;
				int read = 0;
				byte[] buffer = new byte[copyToWindow];
				raf.seek(lemd.dataStart);
				
				while ((left > 0) && ((read = raf.read(buffer, 0, Math.min(left, buffer.length))) >= 0)) {
					os.write(buffer, 0, read);
					left -= read;
				}
			}
			catch (final IOException ex1) {
				throw new UncheckedIOException(ex1);
			}
			finally {
				rwl.readLock().unlock();
			}
		}
		
		@Override
		public int getId() {
			return lemd.id;
		}
	}
	
	/**
	 * An Iterator for the contents of the log.
	 * As it iterates over the log it automatically checks the data's integrity.
	 * It does not check that IDs increment properly.
	 * Right now the caller has to do that (could change in the future).
	 */
	private class FileLogIterator extends AbstractIterator<LogEntryMetadata> {

		private final InputStream is;
		private int pos;

		private FileLogIterator(final InputStream is) {
			this.is = is;
			this.pos = 0;
		}

		@Override
		protected LogEntryMetadata computeNext() {
			try {
				byte[] buffer = new byte[logEntryHeaderLength];
				int read = 0;
				do {
					read = is.read(buffer);
				} while (read == 0);	// Ensure we've either hit EOF or else read at least one byte.

				if (read == -1) {
					return endOfData();
				}
				else {
					// We've read at least one byte by this point.
					// Check the type is correct, otherwise the header size will be wrong.
					if (buffer[0] != (byte) 0) throw new IllegalStateException("type is not 0.");

					// Read the whole header.
					ByteStreams.readFully(is, buffer, read /* offset */, buffer.length - read);
					pos += buffer.length;
					return deserializeAndCheck(buffer);
				}
			}
			catch (final IOException ex1) {
				throw new UncheckedIOException(ex1);
			}
		}
		
		private LogEntryMetadata deserializeAndCheck(final byte[] buffer) {
			final ByteBuffer bb = ByteBuffer.wrap(buffer);
			
			bb.get();	// Skip over the type because it was checked by the caller.
			final int id = bb.getInt();
			final byte[] sha256 = new byte[32];
			bb.get(sha256);
			final int length = bb.getInt();
			
			final byte[] actualSha256 =
				Hashing
					.sha256()
					.hashObject(ByteStreams.limit(is, length), new InputStreamFunnel())
					.asBytes();
			pos += length;
			if (Arrays.equals(sha256, actualSha256) == false) throw new IllegalStateException("sha256 does not match.");

			return new LogEntryMetadata(length, pos - length, id);
		}
	}
	
	/**
	 * Simple helper class.
	 */
	private static class InputStreamFunnel implements Funnel<InputStream> {

		@Override
		public void funnel(final InputStream from, final PrimitiveSink into) {
			try {
				ByteStreams.copy(from, Funnels.asOutputStream(into));
			}
			catch (final IOException ex1) {
				throw new UncheckedIOException(ex1);
			}
		}
	}
	
}
