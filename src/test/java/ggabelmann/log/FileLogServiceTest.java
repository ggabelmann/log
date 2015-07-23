package ggabelmann.log;

import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Greg Gabelmann
 */
public class FileLogServiceTest {
	
	public FileLogServiceTest() {
	}
	
	@BeforeClass
	public static void setUpClass() {
	}
	
	@AfterClass
	public static void tearDownClass() {
	}
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
	}

	private FileLogService createTempFileAndAwaitRunning() throws IOException {
		final FileLogService fls = new FileLogService(Files.createTempFile(null, null));
		fls.startAsync();
		fls.awaitRunning();
		return fls;
	}

	@Test
	public void testGetNextId() throws IOException {
		final Path missingFile = Paths.get(Long.toString(System.currentTimeMillis()));
		final FileLogService fls = new FileLogService(missingFile);
		fls.startAsync();
		fls.awaitRunning();
		assertEquals(0, fls.getNextId());
		assertTrue(Files.notExists(missingFile, LinkOption.NOFOLLOW_LINKS));
	}

	@Test
	public void testGetLogEntry() throws IOException {
		final FileLogService fls = createTempFileAndAwaitRunning();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		// 1 byte more than window in FileLogEntry.java
		for (int i = 0; i < (1 + FileLogService.copyToWindow); i++) {
			baos.write(i);
		}
		final byte[] sourceBytes = baos.toByteArray();
		
		fls.log(new ByteArrayInputStream(sourceBytes));
		
		final LogItem le = fls.getLogItem(0);
		baos = new ByteArrayOutputStream();
		le.copyTo(baos);
		assertArrayEquals(sourceBytes, baos.toByteArray());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetLogEntries() throws IOException {
		final FileLogService fls = createTempFileAndAwaitRunning();
		fls.getLogItems(0, 0);	// The values don't matter because this will throw an exception.
	}

	@Test
	public void testLogNoBytesAndCheckFileBytes() throws IOException {
		final FileLogService fls = createTempFileAndAwaitRunning();
		assertEquals(0, fls.log(ByteSource.empty().openStream()));
		assertEquals(1, fls.log(ByteSource.empty().openStream()));
		assertEquals(2, fls.getNextId());
		
		final ByteBuffer expectedBytes = ByteBuffer.allocate(2 * FileLogService.logEntryHeaderLength);
		expectedBytes.put((byte) 0);	// type
		expectedBytes.putInt(0);	// id
		expectedBytes.put(ByteSource.empty().hash(Hashing.sha256()).asBytes());	// sha256
		expectedBytes.putInt(0);	// length
		expectedBytes.put((byte) 0);	// type
		expectedBytes.putInt(1);	// id
		expectedBytes.put(ByteSource.empty().hash(Hashing.sha256()).asBytes());	// sha256
		expectedBytes.putInt(0);	// length
		
		final byte[] logBytes = Files.readAllBytes(fls.getPath());
		assertArrayEquals(expectedBytes.array(), logBytes);
	}

	@Test
	public void testScanLog() throws IOException {
		FileLogService fls = createTempFileAndAwaitRunning();
		assertEquals(0, fls.log(new ByteArrayInputStream(new byte[] {(byte) 0})));
		assertEquals(1, fls.log(new ByteArrayInputStream(new byte[] {(byte) 1})));
		
		final Path p = fls.getPath();
		fls.stopAsync();
		fls.awaitTerminated();
		
		fls = new FileLogService(p);
		fls.startAsync();
		fls.awaitRunning();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		fls.getLogItem(0).copyTo(baos);
		assertArrayEquals(new byte[] {(byte) 0}, baos.toByteArray());
		
		baos = new ByteArrayOutputStream();
		fls.getLogItem(1).copyTo(baos);
		assertArrayEquals(new byte[] {(byte) 1}, baos.toByteArray());
	}

	@Test
	public void testLogNoBytesWithExpectedId() throws IOException {
		final FileLogService fls = createTempFileAndAwaitRunning();
		assertTrue(fls.log(ByteSource.empty().openStream(), 0));
		assertEquals(1, fls.getNextId());
	}

	public void testShutDown() throws Exception {
		System.out.println("shutDown");
		FileLogService instance = null;
		instance.shutDown();
		fail("The test case is a prototype.");
	}

	public void testStartUp() throws Exception {
		System.out.println("startUp");
		FileLogService instance = null;
		instance.startUp();
		fail("The test case is a prototype.");
	}
	
}
