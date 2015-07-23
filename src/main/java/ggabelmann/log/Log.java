package ggabelmann.log;

import java.io.InputStream;

/**
 * Interface for a log, which is an append-only sequence of items that each have a unique ID.
 * This interface never considers the log to be closed, unavailable, full, contain a corrupted item, etc.
 * Implementations will describe how they handle those situations.
 * 
 * @author Greg Gabelmann
 */
public interface Log {
	
	/**
	 * Returns an item from the log that has the given ID.
	 * 
	 * @param id The ID of the item.
	 * @return The item if it exists.
	 * @throws IllegalArgumentException If the given ID is less than 0 or the item does not exist.
	 */
	public LogItem getLogItem(final int id);
	
	/**
	 * Returns the ID of the next item that will be created.
	 * For example, an empty log could return 0.
	 * 
	 * @return The next ID.
	 */
	public int getNextId();
	
	/**
	 * Logs the data from the given InputStream.
	 * 
	 * @param is Cannot be null.
	 * @return The ID of the new log item.
	 */
	public int log(final InputStream is);
	
	/**
	 * Logs the data from the given InputStream, but only if the new log item will have an ID equal to expectedId.
	 * This is useful for "optimistic writes".
	 * 
	 * @param is Cannot be null.
	 * @param expectedId Cannot be less than zero.
	 * @return True if the new log item has an ID equal to expectedId, otherwise false if no item was created.
	 */
	public boolean log(final InputStream is, final int expectedId);
	
}
