package ggabelmann.log;

import java.io.OutputStream;

/**
 * Interface for an item stored in a log.
 * 
 * @author Greg Gabelmann
 */
public interface LogItem {
	
	/**
	 * Copies the data of this LogItem to the given OutputStream.
	 * 
	 * @param os Cannot be null.
	 */
	public void copyTo(final OutputStream os);
	
	/**
	 * The ID of this log item.
	 * 
	 * @return The ID.
	 */
	public int getId();
	
}
