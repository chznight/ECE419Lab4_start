import java.io.Serializable;

public class JobPacket implements Serializable {

	/* define packet formats */

	 /* sent by client driver, received by job tracker */
	 public static final int JOB_SUBMISSION   = 0;
	 public static final int JOB_QUERY		= 1;

	 /* sent by job tracker, received by client driver */
	 public static final int JOB_RECEIVED	= 2;
	 public static final int JOB_CALCULATING	= 3;
	 public static final int JOB_FOUND		= 4;
	 public static final int JOB_NOTFOUND	= 5;

	 public static final int JOB_NULL		= 6;
	 
	/* initialized to be a null packet */
	public int type = JOB_NULL;
	
	/* content (mandatory for SUBMISSION, QUERY, FOUND) */
	public String content;
	
}
