import java.io.Serializable;

public class DictionaryPacket implements Serializable {

	/* define packet formats */
	 public static final int DICT_REQUEST   = 0;
	 public static final int DICT_REPLY		= 1;

	 public static final int DICT_NULL		= 2;
	 
	/* initialized to be a null packet */
	public int type = DICT_NULL;
	
	/* content (mandatory for REPLY) */
	public String content;
	
}
