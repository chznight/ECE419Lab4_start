import java.io.Serializable;
import java.util.List;
import java.util.*;

public class DictionaryPacket implements Serializable {

	/* define packet formats */
	 public static final int DICT_REQUEST   = 0;
	 public static final int DICT_REPLY		= 1;

	 public static final int DICT_NULL		= 2;
	 
	/* initialized to be a null packet */
	public int type = DICT_NULL;
	
	/* content (index is for REQUEST, content is for REPLY) */
	public ArrayList<String> content;
	public int index;
	
}
