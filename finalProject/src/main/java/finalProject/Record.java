package finalProject;

import java.io.Serializable;

public class Record implements Serializable {
	
	private String recorddt;
	private String recordtype;
	  
	public String getRecorddt() {
		return recorddt;
	}
	public void setRecorddt(String recorddt) {
		this.recorddt = recorddt;
	}
	public String getRecordtype() {
		return recordtype;
	}
	public void setRecordtype(String recordtype) {
		this.recordtype = recordtype;
	}	 
}
