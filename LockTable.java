package concurrency;
import java.util.ArrayList;
import java.util.Iterator;

public class LockTable {
	char ItemName;
	String LockMode;
	ArrayList<Integer> LockedBy;
	ArrayList<Integer> WaitList;
	
	public LockTable(char ItemName)
	{
		this.ItemName = ItemName;
		this.LockMode = "NL";
		this.LockedBy = new ArrayList<Integer>();
		this.WaitList = new ArrayList<Integer>();		
		
	}
	
	public void addholdTransaction(Integer transId)
	{
		this.LockedBy.add(transId);
	}
	
	public void addWaitList(Integer transId)
	{
		this.WaitList.add(transId);
	}
	
	public String toString()
	{
		StringBuffer obj = new StringBuffer();
		obj.append("{");
		obj.append("ItemName:" +this.ItemName);
		obj.append("LockMode: " +this.LockMode);
		obj.append("LockedBy:");
		Iterator ip = this.LockedBy.iterator();
		while(ip.hasNext())
		{
			obj.append(ip.next());
			obj.append(",");
		}
		obj.append("WaitList:");
		Iterator pp = this.WaitList.iterator();
		while(pp.hasNext())
		{
			obj.append(pp.next());
			obj.append(",");
		}
		obj.append("}");
		return obj.toString();
		
		
	}
	

}
