package concurrency;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class TransactionTable {
	Integer TransId;
	Date TimeStamp;
	String TransactionState;
	ArrayList<Character> ItemsLocked;
	ArrayList<String> OperationsList;
	
	public TransactionTable(Integer TransId, String TransactionState)
	{
		this.TransId = TransId;
		this.TimeStamp = new Date();
		this.TransactionState = TransactionState;
		this.ItemsLocked = new ArrayList<Character>();
		this.OperationsList = new ArrayList<String>();
		
	}
	
	public void appendOperation(String string)
	{
		this.OperationsList.add(string);
	}
	
	public void addHoldObject(char itemName)
	{
		this.ItemsLocked.add(itemName);
	}
	
	public String toString()
	{
		StringBuffer obj = new StringBuffer();
		obj.append("{");
		obj.append("TransId:" +this.TransId);
		obj.append("TimeStamp:" +this.TimeStamp);
		obj.append("TransactionState:" +this.TransactionState);
		obj.append("ItemsLocked:");
		Iterator ip = this.ItemsLocked.iterator();
		while(ip.hasNext())
		{
			obj.append(ip.next());
			obj.append(",");
		}
		obj.append("OperationList:");
		Iterator pp = this.OperationsList.iterator();
		while(pp.hasNext())
		{
			obj.append(pp.next());
			obj.append(",");
		}
		obj.append("}");
		return obj.toString();
		
	}

}
