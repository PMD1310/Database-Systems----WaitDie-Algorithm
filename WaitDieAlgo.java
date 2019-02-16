package concurrency;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import concurrency.*;

public class WaitDieAlgo {
	public static HashMap<Integer, TransactionTable> TransMap = new HashMap<Integer, TransactionTable>();
	public static HashMap<Character, LockTable> LockMap = new HashMap<Character, LockTable>();
	public static ArrayList<Integer> ActiveTransaction = new ArrayList<Integer>();
	
	public static void operationAction(String readLine)
	{
		int transid, pos;
		TransactionTable t;
		char obj;
		LockTable lockobj;
		if(readLine.charAt(0)=='b')
		{
			transid = Integer.parseInt(String.valueOf(readLine.charAt(1)));
			t = new TransactionTable(transid, "Active");
			TransMap.put(transid, t);
			System.out.println(readLine+ " -- Operation Successful => Transaction "+transid+" has started");
			System.out.println(t.toString());
						
		}
		else if(readLine.charAt(0)=='r')
		{
			transid = Integer.parseInt(String.valueOf(readLine.charAt(1)));
			pos = readLine.indexOf("(");
			obj = readLine.charAt(pos+1);
			t = TransMap.get(transid);
			
			if(LockMap.containsKey(obj))
			{
				lockobj = LockMap.get(obj);
			}
			else
			{
				lockobj = new LockTable(obj);
			}
			if(t.TransactionState.equalsIgnoreCase("BLOCKED"))
			{
				t.appendOperation(readLine);
				System.out.println(readLine+" --Operation is Successful => Operation added to Operation List");
				System.out.println(t.toString());
			}
			else if(t.TransactionState.equalsIgnoreCase("ABORTED"))
			{
				System.out.println("-- Operation is Ignored --");
			}
			else
			{
				if(lockobj.LockMode.equalsIgnoreCase("NL"))
				{
					lockobj.LockMode = "RL";
					lockobj.addholdTransaction(t.TransId);
					t.addHoldObject(lockobj.ItemName);
					LockMap.put(lockobj.ItemName, lockobj);
					System.out.println(readLine+ "-- Operation is Successful => "+lockobj.ItemName+" is locked by "+t.TransId+"");
					System.out.println(t.toString());
					System.out.println(lockobj.toString());
				}
				else if(lockobj.LockMode.equalsIgnoreCase("RL"))
				{
					lockobj.addholdTransaction(t.TransId);
					t.addHoldObject(lockobj.ItemName);
					LockMap.put(lockobj.ItemName, lockobj);
					System.out.println(readLine+ "-- Operation is Successful => "+lockobj.ItemName+" is locked by "+t.TransId+"");
					System.out.println(t.toString());
					System.out.println(lockobj.toString());
				}
				else
				{
					if(lockobj.LockMode.equalsIgnoreCase("WL") && lockobj.LockedBy.contains(t.TransId) && (lockobj.LockedBy.size() == 1))
					{
						System.out.println(readLine+ "-- Operation is Successful => Lock on "+lockobj.ItemName+" is already locked by " +t.TransId);
						System.out.println(t.toString());
						System.out.println(lockobj.toString());
					}
					else
					{
						int i = 0;
						while(TransMap.get(lockobj.LockedBy.get(i)).TransId == t.TransId)
						{
							i++;
						}
						TransactionTable anotherTrans = TransMap.get(lockobj.LockedBy.get(i));
						if(t.TimeStamp.before(anotherTrans.TimeStamp))
						{
							lockobj.addWaitList(t.TransId);
							t.appendOperation(readLine);
							t.TransactionState = "BLOCKED";
							LockMap.put(lockobj.ItemName, lockobj);
							System.out.println(readLine+" -- Operation is Successful => "+t.TransId+" is blocked");
							System.out.println(t.toString());
							System.out.println(lockobj.toString());
						}
						else
						{
							t.TransactionState = "ABORTED";
							Iterator<Character> ip = t.ItemsLocked.iterator();
							while(ip.hasNext())
							{
								char name = ip.next();
								lockobj = LockMap.get(name);
								if(lockobj.LockedBy.size() == 1)
								{
									lockobj.LockMode = "NL";
								}
								lockobj.LockedBy.remove(t.TransId);
								Iterator pp = lockobj.WaitList.iterator();
								while(pp.hasNext())
								{
									int j = (int)pp.next();
									if(!ActiveTransaction.contains(j));
									ActiveTransaction.add(j);
								}
								lockobj.WaitList.clear();
								LockMap.put(lockobj.ItemName, lockobj);
								System.out.println(lockobj.toString());
							}
							t.ItemsLocked.clear();
							System.out.println(readLine+ " -- Operation is Successful => Transaction "+t.TransId+" is Aborted");
							System.out.println(t.toString());
						}										
					}
				}
			}
			TransMap.put(t.TransId, t);
		}
		else if(readLine.charAt(0) == 'w')
		{
			transid = Integer.parseInt(String.valueOf(readLine.charAt(1)));
        	pos = readLine.indexOf("(");
        	obj = readLine.charAt(pos+1);
        	t = TransMap.get(transid);
        	
        	//Create new object or use existing object
        	if(LockMap.containsKey(obj))
        		lockobj = LockMap.get(obj);
    		else
    			lockobj = new LockTable(obj);
        	
        	//action based on transaction state
        	if(t.TransactionState.equalsIgnoreCase("BLOCKED"))
        	{
        		t.appendOperation(readLine);
        		System.out.println(readLine+ " - Opeartion is Successful => Operation added to opeartion list");
        		System.out.println(t.toString());
        	}
        	else if(t.TransactionState.equalsIgnoreCase("ABORTED"))
        	{
        		System.out.println(readLine+ " - Opeartion is Ignored");
        	}
        	else
        	{
        		if(lockobj.LockedBy.contains(t.TransId) && (lockobj.LockedBy.size() == 1))
        		{
        			lockobj.LockMode ="WL";
        			LockMap.put(lockobj.ItemName, lockobj);
        			System.out.println(readLine+ " -- Operation is Successful => Lock on "+ lockobj.ItemName+" by "+ t.TransId +" is upgraded to Write-Lock");
        			System.out.println(t.toString());
        			System.out.println(lockobj.toString());
        		}
        		else if (lockobj.LockMode.equalsIgnoreCase("NL"))
        		{
        			lockobj.LockMode = "RL";
        			lockobj.addholdTransaction(t.TransId);
        			t.addHoldObject(lockobj.ItemName);
        			LockMap.put(lockobj.ItemName, lockobj);
        			System.out.println(readLine+ " -- Operation is Successful => "+lockobj.ItemName+" is locked by "+t.TransId+"");
        			System.out.println(t.toString());
        			System.out.println(lockobj.toString());
        		}
        		else
        		{
        			int i = 0;
        			while(TransMap.get(lockobj.LockedBy.get(i)).TransId == t.TransId)
    				{
    					i++;
    				}
        			TransactionTable anotherTrans = TransMap.get(lockobj.LockedBy.get(i));
    				if(t.TimeStamp.before(anotherTrans.TimeStamp))
    				{
    					lockobj.addWaitList(t.TransId);
    					t.appendOperation(readLine);
    					t.TransactionState = "BLOCKED";
    					LockMap.put(lockobj.ItemName, lockobj);
    					System.out.println(readLine+ " - Opeartion is Successful => Transaction "+t.TransId+" is Blocked");
    					System.out.println(t.toString());
            			System.out.println(lockobj.toString());
    				}
    				else
    				{
    					t.TransactionState = "ABORTED";
    					Iterator<Character> it = t.ItemsLocked.iterator();
                    	while(it.hasNext())
                    	{
                    		char name = it.next();
                    		lockobj = LockMap.get(name);
                    		if(lockobj.LockedBy.size() == 1)
                    		{
                    			lockobj.LockMode = "NL";
                    		}
                    		lockobj.LockedBy.remove(t.TransId);
                    		Iterator jt = lockobj.WaitList.iterator();
                    		while(jt.hasNext())
                    		{
                    			int j = (int) jt.next();
                    			if(!ActiveTransaction.contains(j))
                    				ActiveTransaction.add(j);
                    			
                    		}
                    		lockobj.WaitList.clear();
                    		LockMap.put(lockobj.ItemName, lockobj);
                    		System.out.println(lockobj.toString());
                    	}
                    	t.ItemsLocked.clear();
                    	System.out.println(readLine+ " - Opeartion is Successful => Transaction "+t.TransId+" is Aborted");
                    	System.out.println(t.toString());
    				}
        		}
        	}
        	TransMap.put(t.TransId, t);
        }
		
		else if (readLine.charAt(0) == 'e')
		{
			transid = Integer.parseInt(String.valueOf(readLine.charAt(1)));
			t = TransMap.get(transid);
			if(t.TransactionState.equalsIgnoreCase("BLOCKED"))
			{
				t.appendOperation(readLine);
				System.out.println(readLine+ " -- Operation is Successful => Operation is added to Operation List");
				System.out.println(t.toString());
			}
			else if(t.TransactionState.equalsIgnoreCase("ABORTED"))
					{
				System.out.println(readLine+ " -- Operation is Aborted.");
					}
			else
			{
				t.TransactionState = "COMMITTED";
				Iterator<Character> ip = t.ItemsLocked.iterator();
				StringBuffer sb = new StringBuffer();
				while(ip.hasNext())
				{
					char name = ip.next();
					lockobj = LockMap.get(name);
					if(lockobj.LockedBy.size() == 1)
					{
						lockobj.LockMode = "NL";
					}
					lockobj.LockedBy.remove(t.TransId);
					Iterator pp = lockobj.WaitList.iterator();
					while(pp.hasNext())
					{
						int j = (int)pp.next();
						if(!ActiveTransaction.contains(j))
							ActiveTransaction.add(j);
					}
					lockobj.WaitList.clear();
					LockMap.put(lockobj.ItemName, lockobj);
					System.out.println(lockobj.toString());
				}
				t.ItemsLocked.clear();
				TransMap.put(t.TransId, t);
				System.out.println(readLine+ " -- Operation is Successful => Transaction "+t.TransId+" has committed");
				System.out.println(t.toString());
			}
		}
		else
		{
			System.out.println("Invalid Operation");
		}
    				
	}
	
        public static void main(String[] args)
        {
        	try
        	{
        		File f = new File("E:\\DB 2\\Input9.txt");
        		BufferedReader bf = new BufferedReader(new FileReader(f));
        		String readLine = "";
        		TransactionTable trans;
        		while((readLine = bf.readLine())!= null)
					{
        				operationAction(readLine);
        				int k = 0;
        				int transid;
        				ArrayList<Integer> arlst = (ArrayList<Integer>) ActiveTransaction.clone();
        				while(arlst.size()!=0)
        				{
        					System.out.println("Inside Active Transaction List");
        					transid = arlst.get(k);
                    		trans = TransMap.get(transid);
                    		trans.TransactionState = "ACTIVE";
                    		TransMap.put(transid, trans);
                    		Iterator it = trans.OperationsList.iterator();
                    		while(it.hasNext())
                    		{
                    			operationAction((String) it.next());
                    		}
                    		trans.OperationsList.clear();
                    		arlst.remove(k);
                    		k++;
                    		TransMap.put(transid, trans);
                    	}
                    	ActiveTransaction.clear();
                    	               	
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

        		
        	}

        }
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		
        		