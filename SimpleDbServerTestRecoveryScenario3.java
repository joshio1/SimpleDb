import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class SimpleDbServerTestRecoveryScenario3 {

	public static void main (String args[]){
//		Init a simpleDB Client.
		SimpleDB.init("simpleDB");
//		Creating a Block.
		Block blk1 = new Block("xyz", 1);
		Block blk2 = new Block("abc", 2);
//		Create a RecoveryManager with id = 123.
		RecoveryMgr rm1 = new RecoveryMgr(123);
//		Commit a transaction.
		rm1.commit();
//		Recover a transaction.
		rm1.recover();
//		Sample setInt
		BufferMgr buffmgr = new BufferMgr(8);
		Buffer buff = buffmgr.pin(blk1);
		Buffer buff2 = buffmgr.pin(blk2);
		int lsn = rm1.setInt(buff, 4, 10);
		buff.setInt(4, 0, 123, lsn);
		lsn = rm1.setInt(buff2, 24, 123);
		buff2.setInt(24, 123, 1234, lsn);
//		Flushing all transactions
		buffmgr.flushAll(123);
//		Using Log Record Iterator to print records .
		LogRecordIterator it = new LogRecordIterator();
		System.out.println(it.nextForward());
		System.out.println(it.nextForward());
		System.out.println(it.next());
		System.out.println(it.next());
		
		
	}
	
}
