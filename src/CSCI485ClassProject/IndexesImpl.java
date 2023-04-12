package CSCI485ClassProject;

import java.util.ArrayList;
import java.util.List;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.Cursor.Mode;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;

public class IndexesImpl implements Indexes{

  private Database db;

  public IndexesImpl() {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    List<String> indexPath = new ArrayList<>();
    indexPath.add(tableName);
    indexPath.add(DBConf.TABLE_INDEX_STORE);
    indexPath.add(attrName);

    Transaction tx = FDBHelper.openTransaction(db);
    
    if (FDBHelper.doesSubdirectoryExists(tx, indexPath)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }
    indexPath.add(indexType == IndexType.NON_CLUSTERED_HASH_INDEX ? "hash" : "b+");
    DirectorySubspace indexSubspace = FDBHelper.createOrOpenSubspace(tx, indexPath);

    Records records = new RecordsImpl();
    Cursor cursor = records.openCursor(tableName, Mode.READ);
    List<String> primaryKeys = records.getPrimaryKeys(tx, tableName);

    Record curr = records.getFirst(cursor);
    while (true) {
      Tuple key = new Tuple();
      Tuple val = new Tuple();

      key.addObject(curr.getValueForGivenAttrName(attrName, indexType == IndexType.NON_CLUSTERED_HASH_INDEX));
      for (String primaryKey : primaryKeys) {
        key.addObject(curr.getValueForGivenAttrName(primaryKey));
      }

      FDBHelper.setFDBKVPair(indexSubspace, tx, new FDBKVPair(indexPath, key, val));

      if (cursor.hasNext()) {
        curr = records.getNext(cursor);
      } else {
        break;
      }
    }
    records.closeDatabase();

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    List<String> indexPath = new ArrayList<>();
    indexPath.add(tableName);
    indexPath.add(DBConf.TABLE_INDEX_STORE);
    indexPath.add(attrName);

    Transaction tx = FDBHelper.openTransaction(db);
    
    if (!FDBHelper.doesSubdirectoryExists(tx, indexPath)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_NOT_FOUND;
    }

    FDBHelper.dropSubspace(tx, indexPath);
    if (FDBHelper.commitTransaction(tx)) {
      return StatusCode.SUCCESS;
    }
    
    return StatusCode.INDEX_NOT_FOUND;
  }

  @Override
  public void closeDatabase() {
    FDBHelper.close(db);
  }
}
