package CSCI485ClassProject;

import java.util.ArrayList;
import java.util.List;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.IndexType;

public interface Indexes {
  /**
   * Create index structures for an existing attribute in a table.
   *
   * The table and the target attribute should exist.
   * @param tableName the target table name
   * @param indexType the index type
   * @param attrName the target attribute name
   * @return StatusCode
   */
  StatusCode createIndex(String tableName, String attrName, IndexType indexType);

  /**
   * Drop the index structure from an attribute in a table.
   *
   * The table, attribute and index structure should exist.
   * @param tableName the target table name
   * @param attrName the target attribute name
   * @return StatusCode
   */
  StatusCode dropIndex(String tableName, String attrName);
  
  public void closeDatabase();

  public static List<String> getIndexPath(Transaction tx, String tableName, String attrName){
    List<String> indexPath = new ArrayList<>();
    indexPath.add(tableName);
    indexPath.add(DBConf.TABLE_INDEX_STORE);
    indexPath.add(attrName);

    String[] types = {"hash", "b+"};

    for (String type : types) {
      indexPath.add(type);
      if (FDBHelper.doesSubdirectoryExists(tx, indexPath)) {
        return indexPath;
      }
      indexPath.remove(indexPath.size()-1);
    }

    indexPath.clear();
    return indexPath;
  }
}
