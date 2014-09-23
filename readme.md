Pho
===

Pho is an HBase client library.
Its primary purpose is to simplify working with HBase data through `get`, `put` and `scan`.
This is mostly accomplished by encoding/decoding scala data types to/from arrays of HBase bytes.

Example usage
-------------

### define a schema

    class MyTable(connection: HConnection) extends PhoTable(connection, "MyTable") {
      // converters convert types to bytes and bytes to types
      val RowKeyConverter = PhoenixConverters.IntConverter
    
      // a pho column is defined by a family, qualifier and converter
      val cf1 = ColumnFamily("cf1")
      val col1 = Column(cf1, "col1", PhoenixConverters.StringConverter)
    }

### get a connection and table instance

    zkQuorum = "localhost"
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = HConnectionManager.createConnection(configuration)

    val myTable = new MyTable(connection)

### write a document into the table

    // this could be a made a method of MyTable
    val key = RowKey(MyTable.RowKeyConverter, 42)
    val doc = Document(key, Seq(
      Cell(MyTable.col1, "Arthur Dent")
    ))
    myTable.write(doc)

### read cells from the table

    val doc = myTable.read(key, Seq(col1))
    // returns Document(key, Seq(Cell(col1, "Arthur Dent")))

### query rows from the table

    val startKey = RowKey(rowKeyConverter, 42)
    val endKey = RowKey(rowKeyConverter, 100)
    val query = Query(
        startKey,
        endKey,
        Seq(col1)  // included in the result set
    )
    val resultSet = myTable.read(query)
    // returns Document(key, Seq(Cell(col1, "Arthur Dent")))

### limit the query result to 10 results

    val query = Query(
        startKey,
        endKey,
        Seq(col1),  // included in the result set
        LimitFilter(10)   // only include 10 results
    )

### look for a document containing a particular value

    val query = Query(
        startKey,
        endKey,
        Seq(col1),  // included in the result set
        EqualsFilter(Cell(col1, "Arthur Dent"))   // only find people with col1="Arthur Dent"
    )

### compound filters

    val query = Query(
        startKey,
        endKey,
        Seq(col1),  // included in the result set
        LimitFilter(1) and EqualsFilter(Cell(col1, "Arthur Dent"))   // get first person named "Arthur Dent"
    )

License
-------

Pho: Copyright 2014 Tagged, Inc.

Pho is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License.

You should have received a copy of the license along with this work. If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
