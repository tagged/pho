pho
===

Another client library for interacting with HBase.

example usage
-------------

### define a schema

    // table name
    val tableName = "MyTable"

    // converters convert types to bytes and bytes to types
    val rowKeyConverter = PhoenixConverters.IntConverter

    // a pho column is defined by a family, qualifier and converter
    val cf1 = ColumnFamily("cf1")
    val col1 = Column(cf1, "col1", PhoenixConverters.StringConverter)

### get a pho connection

    zkQuorum = "localhost"
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = HConnectionManager.createConnection(configuration)
    val pho = new PhoConnection(connection)

### write a document into the table

    val rowKey = RowKey(42, rowKeyConverter)
    val doc = Document(rowKey, Seq(
      Cell(col1, "Arthur Dent")
    ))
    pho.write(tableName, doc)

### read cells from the table

    val retrievedCells = pho.read(tableName, rowKey, Seq(col1))
    // returns Cell(col1, "Arthur Dent")

### query rows from the table

    val startKey = RowKey(42, rowKeyConverter)
    val endKey = RowKey(100, rowKeyConverter)
    val query = Query(
        startKey,
        endKey,
        Seq(col1)  // included in the result set
    )
    val resultSet = pho.read(tableName, query)
    // returns Seq(Cell(col1, "Arthur Dent"))

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
