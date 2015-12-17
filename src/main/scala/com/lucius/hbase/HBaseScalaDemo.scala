package com.lucius.hbase

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object HBaseScalaDemo {
  private val TABLE_NAME = "MY_TABLE_NAME_TOO"
  private val CF_DEFAULT = "DEFAULT_COLUMN_FAMILY"

  def createOrOverwrite(admin: Admin, table: HTableDescriptor) {
    if (admin.tableExists(table.getTableName())) {
      admin.disableTable(table.getTableName())
      admin.deleteTable(table.getTableName())
    }
    admin.createTable(table)
  }

  def createSchemaTables(config: Configuration) = {
    val connection: Connection = ConnectionFactory.createConnection(config)
    val admin: Admin = connection.getAdmin()
    val table: HTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME))
    table.addFamily(new HColumnDescriptor(CF_DEFAULT))
    //原来的Demo中有.setCompressionType(Algorithm.SNAPPY)，但是这个会报错
    //table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY))
    println("###### Creating table #######")
    createOrOverwrite(admin, table)
    System.out.println(" Done.")
  }

  def testScan(config: Configuration) = {
    val connection: Connection = ConnectionFactory.createConnection(config)
    //表的名称是"member"
    val table: Table = connection.getTable(TableName.valueOf("member"));
   // 被废弃的方式：val table2: Table = new HTable(config, "member")
    val scan = new Scan()
    //增加对列族"info"的查询
    scan.addFamily("info".getBytes)
    //增加对列"address：country"的查询
    scan.addColumn("address".getBytes, "country".getBytes)
    //过滤出前缀为"000"的RowKey的数据
    scan.setRowPrefixFilter(Bytes.toBytes("000"))

    val rs:ResultScanner = table.getScanner(scan)
    val it = rs.iterator()
    while(it.hasNext) {
      println(it.next())
    }
    rs.close()
  }

  def modifySchema(config: Configuration) = {
    val connection: Connection = ConnectionFactory.createConnection(config)
    val admin: Admin = connection.getAdmin()
    val tableName: TableName = TableName.valueOf(TABLE_NAME)
    if (!admin.tableExists(tableName)) {
      println("Table does not exist.")
      System.exit(-1)
    }
    val table: HTableDescriptor = new HTableDescriptor(tableName)

    val newColumn: HColumnDescriptor = new HColumnDescriptor("NEWCF")
    //newColumn.setCompactionCompressionType(Algorithm.GZ);
    newColumn.setMaxVersions(HConstants.ALL_VERSIONS)
    admin.addColumn(tableName, newColumn)

    // Update existing column family
    val existingColumn: HColumnDescriptor = new HColumnDescriptor(CF_DEFAULT)
    existingColumn.setCompactionCompressionType(Algorithm.GZ)
    existingColumn.setMaxVersions(HConstants.ALL_VERSIONS)
    table.modifyFamily(existingColumn)
    admin.modifyTable(tableName, table)

    // Disable an existing table
    admin.disableTable(tableName)

    // Delete an existing column family
    admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"))

    // Delete a table (Need to be disabled first)
    admin.deleteTable(tableName)
  }

  def main(args: Array[String]) {
    val config = HBaseConfiguration.create()

    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"))
    //createSchemaTables(config)
    //modifySchema(config);
    testScan(config)
    // println(System.getenv("HBASE_CONF_DIR"))
    // conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
  }
}
