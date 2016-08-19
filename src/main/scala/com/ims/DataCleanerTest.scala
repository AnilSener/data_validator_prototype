package com.ims

/*import dk.eobjects.metamodel.DataContext
import dk.eobjects.datacleaner.data.DataContextSelection
import dk.eobjects.metamodel.schema.TableType*/
//import scala.io.Source
import org.apache.metamodel.csv.CsvConfiguration
import org.datacleaner.beans.filter.NullCheckFilter
import org.datacleaner.beans.writers.ErrorHandlingOption
import org.datacleaner.configuration.DataCleanerEnvironmentImpl
import org.datacleaner.job.concurrent.SingleThreadedTaskRunner

//import scala.reflect.io.Directory
import org.datacleaner.configuration.ConfigurationReader
import org.datacleaner.configuration.DataCleanerConfigurationImpl
import org.datacleaner.connection.{CsvDatastore, Datastore, DatastoreCatalog, DatastoreCatalogImpl, JdbcDatastore}

import org.datacleaner.job.builder.{AnalysisJobBuilder, AnalyzerComponentBuilder}
import org.datacleaner.beans.writers.{InsertIntoTableAnalyzer, WriteDataResult}

import scala.collection.JavaConverters._
import org.apache.metamodel.util.FileResource
import org.datacleaner.api.InputColumn
import org.datacleaner.job.concurrent.MultiThreadedTaskRunner
import org.datacleaner.job.runner.{AnalysisResultFuture, AnalysisRunnerImpl}
import org.datacleaner.result.AnalysisResult

import scala.collection.mutable
import scala.util.{Try,Success, Failure}

/**
  * Created by asener on 12/08/2016.
  */
object DataCleanerTest {
  private final val USER="testuser"
  private final val PASSWORD="kan66CAN"
  private final val CATALOG="test"
  private final val TARGET_DB_HOST="localhost"
  private final val TARGET_DB_PORT="1433"
  private final val INSTANCE="SQLEXPRESS"
  private final val TABLE="Farmatic_Ventas"
  private final val EMPTY_STRING_NULL=false
  def main(args: Array[String]): Unit = {

    val conf=createDataCleanerConf()
    val target = conf.getDatastoreCatalog().getDatastore("target");
    val sourceCsvFile = conf.getDatastoreCatalog().getDatastore("Compras1");

    //GET TARGET COLUMNS
    //TODO: Think about using lazy values
    val dbcon=target.openConnection()
    val targetTableDefs=dbcon.getDataContext.getDefaultSchema.getTables.filter(_.getName.equals("Farmatic_Compras"))
    val targetCols=  targetTableDefs.flatMap(_.getColumns).map(_.getName).toList
    val targetNotNullCols = targetTableDefs.flatMap(_.getColumns).filterNot(_.isNullable).map(_.getName)
    println("Not Nullable Target Columns are:"++targetNotNullCols.mkString(","))
    dbcon.close()
    println("TARGET COLUMNS: "++targetCols.mkString(","))

    //GET SOURCE COLUMNS AS INPUT COLUMNS
    val con=sourceCsvFile.openConnection()
    //println(con.getSchemaNavigator.getDefaultSchema.getTableNames.mkString(","))
    //println(con.getSchemaNavigator.getSchemas.mkString(","))
    con.getSchemaNavigator.getSchemaByName("classes").getTables.map(_.getColumnNames.mkString(",")).foreach(println)
    val sourceColumns=con.getDataContext.getDefaultSchema.getTables.flatMap(_.getColumns).toList
    con.getDataContext.getDefaultSchema.getTables.map(_.getColumns.mkString(",")).foreach(println)
    con.close()

    println("sourceColumns"+sourceColumns.map(_.getName).mkString(","))

    //STARTING TO BUILD AN ANALYSIS JOB
    val builder:AnalysisJobBuilder = new AnalysisJobBuilder(conf);
    builder.setDatastore("Compras1")
    builder.addSourceColumns(sourceColumns.asJava)
    //Convert to a name for Target Column Name -> Input Column
    val inputCols= sourceColumns.map(_.getName).map(builder.getSourceColumnByName(_))
    val inputColsMap = targetCols.zip(inputCols).toMap

    // add a filter to check for NOT NULL fields
    val notNullConsInputCols=targetNotNullCols.map(inputColsMap(_))
    //TODO: Receiving an error here
    val nullCheckBuilder = builder.addFilter(classOf[org.datacleaner.beans.filter.NullCheckFilter]);
    nullCheckBuilder.setConfiguredProperty("Consider empty string as null",EMPTY_STRING_NULL)
    notNullConsInputCols.foreach(nullCheckBuilder.addInputColumn(_));
    val nullOutcome=nullCheckBuilder.getFilterOutcome(NullCheckFilter.NullCheckCategory.NULL)

    println("Null Filter Outcome: "+nullOutcome.getSimpleName())

    //TODO: Generic Writer based on different cases can be created, we can seek to generalize it
    val nullCheckFailWriter = builder.addAnalyzer(classOf[org.datacleaner.extension.output.CreateCsvFileAnalyzer]);
    nullCheckFailWriter.addInputColumns(inputCols.asJava)
    nullCheckFailWriter.setRequirement(nullOutcome);

    val insertBuilder:AnalyzerComponentBuilder[InsertIntoTableAnalyzer]  = builder.addAnalyzer(classOf[InsertIntoTableAnalyzer]);

    insertBuilder.addInputColumns(inputCols.asJava)
    insertBuilder.setConfiguredProperty("Datastore", target);
    insertBuilder.setConfiguredProperty("Column names", targetCols.toArray);
    insertBuilder.setConfiguredProperty("Schema name", "dbo");
    insertBuilder.setConfiguredProperty("Table name", "Farmatic_Compras");
    //Enable when local db configuration works
    //insertBuilder.setConfiguredProperty("Truncate table",true);
    insertBuilder.setConfiguredProperty("How to handle insertion errors?", ErrorHandlingOption.SAVE_TO_FILE);
    insertBuilder.setConfiguredProperty("Error log file location", new java.io.File("logs/error.log"));
    insertBuilder.setConfiguredProperty("Additional error log values", inputCols.toArray);
    //insertBuilder.setRequirement(notNullOutcome);


    // validate and produce to AnalysisJob
    val analysisJob = builder.toAnalysisJob();

    //EXECUTION
    executeAnalysisJob(conf,analysisJob) match{
      case Success(resultFuture)=> handleResult(resultFuture)
      case Failure(e) => println("Future failed:"++e.getMessage)
    }

  }

  //Executes Analysis Job
  def executeAnalysisJob(conf:DataCleanerConfigurationImpl,analysisJob:org.datacleaner.job.AnalysisJob):Try[AnalysisResultFuture]={
    val runner = new AnalysisRunnerImpl(conf);
    val resultFuture = runner.run(analysisJob);

    resultFuture.await();
    if (resultFuture.isSuccessful()) {
      // do something with the successful result
      Try(resultFuture)
    }
    else {
      val errors:mutable.Buffer[Throwable] = resultFuture.getErrors().asScala;
      for (error <- errors) {
        //logger.error("An error occurred while executing job", error);
        println("An error occurred while executing job", error)
      }
      // usually the first error that occurred is the culprit, so we'll throw that one
      throw errors(0);
    }
  }

  //Handles the Result of the Analysis Job Execution
  def handleResult(analysisResult:AnalysisResult):Unit={
    // demonstrate the the result
    // future implements the AnalysisResult interface, which is sufficient
    // for all the followin operations

    val results = analysisResult.getResults().asScala;

    results.foreach{
      case w:WriteDataResult=>{
        println("Inserted " + w.getWrittenRowCount() + " records")
      }
      case r:AnalysisResult=>{
        println("Other Analysis Results...")
      }
    }

  }

  def createDataCleanerConf(multiThreaded:Boolean=true):DataCleanerConfigurationImpl={
    val url=getClass.getResource("/C0010016.04")
    val filename=url.getPath.substring(url.getPath.lastIndexOf("/")+1)
    val url2=getClass.getResource("/C0010916.04")
    val filename2=url2.getPath.substring(url2.getPath.lastIndexOf("/")+1)
    val url3=getClass.getResource("/V0010016.04")
    //Col line number 0
    import org.apache.metamodel.csv.CsvConfiguration.DEFAULT_ESCAPE_CHAR
    val csv_conf=new CsvConfiguration(0, "UTF-8",  '|', '"'.toChar,DEFAULT_ESCAPE_CHAR, true)


    val datastore1:Datastore = new CsvDatastore("Compras1",new FileResource(url.getFile), csv_conf);

    val datastore2:Datastore = new CsvDatastore("Compras2",new FileResource(url.getFile), csv_conf);
    val sourceList = List(datastore1,datastore2)
    val datastore3:Datastore = new CsvDatastore("Ventas", url3.getPath);
    val multipleConnections:Boolean = true
    val target:Datastore = new JdbcDatastore("target",
      s"jdbc:jtds:sqlserver://$TARGET_DB_HOST:$TARGET_DB_PORT/$CATALOG;instance=$INSTANCE;useUnicode=true;characterEncoding=UTF-8", "net.sourceforge.jtds.jdbc.Driver",
      USER,PASSWORD, multipleConnections);

    val validationMap:Map[String,List[Datastore]]=Map("Farmatic_Compras"->sourceList)
    val taskRunner=multiThreaded match {
      case true => new MultiThreadedTaskRunner(20);
      case false =>new SingleThreadedTaskRunner();
    }

    val environment = new DataCleanerEnvironmentImpl().withTaskRunner(taskRunner)
    val configuration = (new DataCleanerConfigurationImpl()
      .withDatastoreCatalog(new DatastoreCatalogImpl((target::validationMap("Farmatic_Compras")).asJava))
      .withEnvironment(environment));

    //configuration = configuration.replace(new MultiThreadedTaskRunner(10));
    //configuration = configuration.replace(new DatastoreCatalogImpl(datastore1, target));
    //println(configuration.getDatastoreCatalog())
    println(configuration.getDatastoreCatalog.getDatastoreNames.mkString(","))
    println(configuration.getDatastoreCatalog.getDatastore("Compras1").getName())
    println(configuration.getServerInformationCatalog.getServerNames.mkString(","))
    configuration
  }

}
