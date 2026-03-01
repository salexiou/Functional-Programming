import org.apache.spark.sql.SparkSession

object FP_Application01 extends App{

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Application_01")
   .getOrCreate()


  /////// Paths Of 1st Application ////////

  /////// Path of data with Document - Category information ////////
  val pathTopicsRCV ="hdfs://localhost:9000/FinalProjectData/Data_Exercise_1/rcv1-v2.topics.qrels"
  val data_topics = spark.sparkContext.textFile(pathTopicsRCV)

  /////// Path of data with Document - Term information ////////
  val pathDatFiles = "hdfs://localhost:9000/FinalProjectData/Data_Exercise_1/datFiles"
  val data_datFiles = spark.sparkContext.textFile(pathDatFiles)

  /////// Path of data with Stem - Term information ////////
  val pathStemFiles = "hdfs://localhost:9000/FinalProjectData/Data_Exercise_1/stem.termid.idf.map.txt"
  val data_StemFiles = spark.sparkContext.textFile(pathStemFiles)

  /////// Path of Output File ////////

  val finalOutputPath = "hdfs://localhost:9000/outputFiles_App1/finalOutput"

  ////// CALCULATE |DOC(C)| //////

  val dataProcess001 = data_topics
    .map(line => { // No need for flatmap , data is already in lines of "<category name><document id>1"
      val parts = line.split("\\s+") // Split the lines based on  whitespace
      (parts(1), parts(0)) // Keep <category name> , <document id> for each line
    })
  //////  Result : Tuples of -> ( Document Id , Category Name) //////

  val resultData001 = dataProcess001
    .map (a => (a._2, 1)) // Creating tuples of -> (Category Name,1) for counting
    .reduceByKey(_ + _) // Counting the number of Document Ids per category

  //////  Result : Tuples -> (Category Name , Number of Document Ids per category) //////


  //////////////////////// CALCULATE |DOC(T)| ////////////////////////

  val dataProcess002 = data_datFiles
    .flatMap{ line =>
      val parts = line.split("\\s+",2) // Split lines into pairs of -> (<document id> , <term id>: <weight>)
      val documentId = parts(0) // Keep the first element of the parts variable (<document id>)
      val terms = parts(1).split("\\s+").map(_.split(":")(0)) // Keep the first element of the second element of parts (<termid>) by splitting based on ':' for each part
      terms.map(term => (documentId , term)) // Keep for each line the document id and the term id
    }

  //////  Result : Tuples -> (Document Id , Term Id) //////

  val resultData002 = dataProcess002
    .map (a => (a._2, 1)) //  Creating tuples of -> (Term Id,1) for counting
    .reduceByKey(_ + _) // Counting the number of Document Ids per Term

  //////  Result : Tuples -> (Term Id , Number of Document Ids Per Term) //////

  //////////////////////// CALCULATE |DOC (T) ∩ DOC (C)|   ////////////////////////

  val resultData003 = dataProcess001
    .join(dataProcess002) // Join Data with Document Id as key which by default is the first element of the tuples
    // Result -> (Document Id,(Category Name, Term Id))
    .map (result => (result._2 , 1)) // Creating tuples of -> ((Category Name,Term Id),1) for counting
    .reduceByKey(_+_) // Counting the number of Document Ids Per (Category Name,Term Id) tuple
    .map(finalresult => (finalresult._1._1,finalresult._1._2,finalresult._2)) // Creating tuples of (Category Name, Term Id , Number of Document Ids Per (Category Name,Term Id)) for later joins

  //////  Result : Tuples -> (Category Name, Term Id , Number of Document Ids Per Term-Category pair) //////

  //////      Process data with stem       //////

  val dataProcess004 = data_StemFiles
    .map(line => {
      val parts = line.split("\\s+") // Split lines based on whitespace
      (parts(1), parts(0)) // Returning tuples of (Term id, Stem)
    })

  //////  Result : Tuples -> (termId,stem) //////

  val addNumberOfDocumentsPerCategoryName = resultData003.keyBy(_._1) // Set as key the first element (Category Name)
    .join(resultData001) // Join the RDDs on key : Category Name in order to add the number of Document Per Category
    // Result -> (Category Name,((Category Name,Term Id,|DOC (T) ∩ DOC (C)|), |DOC(C)|))
    .map(result => (result._2._1._1,result._2._1._2,result._2._1._3,result._2._2))

  //////  Result : Tuples -> (Category Name,Term Id,Number Of Documents Per Pair,Number Of Documents Per Category) //////

  val addNumberOfDocumentsPerTerm = addNumberOfDocumentsPerCategoryName.keyBy(_._2) // Set as key the second element (Term Id)
    .join(resultData002) // Join the RDDs on key : Term Id in order to add the number of Document Per Term
    // Result -> (Term Id,((Category Name,Term Id,|DOC (T) ∩ DOC (C)|,|DOC(C)|),|DOC(T)|))
    .map(result => (result._2._1._1,result._2._1._2,result._2._1._3,result._2._1._4,result._2._2))

  //////  Result : Tuples -> (Category Name,Term Id,Number Of Documents Per Pair,Number Of Documents Per Category,Number Of Documents Per Term) //////

  val replaceTermWithStem = addNumberOfDocumentsPerTerm.keyBy(_._2) // Set as key the second element (Term Id)
    .join(dataProcess004) // Join the RDDs on key : Term Id in order to replace the term with the corresponding stem
    // Result -> (Term Id,((Category Name,Term Id,|DOC (T) ∩ DOC (C)|,|DOC(C)|,|DOC(T)|),Stem))
    .map(result => (result._2._1._1,result._2._2,result._2._1._3,result._2._1._4,result._2._1._5))

  //////  Result : Tuples -> (Category Name,Stem,Number Of Documents Per Pair,Number Of Documents Per Category,Number Of Documents Per Term) //////

  // Calculate the Jaccard Index for every tuple in 'replaceTermWithStem'
  val ResultWithJaccardIndex = replaceTermWithStem
    .map(result => (result._1,result._2,result._3.toFloat/(result._4 + result._5 - result._3).toFloat)) // calculation of Jaccard Index based on definition given
    // and return tuples of -> (Category Name , Stem , Jaccard Index)
    .map{ finalResult =>
      val CategoryName = finalResult._1
      val Stem = finalResult._2
      val JaccardIndex = finalResult._3
      s"$CategoryName ; $Stem ; $JaccardIndex"
    }// Map the results in order to modify them to given form (<category name> ;<stemid>;<JaccardIndex>)


  ResultWithJaccardIndex.saveAsTextFile(finalOutputPath) // Save the results as text file in to output path defined earlier

  spark.stop()
}