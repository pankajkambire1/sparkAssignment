import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source


object sparkAssignemt extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","Spark Assignment 2")
  
  val rdd1 = sc.textFile("/Users/Pankaj/Downloads/chapters.csv")
  
  val rdd2 = rdd1.map(x => (x.split(",")(0),x.split(",")(1)))
  
  val rdd3 = rdd2.map(x => (x._2,(x._1,1)))
  
  val rdd4 = rdd3.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2)).map(x=>(x._1.toInt,x._2._2.toInt))
  
  val view1Rdd = sc.textFile("/Users/Pankaj/Downloads/views-*.csv")
  
  val chapterRdd = sc.textFile("/Users/Pankaj/Downloads/chapters.csv")
  
  val titleRdd = sc.textFile("/Users/Pankaj/Downloads/titles.csv")
   
  val title1Rdd = titleRdd.map(x => (x.split(",")(0).toInt,x.split(",")(1)))
 
  val chapter1Rdd = chapterRdd.map(x => (x.split(",")(0).toInt,x.split(",")(1).toInt))
  
  val rddv1 = view1Rdd.map(x => (x.split(",")(1).toInt,x.split(",")(0).toInt)).distinct()

  val finalMapped = rddv1.join(chapter1Rdd)
  
  val dropChap = finalMapped.map(x=> ((x._2._1,x._2._2),1))
  
  val reduceDropped = dropChap.reduceByKey((x,y)=> x+y)
    
  val dropUser = reduceDropped.map(x=> (x._1._2,x._2))
  
  val matchRdd = dropUser.join(rdd4)
  
  val perCentage = matchRdd.map(x => (x._1,x._2._1.toFloat/x._2._2.toFloat))
  
  val newRdd = perCentage.mapValues(x =>
  
    {
     if(x >= 0.9) 10l
     else if(x >=  0.5 &&  x < 0.9  ) 4l
     else if(x >=  0.25 &&  x < 0.5) 2l
     else 0l 
    }
  
  )
  
  val totalScore = newRdd.reduceByKey((x,y)=>x+y)
  
  val newScores = totalScore.join(title1Rdd).map(x => (x._2._1,x._2._2))
  
  newScores.collect().foreach(println)
  
}