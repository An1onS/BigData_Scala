package BigData_Scala

import java.io.{File, FileWriter}
import scala.io.Source

import com.google.gson.Gson

object Main extends App {

  val targetMovieId = 1196//Вариант 31
  val dataFilePath = "src/main/resources/u.data"
  val file = Source.fromFile(dataFilePath)
  val lines = file.getLines().toList

  def parseLine(line: String): Data = {
    val tokens: Seq[String] = line.split("\t")
    Data(
      userId = tokens(0).toLong,
      itemId = tokens(1).toLong,
      rating = tokens(2).toLong,
      timestamp = tokens(3).toLong
    )
  }
  val data: Seq[Data] = lines.map(parseLine)

  val (_,allRates) = data.groupBy(_.rating)
    .view.mapValues(_.size)
    .toSeq.sortBy(_._1)
    .unzip

  val targetMovieData = data.filter(d => d.itemId == targetMovieId)
  val target = targetMovieData.groupBy(_.rating).view.mapValues(_.size)
  val rates :Map[Long, Int] = Map(
    1L->0,
    2L->0,
    3L->0,
    4L->0,
    5L->0
  )
  val targetWithZeroes = rates ++ target.map{ case (k,v) => k -> (v + rates.getOrElse(k,0)) }
  val (_,targetRates) = targetWithZeroes.toSeq.sortBy(_._1).unzip
  case class Output(hist_film:Array[Int], hist_all:Array[Int])
  val output = Output(
    targetRates.toArray,
    allRates.toArray
  )
  val gson = new Gson
  val path = "output.json"
  println(gson.toJson(output))
  val fileWriter = new FileWriter(new File(path))
  fileWriter.write(gson.toJson(output))
  fileWriter.close()

}
