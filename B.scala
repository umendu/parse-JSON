package com.parse.json
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.Extraction
//import com.google.gson.Gson
object B {
  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    println("hai..")
    case class Messages(var id: Int, var content: String)
    case class Model(var hello: String, var age: Int, var messages: Messages)
    val rawJson = """{"hello": "world", "age": 42,
	"messages": {"id":2,"content":"Forza Roma!"}}"""
    val m = parse(rawJson).extract[Model]
    m.age=23
    m.hello="reiiiiiiiiiiii"
    println(m.hello)
    val d = Extraction.decompose(m)
   // println(new Gson().toJson(m))
    //var s = new Model()
  }
}