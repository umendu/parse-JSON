package com.parse.json
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
object Test {
  def main(args: Array[String]): Unit = {
     val JSONString = """
      {
         "name":"luca",
         "id": "1q2w3e4r5t",
         "age": 26,
         "url":"http://www.nosqlnocry.wordpress.com",
         "loginTimeStamps": [1434904257,1400689856,1396629056],
         "messages": [
          {"id":1,"content":"Please like this post!"},
          {"id":2,"content":"Forza Roma!"}
         ],               
         "profile": { "id":"my-nickname", "score":123, "avatar":"path.jpg" }
      }
      """
     
      val JSONString1 = """
      {
         "name":"luca",
         "id": "1q2w3e4r5t",
         "age": 26,
         "url":"http://www.nosqlnocry.wordpress.com",
         "loginTimeStamps": [1434904257,1400689856,1396629056],
         "messages": {"id":2,"content":"Forza Roma!"}
      }
      """
      val JSON = parse(JSONString)
      println(JSON\"messages"\"content")
  }
}