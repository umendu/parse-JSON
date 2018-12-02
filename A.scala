package com.parse.json
//import com.google.gson.Gson
object A {
  def main(args: Array[String]): Unit = {
    case class Person(name: String, address: Address)
    case class Address(city: String, state: String)
    val p = Person("Alvin Alexander", Address("Talkeetna", "AK"))
    // create a JSON string from the Person, then print it
  //  val gson = new Gson
  //  val jsonString = gson.toJson(p)
  //  println(jsonString)
  }
}