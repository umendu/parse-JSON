package com.parse.json
/*import org.json4s.native.JsonMethods._
import org.json4s._*/
import org.json4s._
import org.json4s.jackson.JsonMethods._
object Sample {
  def main(args: Array[String]): Unit = {

    val json = """
  [
    {"name": "Foo", "emails": ["Foo@gmail.com", "foo2@gmail.com"]},
    {"name": "Bar", "emails": ["Bar@gmail.com", "bar@gmail.com"]}
  ]
"""
    val obj = parse(json).extract[List[User]](null,null)
    printf(obj.toString())
  //  printf("users: %s\n", obj.users.toString)
  }

  case class User(name: String, emails: List[String])
  case class UserList(users: List[User]) {
    override def toString(): String = {
      this.users.foldLeft("")((a, b) => a + b.toString)
    }
  }
}