import util.Util.Row
import util.Util.Line
import TestTables.{ref_programmingLanguages1, ref_programmingLanguages2, table1, table2, table3, tableFunctional, tableImperative, tableObjectOriented}

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)

  def ||(other: FilterCond): FilterCond = Or(this, other)

  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName) match {
      case None => None
      case Some(_) => {
        println(colName + " " + r(colName).split("; ").toList.map(s => predicate.apply(s)).contains(true))
        if (r(colName).split("; ").toList.map(s => predicate.apply(s)).contains(true)) {
          Some(true)
        } else {
          Some(false)
        }
      }
    }
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
//    println(r)
//    println(f1)
    if (f1.eval(r).nonEmpty & f2.eval(r).nonEmpty) {
      if (f1.eval(r).get & f2.eval(r).get) {
        Some(true)
      } else {
        Some(false)
      }
    } else {
      None
    }
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if (f1.eval(r).nonEmpty & f2.eval(r).nonEmpty) {
      if (f1.eval(r).get || f2.eval(r).get) {
        Some(true)
      } else {
        Some(false)
      }
    } else {
      None
    }
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}

/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = {
    if (target.eval.isEmpty) {
      None
    } else {
      val a = target.eval.get
      if (columns.forall(a.getColumnNames.contains)) {
        Some(Table.apply(a.select(columns).get.toString))
      } else {
        None
      }
    }
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = {
    println("===" + condition)
    if (target.eval.isEmpty) {
      None
    } else {
      Some(Table.apply(target.eval.get.filter(condition).get.toString))
    }
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = {
    if (target.eval.isEmpty) {
      None
    } else {
      val a = target.eval.get
      Some(Table.apply(a.newCol(name, defaultVal).toString))
    }
  }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = {
    if (t1.eval.isEmpty || t2.eval.isEmpty) {
      None
    } else {
      val a = t1.eval.get
      val b = t2.eval.get
      val c = a.merge(key, b).get.getColumnNames
      val d = a.merge(key, b).get.getTabular.filter(_ != List()).distinct
      Some(new Table(c, d))
    }
  }
}


class Table(columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames: Line = columnNames

  def getTabular: List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val names = columnNames.mkString(",")
    val table = tabular.map(linie => linie.mkString(",")).mkString("\n")
    names ++ "\n" ++ table
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    if (!columns.forall(getColumnNames.contains)) {
      None
    } else {
      val a = getColumnNames
      val idxCol = columns.map(c => a.indexOf(c))
      val tb = getTabular
      val q = tb.map(line => idxCol.map(idx => line(idx)).mkString(",")) mkString ("\n")
      val str = columns.mkString(",") ++ "\n" ++ q
      Some.apply(Table.apply(str))
    }
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    println("---")
    println(cond)
    println(getColumnNames)
    val rowList = getTabular.map(linie => getColumnNames.map(elem => (elem, linie(getColumnNames.indexOf(elem)))).toMap)
    val a = rowList.foldLeft(Nil: List[Map[String, String]])((acc, elem) => {
      if (cond.eval(elem).contains(true)) acc ++ List(elem) else acc
    })
    if (a.nonEmpty) {
      val b = a.map(elem => getColumnNames.map(e => elem.get(e).mkString))
      val table = new Table(getColumnNames, b)
      Some.apply(table)
    } else {
      None
    }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val a = (getColumnNames ++ List(name)).mkString(",")
    val b = (getTabular.map(line => line.mkString(",") ++ "," ++ defaultVal).mkString("\n"))
    Table.apply(a ++ "\n" ++ b)
  }

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    if (getColumnNames.contains(key) & other.getColumnNames.contains(key)) {

      val tab1 = getTabular
      val tab2 = other.getTabular
      val tab1col = getColumnNames
      val tab2col = other.getColumnNames

      val finColNames = tab1col ++ (tab2col.diff(tab1col))

      val rowListTb1 = tab1.map(linie => tab1col.map(elem => (elem, linie(tab1col.indexOf(elem)))).toMap)
      val rowListTb2 = tab2.map(linie => tab2col.map(elem => (elem, linie(tab2col.indexOf(elem)))).toMap)

      val t1Keys = tab1.map(e => e(tab1col.indexOf(key)))
      val t2Keys = tab2.map(e => e(tab2col.indexOf(key)))

      val bothTables = t1Keys.intersect(t2Keys)
      val onlyT1 = t1Keys.diff(t2Keys)
      val onlyT2 = t2Keys.diff(t1Keys)


      val bothTablesRowT1 = bothTables.flatMap(name => rowListTb1.map(row => if (row(key).equals(name)) row else Nil)).filter(_ != Nil)
      val bothTablesRowT2 = bothTables.flatMap(name => rowListTb2.map(row => if (row(key).equals(name)) row else Nil)).filter(_ != Nil)


      val onlyT1Row = onlyT1.flatMap(name => rowListTb1.map(row => if (row(key).equals(name)) row else Nil)).distinct.filter(_ != Nil)
      val onlyT2Row = onlyT2.flatMap(name => rowListTb2.map(row => if (row(key).equals(name)) row else Nil)).distinct.filter(_ != Nil)

      val a1 = for (row <- bothTablesRowT1)
        yield {
          val idx = bothTablesRowT1.indexOf(row)
          val rowTb1 = row.toMap
          val rowTb2 = bothTablesRowT2(idx).toMap
          if (rowTb1.get(key) == rowTb2.get(key)) {
            val a = finColNames.map(name => {
              if (rowTb1.get(name).equals(rowTb2.get(name))) {
                rowTb1.get(name).mkString
              } else if (!rowTb1.contains(name)) {
                rowTb2.get(name).mkString
              } else if (!rowTb2.contains(name)) {
                rowTb1.get(name).mkString
              } else {
                rowTb1.get(name).mkString ++ ";" ++ rowTb2.get(name).mkString
              }
            })
            a
          } else {
            val c = finColNames.map(name => {
              if (rowTb1.contains(name)) {
                rowTb1.get(name).mkString
              } else {
                ""
              }
            })
            c
          }
        }

      val x = for (r <- onlyT1Row)
        yield {
          val row = r.toMap
          val f = finColNames.map(name => {
            if (row.contains(name)) {
              row.get(name).mkString
            } else {
              ""
            }
          })
          f
        }

      val y = for (r <- onlyT2Row)
        yield {
          val row = r.toMap
          val f = finColNames.map(name => {
            if (row.contains(name)) {
              row.get(name).mkString
            } else {
              ""
            }
          })
          f
        }

      Some(new Table(finColNames, a1 ++ x ++ y))

    } else {
      None
    }
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    new Table(s.split("\n").head.split(",").toList,
      s.split("\n").tail.map(linie => linie.split(",", -1).toList).toList)
  }


}
