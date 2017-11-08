package RDDOpreation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// 网站参考：http://blog.csdn.net/yxgxy270187133/article/details/42026259
object RDDOperation {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)

    println("==aggregate========================================")
    val aggregate1 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    println(aggregate1.aggregate(0)(math.max, _ + _))

    val aggregate2 = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)
    println(aggregate2.aggregate("")(_ + _, _ + _))
    println(aggregate2.aggregate("x")(_ + _, _ + _))

    val aggregate3 = sc.parallelize(List("12", "23", "345", "4567"), 2)
    println(aggregate3.aggregate("")((x, y) => math.max(x.length, y.length).toString, (x, y) => x + y))
    println(aggregate3.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y))

    val aggregate4 = sc.parallelize(List("12", "23", "345", ""), 2)
    println(aggregate4.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y))

    val aggregate5 = sc.parallelize(List("12", "23", "", "345"), 2)
    println(aggregate5.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y))

    println("==cartesian========================================")
    val cartesianx = sc.parallelize(List(1, 2, 3, 4, 5))
    val cartesiany = sc.parallelize(List(6, 7, 8, 9, 10))
    cartesianx.cartesian(cartesiany).collect.foreach(ele => print(ele._1, ele._2))
    println

    println("==cogroup [Pair], groupWith[Pair]========================================")
    val cogroupa = sc.parallelize(List(1, 2, 1, 3), 1)
    val cogroupb = cogroupa.map((_, "b"))
    val cogroupc = cogroupa.map((_, "c"))
    cogroupb.cogroup(cogroupc).collect.foreach(ele => print(ele._1, ele._2))
    println

    val cogroupd = cogroupa.map((_, "d"))
    cogroupb.cogroup(cogroupc, cogroupd).collect.foreach(ele => print(ele._1, ele._2))
    println

    val cogroupx1 = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
    val cogroupy1 = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
    cogroupx1.cogroup(cogroupy1).collect.foreach(ele => print(ele._1, ele._2))
    println

    println("==collect,toArray========================================")
    val collecta = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    collecta.collect.foreach(ele => print(ele + " "))
    println


    println("==collectAsMap [Pair]========================================")
    val collectAsMapa = sc.parallelize(List(1, 2, 1, 3), 1)
    val collectAsMapb = collectAsMapa.zip(collectAsMapa)
    println(collectAsMapb.collectAsMap)

    println("==combineByKey[Pair]========================================")
    val combineByKeya = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val combineByKeyb = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val combineByKeyc = combineByKeyb.zip(combineByKeya)
    val combineByKeyd = combineByKeyc.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    combineByKeyd.collect.foreach(ele => print(ele._1, ele._2))
    println

    println("==countByKey [Pair]========================================")
    val countByKeyc = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
    println(countByKeyc.countByKey)

    println("==countByValue [Pair]========================================")
    val countByValueb = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1, 1, 1))
    println(countByValueb.countByValue)

    println("==countApproxDistinct========================================")
    val countApproxDistincta = sc.parallelize(1 to 10000, 20)
    val countApproxDistinctb = countApproxDistincta ++ countApproxDistincta ++ countApproxDistincta ++ countApproxDistincta ++ countApproxDistincta
    println(countApproxDistinctb.countApproxDistinct(0.1))
    println(countApproxDistinctb.countApproxDistinct(0.05))
    println(countApproxDistinctb.countApproxDistinct(0.01))
    println(countApproxDistinctb.countApproxDistinct(0.001))


    println("==countApproxDistinctByKey [Pair]========================================")
    val countApproxDistinctByKeya = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
    val countApproxDistinctByKeyb = sc.parallelize(countApproxDistinctByKeya.takeSample(withReplacement = true, 10000, 0), 20)
    val countApproxDistinctByKeyc = sc.parallelize(1 to countApproxDistinctByKeyb.count().toInt, 20)
    val countApproxDistinctByKeyd = countApproxDistinctByKeyb.zip(countApproxDistinctByKeyc)
    println(countApproxDistinctByKeyd.countApproxDistinctByKey(0.1).collect.foreach(ele => print(ele._1, ele._2)))
    println(countApproxDistinctByKeyd.countApproxDistinctByKey(0.01).collect.foreach(ele => print(ele._1, ele._2)))
    println(countApproxDistinctByKeyd.countApproxDistinctByKey(0.001).collect.foreach(ele => print(ele._1, ele._2)))

    println("==dependencies========================================")
    val dependenciesb = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1, 1, 1))
    println(dependenciesb.dependencies.length)
    println(dependenciesb.map(dependenciesa => dependenciesa).dependencies.length)

    println("==distinct========================================")
    val distinctc = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    distinctc.distinct.collect.foreach(ele => print(ele + " "))
    println
    val distincta = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    println(distincta.distinct(2).partitions.length)
    println(distincta.distinct(3).partitions.length)

    println("==first========================================")
    val firstc = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
    println(firstc.first)

    println("==filter========================================")
    val filtera = sc.parallelize(1 to 10, 3)
    val filterb = filtera.filter(_ % 2 == 0)
    filterb.collect.foreach(ele => print(ele + " "))
    println

    val filterb1 = sc.parallelize(1 to 8)
    filterb1.filter(_ < 4).collect.foreach(ele => print(ele + " "))
    println

    val filtera2 = sc.parallelize(List("cat", "horse", 4.0, 3.5, 2, "dog"))
    filtera2.collect({ case a: Int => "is integer" case b: String => "is string" }).collect.foreach(ele => print(ele + "|"))
    println

    val filtermyfunc: PartialFunction[Any, Any] = {
      case a: Int => "is integer"
      case b: String => "is string"
    }
    println(filtermyfunc.isDefinedAt(""))
    println(filtermyfunc.isDefinedAt(1))
    println(filtermyfunc.isDefinedAt(1.5))

    println("==flatMap========================================")
    val flatMapa = sc.parallelize(1 to 10, 5)
    flatMapa.flatMap(1 to _).collect.foreach(ele => print(ele + "|"))
    println
    sc.parallelize(List(1, 2, 3), 2).flatMap(x => List(x, x, x)).collect.foreach(ele => print(ele + "|"))
    println
    val flatMapx  = sc.parallelize(1 to 10, 3)
    flatMapx.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect.foreach(ele => print(ele + "|"))
    println

    println("==flatMapValues========================================")
    val flatMapValuesa = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther","eagle"), 2)
    val flatMapValuesb = flatMapValuesa.map(x => (x.length, x))
    flatMapValuesb.flatMapValues("x" + _ + "x").collect.foreach(ele => print(ele + " "))

    println("==fold========================================")
    val folda = sc.parallelize(List(1,2,3), 3)
    println(folda.fold(0)(_ + _))

    println("==foldByKey [Pair]========================================")
    val foldByKeya = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val foldByKeyb = foldByKeya.map(x => (x.length, x))
    foldByKeyb.foldByKey("")(_ + _).collect.foreach(ele => print(ele + " "))
    println

    val foldByKeya1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther","eagle"), 2)
    val foldByKeyb1 = foldByKeya1.map(x => (x.length, x))
    foldByKeyb1.foldByKey("")(_ + _).collect.foreach(ele => print(ele + " "))
    println

    println("==foreach========================================")
    val foreachc = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu","crocodile", "ant", "whale", "dolphin", "spider"), 3)
    foreachc.foreach(x => println(x + "s are yummy"))

    println("==foreachPartition========================================")
    val foreachPartitionb = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    foreachPartitionb.foreachPartition(x => println(x.sum))

    println("==getStorageLevel========================================")
    val getStorageLevela = sc.parallelize(1 to 100000, 2)
    getStorageLevela.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
    println(getStorageLevela.getStorageLevel.description)
//    getStorageLevela.cache  //Cannot change storage level of an RDD after it was already assigned a level

    println("==glom========================================")
    val gloma = sc.parallelize(1 to 100, 3)
    gloma.glom.collect.foreach(ele => ele.foreach(x => print(x + " ")))
    println

    println("==groupBy========================================")
    val groupBya = sc.parallelize(1 to 9, 3)
    groupBya.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect.foreach(x => print(x + " "))
    println

    val groupBya1 = sc.parallelize(1 to 9, 3)
    def myfunc(a: Int) : Int =
    {
      a % 2
    }
    groupBya1.groupBy(myfunc).collect.foreach(x => print(x + " "))
    println

    val groupBya2 = sc.parallelize(1 to 9, 3)
    groupBya2.groupBy(myfunc(_), 1).collect.foreach(x => print(x + " "))
    println

    println("==groupByKey [Pair]========================================")
    val groupByKeya = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider","eagle"), 2)
    val groupByKeyb = groupByKeya.keyBy(_.length)
    groupByKeyb.groupByKey.collect.foreach(x => print(x + " "))
    println

    println("==histogram [Double]========================================")
    val histograma = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8,9.0), 3)
    histograma.histogram(5)._1.foreach(x => print(x + " "))
    println
    histograma.histogram(5)._2.foreach(x => print(x + " "))
    println

    val histograma1 = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1,7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
    histograma1.histogram(6)._1.foreach(x => print(x + " "))
    println
    histograma1.histogram(6)._2.foreach(x => print(x + " "))
    println


    val histograma2 = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8,9.0), 3)
    histograma2.histogram(Array(0.0, 3.0, 8.0)).foreach(x => print(x + " "))
    println

    val histograma3 = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1,7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
    histograma3.histogram(Array(0.0, 5.0, 10.0)).foreach(x => print(x + " "))
    println
    histograma3.histogram(Array(0.0, 5.0, 10.0, 15.0)).foreach(x => print(x + " "))
    println

    println("==id========================================")
    val idy = sc.parallelize(1 to 10, 10)
    println(idy.id)

    println("==intersection========================================")
    val intersectionx = sc.parallelize(1 to 20)
    val intersectiony = sc.parallelize(10 to 30)
    val intersectionz = intersectionx.intersection(intersectiony)
    intersectionz.collect.foreach(x => print(x + " "))
    println

    println("==join[Pair]========================================")
    val joina = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"),3)
    val joinb = joina.keyBy(_.length)
    val joinc =sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3)
    val joind = joinc.keyBy(_.length)
    joinb.join(joind).collect.foreach(x => print(x + " "))
    println

    println("==keyBy========================================")
    val keyBya = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"),3)
    val keyByb = keyBya.keyBy(_.length)
    keyByb.collect.foreach(x => print(x + " "))
    println

    println("==keys [Pair]========================================")
    val keysa = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther","eagle"), 2)
    val keysb = keysa.map(x => (x.length, x))
    keysb.keys.collect.foreach(x => print(x + " "))
    println
    keysb.values.collect.foreach(x => print(x + " "))
    println

    println("==leftOuterJoin [Pair]========================================")
    val leftOuterJoina = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"),3)
    val leftOuterJoinb = leftOuterJoina.keyBy(_.length)
    val leftOuterJoinc =sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3)
    val leftOuterJoind = leftOuterJoinc.keyBy(_.length)
    leftOuterJoinb.leftOuterJoin(leftOuterJoind).collect.foreach(x => print(x + " "))
    println

    println("==lookup========================================")
    val lookupa = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther","eagle"), 2)
    val lookupb = lookupa.map(x => (x.length, x))
    println(lookupb.lookup(5))

    println("==map========================================")
    val mapa = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"),3)
    val mapb = mapa.map(_.length)
    val mapc = mapa.zip(mapb)
    mapc.collect.foreach(x => print(x + " "))
    println

    println("==mapPartitions========================================")

    sc.stop()
  }
}
