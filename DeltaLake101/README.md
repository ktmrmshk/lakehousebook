# 入門 Delta Lake

## はじめに

[Delta Lake(デルタレイク)](https://delta.io)は、データレイクのストレージレイヤを掌り、論理的にデータウェアハウスの機能を実現するOSSです。Databricks社は、このDelta Lakeとデータガバナンス機能を提供するUnity Catalogをセットにして、レイクハウス(データレイクとデータウェアハウスのいいとこどり)というアーキテクチャを提唱し、そのマネージドクラウドサービスをメインで展開しています。

今回は、このDelta Lakeの機能と使い方を一通り見ていきます。Delta LakeはOSSであるため、Databricks上に加えて、JVM環境があればどの環境でも動作します。ここでは、OSSとして利用できる機能を中心に見ていきます。


## 前提とセットアップ

Databricksのワークスペース環境がある場合、Notebook上でそのままDelta Lakeのコードが実行できます。一方、ローカル環境など、Databricks以外の環境でDelta Lakeを実行する方法も見ていきます。最初に以下の条件が必要です。

* JVM実行環境
* Python3

Delta Lakeは[Apache Spark](https://spark.apache.org)と密接に連携しているため、今回もSpark環境ベースで進めます。シングルノード(例えば、あなたのLaptop上など)であれば、Spark(PySpark)は`pip`コマンドで簡単に整えることができます。また、分散ストレージ(HDFS, Object Storageなど)も不要です。

ここでは、`Ubuntu 22.04.3 LTS`を例にとって、セットアップしてみます。なお、Databricks環境が利用する場合は、次の節まで読み飛ばしてください。

まず、JavaとPython3が利用可能であることを確認します。

```bash
$ java -version
openjdk version "18.0.2-ea" 2022-07-19
OpenJDK Runtime Environment (build 18.0.2-ea+9-Ubuntu-222.04)
OpenJDK 64-Bit Server VM (build 18.0.2-ea+9-Ubuntu-222.04, mixed mode, sharing)

$ python --version
Python 3.10.12
```

それでは、`PySpark v3.5`とDelta Lakeをインストールします。

```bash
$ pip install pyspark==3.5 delta-spark==3.0.0
```

Python実行環境は、デフォルトのREPL(`$ python`と実行したときに立ち上がるインタラクティブなインタプリタUI)でもいいのですが、Jupyter Notebookの方がデバッグしやすいので、それもインストールしてしまいましょう。

```bash
$ pip install jupyterlab
```

では、早速jupyterのサーバを立ち上げて、notebook上でSparkとDelta Lakeの動作確認をしてみましょう。Delta Lakeのデータなどを保存・参照するための作業ディレクトリとして`/data`を作成し、その配下で作業していきます。

```bash
$ sudo mkdir /data
$ sudo chmod 777 /data
$ cd /data

$ jupyter-lab --ip="0.0.0.0" --NotebookApp.token=''
```

サーバを立ち上げたホストに`port:8888`に対してブラウザでアクセスします(ローカルの場合は`http://localhost:8888/`)。

新規でNotebookを作成し、以下のコマンドを実行します。

```python
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame([{'name': 'sato'}, {'name': 'tanaka'}, {'name': 'suzuki'}])
df.count()
df.show()

======= Result ========
+------+
|  name|
+------+
|  sato|
|tanaka|
|suzuki|
+------+
```

Sparkの動作確認は取れました。続いて、Delta Lakaについても確認します。

```python
df.write.format('delta').save('/data/test.delta')
spark.sql(' SELECT * FROM delta.`/data/test.delta` ').show()

======= Result ========
+------+
|  name|
+------+
|  sato|
|tanaka|
|suzuki|
+------+
```

上記の結果が得られたら環境のセットアップは完了です。


## Delta Lakeの基本操作

ここでは、CSVファイルを読み込んでDelta Lakeで保存したあと、SQLを使った基本操作をみていきます。

なお、データファイルを配置する作業ディレクトリとして`/data`ディレクトリを使用します。Databricksを使用する場合は、DBFS(`dbfs:/...`), Unity Catalog Volumesパス(例えば`s3://...`)を使うことが多いと思いますが、適宜パスを読み替えて読み進めてください。

### CSVファイルの読み込み

Delta LakeにデータをLoadする一番典型はCSVファイルの読み込みだと思います。まずはこちらからやっていきます。

CSVファイルはパブリックドメイン(CC0)になっているggplotの[diamonds.csv](https://github.com/tidyverse/ggplot2/blob/main/data-raw/diamonds.csv)を使用します。

```bash
[Shell]

$ cd /data
$ curl -O 'https://raw.githubusercontent.com/tidyverse/ggplot2/main/data-raw/diamonds.csv'

$ ls
diamonds.csv
```

このcsvファイルをSparkのデータフレームとして読み込みます。

```python
[Notebook/Python]

csv_schema = '''
carat double,
cut string,
color string,
clarity string,
depth double,
table double,
price double,
x double,
y double,
z double
'''

df = (
    spark.read.format('csv')
    .option('header', True)
    .schema(csv_schema)
    .load('/data/diamonds.csv')
)

df.limit(5).show()

====== Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|  Ideal|    E|    SI2| 61.5| 55.0|326.0|3.95|3.98|2.43|
| 0.21|Premium|    E|    SI1| 59.8| 61.0|326.0|3.89|3.84|2.31|
| 0.23|   Good|    E|    VS1| 56.9| 65.0|327.0|4.05|4.07|2.31|
| 0.29|Premium|    I|    VS2| 62.4| 58.0|334.0| 4.2|4.23|2.63|
| 0.31|   Good|    J|    SI2| 63.3| 58.0|335.0|4.34|4.35|2.75|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

ここではSparkデータフレームをこの記事に載せやくするために表示されるために`show()`を使っています。Databricks上では`display()`を使用できますので、グラフ可視化などが直感的にできるようになっています。

それでは、このSparkデータフレームをDelta Lake(Delta Lakeフォーマット)に保存します。

```python
[Notebook/Python]

df.write.format('delta').mode('overwrite').save('/data/diamonds.delta')
```

保存先のファイルを見てます。

```bash
[Shell]

$ tree /data/diamonds.delta

/data/diamonds.delta/
├── _delta_log
│   └── 00000000000000000000.json
└── part-00000-31182f1d-acd7-4f78-8908-6d0055ef2ad0-c000.snappy.parquet
```

上記の通り、Delta Lakeはテーブルデータをparquetファイルで、メタデータなどを含む管理データ(Delta Log)をjsonファイルで管理します。

それでは、この保存したDelta Lakeデータ(Deltaファイル)からデータフレームに読み込んでみましょう。

```python
[Notebook/Python]

df_delta = spark.read.format('delta').load('/data/diamonds.delta')
df_delta.limit(5).show()

======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|  Ideal|    E|    SI2| 61.5| 55.0|326.0|3.95|3.98|2.43|
| 0.21|Premium|    E|    SI1| 59.8| 61.0|326.0|3.89|3.84|2.31|
| 0.23|   Good|    E|    VS1| 56.9| 65.0|327.0|4.05|4.07|2.31|
| 0.29|Premium|    I|    VS2| 62.4| 58.0|334.0| 4.2|4.23|2.63|
| 0.31|   Good|    J|    SI2| 63.3| 58.0|335.0|4.34|4.35|2.75|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

ちゃんと読み込めているようです。
ここまではPythonベースで実行してきましたが、SparkはSQLも使うことができます。SQLを使う場合は、テーブル名として`` delta.`/data/diamonds.delta` ``というようにファイルパスを含んだ形での表現方法が使えます。`spark.sql()`はSQL文を引数に取り、結果をSparkデータフレームとして返してくれます。

```python
[Notebook/Python]

spark.sql('SELECT * FROM delta.`/data/diamonds.delta` LIMIT 5;').show()

======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|  Ideal|    E|    SI2| 61.5| 55.0|326.0|3.95|3.98|2.43|
| 0.21|Premium|    E|    SI1| 59.8| 61.0|326.0|3.89|3.84|2.31|
| 0.23|   Good|    E|    VS1| 56.9| 65.0|327.0|4.05|4.07|2.31|
| 0.29|Premium|    I|    VS2| 62.4| 58.0|334.0| 4.2|4.23|2.63|
| 0.31|   Good|    J|    SI2| 63.3| 58.0|335.0|4.34|4.35|2.75|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

同じ結果が得られます。

Delta LakeはPython(PySpark)のAPIも対応していますが、どうしてもテーブル処理の周辺はSQLの方が書きやすいため、そして、一部のDelta LakeのオペレーションはSQLベースになっているため、このあとはSQL多めになってきます。
Pythonでも実行可能なものはあるので、興味ある方はDelta Lakeの[ドキュメント](https://docs.delta.io/latest/index.html)を参照ください。


### 基本的なテーブル操作(更新系)

#### INSERT(Append)

まずはINSERT(Append)操作をPythonとSQLでやってみます。
Pythonでは、データフレームを作って、`append`モードで`save`します。

```Python
[Notebook/Python]

new_rec=[{'catat': 0.12, 'cut': 'Good', 'color': 'I', 'clarity': 'VS1', 'depth': 56.7, 'price': 123.0}]
df_tmp = spark.createDataFrame(new_rec)

df_tmp.write.format('delta').mode('append').save('/data/diamonds.delta')
```

確認してみます。

```python
[Notebook/Python]

spark.sql(' SELECT * FROM delta.`/data/diamonds.delta` ORDER BY carat LIMIT 5;').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.12|   Good|    I|    VS1| 56.7| NULL|123.0|NULL|NULL|NULL|
|  0.2|Premium|    E|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|    E|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
|  0.2|Premium|    E|    VS2| 59.8| 62.0|367.0|3.79|3.77|2.26|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

最初のレコードに追記されています。値を入れていなかった列については`NULL`になっています。

続いて、SQLでもINSERTしてみます。

```python
[Notebook/Python]

spark.sql('''

INSERT INTO delta.`/data/diamonds.delta`
(carat, cut, color, clarity, depth, table, price, x, y, z)
VALUES 
(0.15, 'Premium', 'I', 'SI2', 67.8, NULL, 234.5, NULL, NULL, NULL);

''')

spark.sql(' SELECT * FROM delta.`/data/diamonds.delta` ORDER BY carat LIMIT 5;').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.12|   Good|    I|    VS1| 56.7| NULL|123.0|NULL|NULL|NULL|
| 0.15|Premium|    I|    SI2| 67.8| NULL|234.5|NULL|NULL|NULL|
|  0.2|Premium|    E|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|    E|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

#### DELETE

レコードの削除です。

```python
spark.sql('''

DELETE FROM delta.`/data/diamonds.delta`
WHERE table is NULL

''')

spark.sql(' SELECT * FROM delta.`/data/diamonds.delta` ORDER BY carat LIMIT 5;').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|  0.2|Premium|    E|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|    E|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
|  0.2|Premium|    E|    VS2| 59.8| 62.0|367.0|3.79|3.77|2.26|
|  0.2|Premium|    E|    VS2| 59.7| 62.0|367.0|3.84| 3.8|2.28|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

先ほど追記したレコードが削除されています。


#### UPDATE / UPSERT

レコードをアップデートしてみます。例えば、`color`を`E => EEE`にしてみます。

```python
spark.sql(' UPDATE delta.`/data/diamonds.delta` SET color = "EEE" WHERE color = "E"')
spark.sql(' SELECT * FROM delta.`/data/diamonds.delta` ORDER BY carat LIMIT 5;').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|  0.2|Premium|  EEE|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|  EEE|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
|  0.2|Premium|  EEE|    VS2| 59.8| 62.0|367.0|3.79|3.77|2.26|
|  0.2|Premium|  EEE|    VS2| 59.7| 62.0|367.0|3.84| 3.8|2.28|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

UPDATEは現在のレコードを個別に更新する場面で使います。一方、元のテーブルの他に、追加リストのテーブルがある場合、レコードがすでにあればそのレコードを上書き(UPDATE)、もしレコードがなければ追加する(INSERT)ことがよく行われます。UPSERTです。UPSERTは`MERGE INTO`を使用して実行可能です。今回のDiamondsテーブルの場合、プライマリキー的な列がないため、ここでは[ドキュメント](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)に載っている例を示しておきます。

```SQL
MERGE INTO people10m
USING people10mupdates
ON people10m.id = people10mupdates.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT * 
```

### 集計系

Delta LakeのテーブルはSQLで操作できるため、通常のデータウェアハウスやデータベースと同じように集計処理が可能です。

```python
spark.sql('''
SELECT 
  cut,
  color,
  clarity,
  count(1) as count,
  median(price),
  stddev(price)
FROM delta.`/data/diamonds.delta`
GROUP BY cut, color, clarity
ORDER BY median(price) DESC
LIMIT 5;
''').show()
======= Results ========
+---------+-----+-------+-----+-------------+------------------+
|      cut|color|clarity|count|median(price)|     stddev(price)|
+---------+-----+-------+-----+-------------+------------------+
|     Good|    D|     IF|    9|      15081.0| 7144.444100138233|
|  Premium|    D|     IF|   10|      10129.0| 7617.155071868295|
|    Ideal|    J|     I1|    2|       9454.0|10018.288875851005|
|Very Good|    D|     IF|   23|       9182.0| 6579.462282653025|
|  Premium|    J|   VVS1|   24|       7284.5|5627.7253557141385|
+---------+-----+-------+-----+-------------+------------------+
```

ちなみに、Sparkデータフレームの関数として`summary()`が用意されており、基本的な統計値を出してくれます。

```python
spark.read.format('delta').load('/data/diamonds.delta').summary().show(truncate=6)
======= Results ========
+-------+------+------+-----+-------+------+------+------+------+------+------+
|summary| carat|   cut|color|clarity| depth| table| price|     x|     y|     z|
+-------+------+------+-----+-------+------+------+------+------+------+------+
|  count| 53940| 53940|53940|  53940| 53940| 53940| 53940| 53940| 53940| 53940|
|   mean|0.7...|  NULL| NULL|   NULL|61....|57....|393...|5.7...|5.7...|3.5...|
| stddev|0.4...|  NULL| NULL|   NULL|1.4...|2.2...|398...|1.1...|1.1...|0.7...|
|    min|   0.2|  Fair|    D|     I1|  43.0|  43.0| 326.0|   0.0|   0.0|   0.0|
|    25%|   0.4|  NULL| NULL|   NULL|  61.0|  56.0| 950.0|  4.71|  4.72|  2.91|
|    50%|   0.7|  NULL| NULL|   NULL|  61.8|  57.0|2401.0|   5.7|  5.71|  3.53|
|    75%|  1.04|  NULL| NULL|   NULL|  62.5|  59.0|5324.0|  6.54|  6.54|  4.04|
|    max|  5.01|Ver...|    J|   VVS2|  79.0|  95.0|188...| 10.74|  58.9|  31.8|
+-------+------+------+-----+-------+------+------+------+------+------+------+
```

テーブルのJOINやwith構文など、SQLを使ってよくやる処理はほぼ対応していると思われます。


### テーブルの履歴

Delta Lakeは、テーブルの変更履歴を内部(Delta Log)で持っているため、その参照も簡単に見れます。

```python
(
    spark.sql('desc history delta.`/data/diamonds.delta`')
    .select('version','timestamp', 'operation', 'operationParameters', 'readVersion')
    .show()
)
======= Results ========
+-------+--------------------+---------+--------------------+-----------+
|version|           timestamp|operation| operationParameters|readVersion|
+-------+--------------------+---------+--------------------+-----------+
|      4|2023-10-28 21:05:...|   UPDATE|{predicate -> ["(...|          3|
|      3|2023-10-28 21:01:...|   DELETE|{predicate -> ["i...|          2|
|      2|2023-10-28 20:57:...|    WRITE|{mode -> Append, ...|          1|
|      1|2023-10-28 20:45:...|    WRITE|{mode -> Append, ...|          0|
|      0|2023-10-28 19:37:...|    WRITE|{mode -> Overwrit...|       NULL|
+-------+--------------------+---------+--------------------+-----------+
```

対象のテーブルに対して、いつ、どのような変更があったのかがシンプルに追随できます。


### タイムトラベル

履歴が追えるということは、テーブルを以前の状態に戻せることも意味します。
Delta Lakeではこれをタイムトラベルという機能で提供しています。

テーブルのVersion、もしくは、タイムスタンプ指定で参照することができます。
Version指定のパターンから。

```python
# Version=1のテーブルを参照
spark.sql('''
    SELECT * FROM delta.`/data/diamonds.delta` VERSION AS OF 1
    ORDER BY carat LIMIT 5;
''').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.12|   Good|    I|    VS1| 56.7| NULL|123.0|NULL|NULL|NULL|
|  0.2|Premium|    E|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|    E|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
|  0.2|Premium|    E|    VS2| 59.8| 62.0|367.0|3.79|3.77|2.26|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

続いて、タイムスタンプ指定のパターンで見てみます。

```python
spark.sql('''
    SELECT * FROM delta.`/data/diamonds.delta` TIMESTAMP AS OF "2023-10-28 21:00"
    ORDER BY carat LIMIT 5;
''').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.12|   Good|    I|    VS1| 56.7| NULL|123.0|NULL|NULL|NULL|
| 0.15|Premium|    I|    SI2| 67.8| NULL|234.5|NULL|NULL|NULL|
|  0.2|Premium|    E|    VS2| 59.0| 60.0|367.0|3.81|3.78|2.24|
|  0.2|Premium|    F|    VS2| 62.6| 59.0|367.0|3.73|3.71|2.33|
|  0.2|Premium|    E|    VS2| 61.1| 59.0|367.0|3.81|3.78|2.32|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

EXCEPTと併用すると、Version間での差分レコードを抜き出せます。例えば、Version=2と0の変更レコードを見てみます。

```python
spark.sql('''
    SELECT * FROM delta.`/data/diamonds.delta` VERSION AS OF 2
    EXCEPT
    SELECT * FROM delta.`/data/diamonds.delta` VERSION AS OF 0
''').show()
======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.15|Premium|    I|    SI2| 67.8| NULL|234.5|NULL|NULL|NULL|
| 0.12|   Good|    I|    VS1| 56.7| NULL|123.0|NULL|NULL|NULL|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

途中でINSERT/Appendしたレコードが差分レコードとして確認できます。


### テーブルのCLONE


Delta Lakeは、これまでも何回か触れた通り、Parquet(データ本体)とJSON(メタデータ・ログ)を使って、差分データ管理をしながらテーブルデータの機能をファイルで提供しています。また、Delta Lakeはそのフォルダ内に自己完結なファイル構成になっています。

以上の状況を考えたときに、テーブルをコピー(複製)するにはどうしたら良いでしょうか?
最初に考えらるシンプルな方法は、

* Delta Lakeのディレクトリを丸ごとコピーする

ということでしょう。その通りです。これをDeep Clone(深いクローン)と呼びます。
ただし、この方法だと全てのデータをコピーしないといけないため、テーブルサイズが大きい場合、コストが高くなります。

もう少し考えて、Delta Lakeはテーブル更新するたびに差分データを積んでいくスタイルになっている点に注目すると、これまで(のVersion)のテーブルデータは既存のものを、今後更新されるデータは別フォルダに積んでいくことにすれば、分岐を作ることができ、論理的には2つのテーブルを独立させることができます。イメージ的には、gitでbranchを作るイメージです。

このように、これまでの履歴についてはオリジナルのDelta Lakeテーブルが持つデータをそのまま流用して、今後更新される部分のみを分岐させることでテーブルを複製することができます。これをShallow Clone(浅いクローン)と呼びます。この場合、Clone時にはデータのコピーは発生しないため、コストが軽くテーブルの複製ができます。

2つの使い分けとしては、テーブルの完全なバックアップ用途としてはDeep Cloneを、一時的にテーブルの時間的断面を切り出す時はShallow Cloneを使うのが一般的です。テーブルの時間的断面を切り出すとは、テーブルの状態が刻一刻と変化していく場合に、分析などの目的でテーブルを静的な状態にする目的で準備することを指します。

時間的断面の切り出しは、読み出しだけであれば、先ほどタイムトラベルを使えばCLONEしなくても良いのですが、テーブルに変更を加える場合にはShallow Cloneが必要になります。

#### Deep Clone

それでは、実際にClone操作をやってみましょう。
まずは、Deep Cloneは、ディレクトリをそのままコピーすれば終わりです。ただし、Deltaの更新日時についてはDeltaログファイルのファイルシステム上のタイムスタンプを参照するため、それらを保持してコピーする必要がります(`cp`コマンドの`-p`オプションが必須)。Databricks上では、テーブル操作の一貫性から`CREATE TABLE DEEP CLONE`が用意されていますので、そちらを利用ください。OSSでは現時点では利用できません。

```bash
[Shell]

$ cp -p -r /data/diamonds.delta /data/deep_clone_diamonds.delta
```

Deep Cloneしたテーブルが参照できるか一応確認してみます。

```python
[Notebook/Python]

spark.sql(' SELECT * FROM delta.`/data/deep_clone_diamonds.delta` LIMIT 5').show() 

======= Results ========
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
|carat|    cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|  Ideal|  EEE|    SI2| 61.5| 55.0|326.0|3.95|3.98|2.43|
| 0.21|Premium|  EEE|    SI1| 59.8| 61.0|326.0|3.89|3.84|2.31|
| 0.23|   Good|  EEE|    VS1| 56.9| 65.0|327.0|4.05|4.07|2.31|
| 0.29|Premium|    I|    VS2| 62.4| 58.0|334.0| 4.2|4.23|2.63|
| 0.31|   Good|    J|    SI2| 63.3| 58.0|335.0|4.34|4.35|2.75|
+-----+-------+-----+-------+-----+-----+-----+----+----+----+
```

テーブル履歴も見てみます。

```python
[Notebook/Python]

(
    spark.sql('desc history delta.`/data/deep_clone_diamonds.delta`')
    .select('version','timestamp', 'operation', 'operationParameters', 'readVersion')
    .show()
)

======= Results ========
+-------+--------------------+---------+--------------------+-----------+
|version|           timestamp|operation| operationParameters|readVersion|
+-------+--------------------+---------+--------------------+-----------+
|      4|2023-10-28 21:05:...|   UPDATE|{predicate -> ["(...|          3|
|      3|2023-10-28 21:01:...|   DELETE|{predicate -> ["i...|          2|
|      2|2023-10-28 20:57:...|    WRITE|{mode -> Append, ...|          1|
|      1|2023-10-28 20:45:...|    WRITE|{mode -> Append, ...|          0|
|      0|2023-10-28 19:37:...|    WRITE|{mode -> Overwrit...|       NULL|
+-------+--------------------+---------+--------------------+-----------+
```

#### Shallow Clone

続いてShallow Cloneを実践していきます。
SQLを使います。

```python
[Notebook/Python]

spark.sql('''
    CREATE TABLE delta.`/data/shallow_clone_diamonds.delta`
    SHALLOW CLONE delta.`/data/diamonds.delta`
''')
```

Shallow Cloneの場合には、Clone直後のディレクトリ構成はDelta Logファイルのみになっています。

```bash
[Shell]

$ tree shallow_clone_diamonds.delta
shallow_clone_diamonds.delta
└── _delta_log
    ├── 00000000000000000000.checkpoint.parquet
    ├── 00000000000000000000.json
    └── _last_checkpoint

1 directory, 3 files
```

テーブル参照とその後の操作時の挙動も見ていきましょう。

```python
[Notebook/Python]

(
    spark.sql('desc history delta.`/data/shallow_clone_diamonds.delta`')
    .select('version','timestamp', 'operation', 'operationParameters', 'readVersion')
    .show(truncate=False, vertical=True)
)

======= Results ========
-RECORD 0--------------------------------------------------------------------------------
 version             | 0                                                                 
 timestamp           | 2023-10-29 10:04:37.142                                           
 operation           | CLONE                                                             
 operationParameters | {source -> delta.`file:/data/diamonds.delta`, sourceVersion -> 4} 
 readVersion         | -1   
```

Version=0になり、Deltaログに参照元のテーブルが刻まれます。


### OPTIMIZE / Indexing(Z-Order)

Delta Lakeは更新を繰り返していくたびに差分データを追加していくため、parquetファイルが増えていきます。
細かいparquetファイルが大量にあると、読み込みに時間がかかり、結果としてパフォーマンスが低下します。

逆に一つだけ巨大なサイズのparquetが出来上がることもあり、無駄なファイルを読み込まないことでパフォーマンスを発揮させる(File Pruning/Data Skippingと呼ばれる機能)Delta Lakeのパフォーマンスを低下させます。

この問題を解消するための操作が`OPTIIZE`で、実行することでparquetファイルを最適なサイズに変換してくれます(Bin Packing, Compactionと呼ばれます)。

この`OPTIMIZE`に加えて、特定のカラムの値がまとまるようにデータを並べ替えてparquetを構成する`Z-Order`も利用可能になっています。こちらは通常のデータベースのIndexingに対応する操作になります。

それでは、`OPTIMIZE`(単体)と`Z-Oerder`をやってみましょう。
`OPTIMIZE`は単純です。

```python
spark.sql(' OPTIMIZE delta.`/data/diamonds.delta` ')
```

`Z-Oerder`は、並べ替えるカラム(列)を指定します。複数指定できます。

```python
spark.sql('OPTIMIZE delta.`/data/diamonds.delta` ZORDER BY (carat, price)')
```

なお、Z-orderの改良版であるLiquid Clusteringですが、Databricks上では利用できますが、OSSのDelta Lake v3.0.0(今回使用している版)ではまだリリースされておりません。時期に利用可能になる予定のため、次回の機会に残しておきます。


## まとめ

いかがでしたでしょうか。今回は、Delta Lakeでできる基本的な操作に関してみてきました。今回触れられなかったChange Data Feed, スキーマエボリューション、Liquid Clustering、UniForm、テーブル制約、Deletion Vectorに関しては次回の記事で取り上げたいと思います。また、これらの機能はDelta Lakeのログ仕様と密接に関わっており、これらを把握することで理解しやすくなりますので、この点に関しても触れていきたいと思います。


## 参考

* 今回のコードのGit Repo => `https://github.com/ktmrmshk/lakehousebook`


