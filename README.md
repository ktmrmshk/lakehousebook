# Understanding Lakehouse in practice - from CSV to レイクハウス

## はじめに

この本を手に取った方であれば、仕事や趣味などで「データを扱う」ということを何かしらされている場合が多いと思います。いやいや、そもそも一般に生きていれば「データ」は溢れているし、「データ」に触れないで、意識しているかしていないかを除けば、完全に「データ」から逃れて生活することは難しいと思います。例えば、出かけるときに服装や持ち物をどうしようかと「天気予報」を見て決めるし、晴れの天気予報だけど最近は朝晴れている場合、職場の周りは夕立が多く、折り畳み傘を持って行った方がいい、などというわけです。ここでは、「天気予報」に加えて、今までの経験値が「データ」として予測に使われているわけです。

ということで、「データを扱う」というのは、もはや壮大なテーマであり、一概にこういう方法やツールで完全に対応できるという話ではないわけです。ではどうするかというと、「データを扱う」人が適切に考えて、管理し、操作することが唯一の解なのかなと思います。その意味で、ここでは、特に現在のITシステムやAI(人工知能)周りで問題になりやすい3Vな「データ」を扱うプラクティスを実際に体感しながら、「データを扱う上での勘所が感覚として解る」というのを目標に話を進めていきたいと思います。

私も含め、一般の人間は最初に頭で考えるよりも、まずは、体感して、感覚的に身につけ、それから頭を使って汎化し、一般化知識にしていく、というのがうまくいくことが多いと思い気がします。その立場で、テキスト上の数値データであるCSVファイルから初めて、昨今新しいデータアーキテクチャである「レイクハウス」を理解するところまでを見ていきます。

「レイクハウス」はデータを効率的・効果的に扱うためのプラクティスから生まれた「考え方」であり、何かのツールやベンダ固有の製品・サービスではありません。そのため、ここで目標にしている「体感的にわかる」状態になれば、皆さんの好きなツールやサービスで実現・構成することが可能です。ただし、具体例がないと「体感的に」分かりづらい部分がありますので、今回の説明では、以下のオープンソースのフレームワークを使用します。プレーンなLinux環境から始めて、実際のコードも載せますので、実行しながら体感してください。

* 計算機(+物理ストレージ): Linux (Ubuntu) (+ローカルディスク)
* 処理エンジン: Apache Spark
* ストレージエンジン: Deltalake

それでは、早速いきましょう。

## データを扱う

漠然としてますが、皆さんは「データ」といったら何を思い浮かべますか?色々な「データ」があると思います。例えば、「過去１年間分の天気予報」、「電車の運行ダイヤ」、「スマホで取ってきた写真」、「ビデオや音楽」、「ブログ記事」、「株価」、「円周率」、「Webサイトのアクセスログ」、「レシート・購入履歴」など、いろいろあると思います。これらのデータを分類すると大きく2つに分類できると思います。

* **構造化データ** データの形式が完全に固定されているもの
* **非構造化データ** 構造化データではないもの

例えば、「過去１年間分の天気予報」は、前者の構造化データになりと思います。天気予報は、おそらく、日時、場所、天気(晴れ、雨など)などが含まれるでしょう。これらのデータは固定的に扱うことができます。日時であれば、「2023-04-01」と形式化できますし、場所についても、緯度軽度の数値(小数点)、天気は固定的な文字列の中から選ばれるようになります。このように、データの形式が固定的になるので、構造化データとして捉えることが可能です。

一方で、「ビデオや音楽」はどうでしょうか? 一言で言いづらいかもしれませんが、写真は画像データであり、ファイルフォーマット、解像度、サイズなどがバラバラである可能性が高いですね。データの形式が固定的になりずらいため、非構造化データになると思います。

それでは、それ以外に例で挙げたデータがどちらになるか、考えてみてください。私は以下のように分類しまいた。

* 構造化データ
  * 電車の運行ダイヤ (列車の識別番号、駅名、到着時刻、出発時刻)
  * 株価 (日付、銘柄、株価)
  * 円周率 (数値)
  * 購入履歴 (日時、商品、個数、単価、支払い金額)
  * webサイトのアクセスログ (日時、アクセス元IP、ページ、レスポンスコード、エラーメッセージ)

* 非構造化データ
  * ブログ記事(タイトル、本文、写真などが混ざったドキュメント)
  * レシート(購入記録などがカスタムのレイアウトで表現される)

ここで、「おやっ!?」と思った方もいるかと思います。例えば、ブログ記事については、タイトル(文字列)、本文(文字列)、画像(バイナリ形式)とすれば構造化データと見えるし、実際多くのブログサイトシステム(CMS)では構造化データを扱うRDBMS(MySQLやPostgresSQLなど)にデータを蓄積する構成を取っていたりします。

そうです。構造化データと非構造化データの区切りは、実際には曖昧になっています。データをシステムで扱う上では、「テーブル」構成になっているかが一つの区切りになっています。構造化データは、テーブル構造をとることができ、かつ、その属性(カラム、列)が固定されたデータ形式で定義できるものを指します。

ブログ記事を考えると、ブログサイト全体として、タイトル(文字列)、本文(文字列)、添付データ(バイナリ)という3つのカラムを用意して管理されていれば、それは構造化データといえます。逆に、ウェブ上のブログサイトからスクレイピングしてきたような状態は、単に整理されていない文字列(例えばHTMLやDOMデータ)であるので、非構造データになります。

特に、構造化データは管理して意味がある状態のテーブルであり、一般的には、飛行増加データを整理していくと構造化データになっていきます。

例えば、webアクセスログなども、通常は、web serverから測れた状態では、単なる文字列ですが、そこからパースして、意味があるテーブルデータにしていくことが一般的な流れになっています。

-- 時間があれば、web logのパースを見ていく。

## テーブルデータとCSV

ここからは構造化データ、つまりテーブルデータを見ていきます。みなさんがテーブルデータを扱う際に使用するツールやファイルフォーマットはどんなものがありますか?一番メジャー(?)なツールは表計算ソフト(ExcelやSpreadsheetなど)で、ファイルフォーマットはCSV(コンマ区切りでデータが並んでいる文字列ファイル)だと思います。

実は、みなさんもご存知の通り、他にも多くのツールやファイルフォーマットが存在します。
それでは、なぜ、CSVファイルが多く使われるのでしょうか?
理由はいくつかあると思いますが、端的にいうと「多くの人にとって使いやすい、わかりやすい」からというのがあると思います。CSVは、テキストエディタでも編集できるし、各種ツールでもimport/exportがサポートされている現実もあると思います。

例えば、ブログサイトのエントリーを管理しているCSVファイルを見てみましょう。

** ファイル: `my_blog.csv` **
```csv
タイトル,カテゴリ,評価,公開日時
山手線一周,旅行,3.4,2023-04-01
データ分析入門,分析,4.3,2023-03-02
昨日のご飯,生活,3,2023-02-14
```

テキストファイルなので、見やすく、編集もしやすいですね。
テーブルデータを扱うには、CSV一択でいい気もします。しかし、皆さんもお気づきのように、CSVファイルには多くの弱点があります。以下にいくつか挙げてみます。

1. 編集ミスなどで、データが壊れやすくなる
1. レコード数、カラム数が膨大になると、もはや可読性が低くなる
2. 文字列なので、各カラムのデータ型に厳密性がない
3. テキストファイルなので、圧縮されておらず、ファイルサイズが大きくなる
4. (レコード数が大規模になると)集計処理が遅い
5. 複数人で同時に編集した際に、データが壊れる可能性がある
6. いつ、誰が、どの部分のデータを改変したのかがわからない

一人で趣味程度に管理しているデータであれば問題ないかもしれませんが、業務や本番システムで使うには、非常に心許無いのです。これらを解決するのが、データベースシステムなのです。


## データベースシステム

データベースは、先ほど挙げたCSVファイルの弱点を全て克服する性能・機能を持っています。
ここでは主にテーブルデータを扱うのに特化したリレーショナルデータベース(Relational Data Base Management System; RDBMS)を想定します。MySQLやSQLiteなどをイメージしていただければ問題ありません。

これらのデータベースでは、データをテーブルとして使用する際に、カラムとそのデータ型(これをスキーマと呼びます)を厳密に定義して、その後データを挿入(投入)していくスタイルを取ります。先にデータ型を定義しておくことで、汚れたデータを入り口で弾くようなフィルターとして機能させることができるようになっています。「日時型」のカラムに数値を入れようとするとエラーで弾かれます。このように、データを書く前にスキーマを定義しておく方式をスキーマ・オン・ライト(Schema on write)と呼びます。

それでは、SQLiteで先ほどのCSVで表現したテーブルを作成したいと思います。
SQLiteはローカルで軽量に動く、サーバが不要、Pythonに標準で組み込まれている、データタイプが5種類しかなく極めてシンプル、という理由から、個人的に結構好きなデータベースです。

```sql
CREATE TABLE my_blog (
    `タイトル` TEXT,
    `カテゴリ` TEXT,
    `評価` REAL,
    `公開日時`　DATE
);

INSERT INTO my_blog VALUES
("山手線一周","旅行", 3.4, "2023-04-01"),
("データ分析入門","分析", 4.3, "2023-03-02"),
("昨日のご飯","生活", 3, "2023-02-14");
```

最後に、上記で作成したテーブルを表示させてみましょう。

```sql
.mode column -- 表示モードを整える
.headers on -- ヘッダーを表示される

SELECT * FROM my_blog;

タイトル     カテゴリ  評価   公開日時      
-------  ----  ---  ----------
山手線一周    旅行    3.4  2023-04-01
データ分析入門  分析    4.3  2023-03-02
昨日のご飯    生活    3.0  2023-02-14
```

このようにデータをきっちり収める場合には、確かにこちらの方が適しているようにも思えます。
しかし、実際に運用してみると、文脈によっては、意外にこの方式だとメンドーなこともあったりします。例えば、データ型は意外に運用しているうちに変更されることがあったり、カラムが追加になったり、データベースに入れるデータ量が大きすぎて`INSERT`処理に時間がかかったりと。

また、構造化される前にデータや、そもそも画像データのように非構造なデータも一緒に扱いたい場合も出てきます。こうした、そこまで「きっちり」しなくてもいい、むしろ、データベースのような厳密性は若干かけるけど、柔軟にデータを管理したい場合には、「データ」をそのままファイルとして扱っておいて、そのデータが必要になったタイミングで、構造化処理を実施すれば良いのではないか、という考えに行き着きます。これがデータレイクの考え方です。

## データレイクとスキーマ・オン・リード

データレイクは、先ほど書いたように、データを「テーブル」ではなく、「ファイル」として置いておくための保存場所です。データレイクが登場した背景は、データベースが持つスキーマ・オン・ライト方式の課題解決に加えて、ストレージが安価になり、大規模なファイルを保存して置いても、そこまで多くのお金がかからなくなった、という理由もあります。主に使用されるのは、オブジェクトストレージ(AWS S3やAzure Blogストレージなど)です。

例えば、Webサーバのアクセスログ(gzipされたテキストデータ)をオブジェクトストレージ上に適当にフォルダを切って、置いておけば、それがもうデータレイクとも言えます。と、ここで、じゃあ単なるデータレイクとストレージは変わらないじゃないか、と思ったと思います。

そうです。データレイクには、もう一つ重要なそうそがあります。それは、先ほど書いた通り「データが必要になったタイミングで構造化処理を実施する」機能です。この機能を実現するツールは色々あります。有名なのが、Hadoop(Hive)、Presto、Sparkあたりでしょうか。データの構造化処理ができればいいので、pandasなどでも実施は可能だと思います。ここでは、Apache Sparkを使ってデータレイクの処理を見ていきたいと思います。

それでは、Apache SparkをUbuntuにインストールして、起動し、先ほどのCSVファイルを操作してみましょう。

```shell
### Ubuntu 22.04.2 LTS
$ sudo apt install openjdk-11-jre-headless
$ sudo apt install python-is-python3 python3-pip
$ pip install pyspark==3.3.2
### Shellのリロード
$ pyspark --version
version 3.3.1
```

先ほどのファイルを絶対パスからアクセスしやすい位置に置いておきます。

```sh
$ sudo mkdir /lake
$ sudo chmod 777 /lake
$ mv my_blog.csv /lake/
$ ls -l /lake
total 4
-rw-rw-r-- 1 ubuntu ubuntu 165 May  2 11:16 my_blog.csv
```

また、少し構造化処理を加えるので、レコードを足しておきたいと思います。同じディレクトリに、別ファイルとして以下のCSVも作っておきます。

```sh
### my_blog_002.csvを以下の内容で作成しておく
$ cat /lake/my_blog_002.csv

タイトル,カテゴリ,評価,公開日時
東武日光線往復,旅行,4.8,2023-03-25
Spark入門,分析,2.9,2023-04-10
明日の運動,生活,1.9,2023-03-09

$ ls -l /lake/
total 8
-rw-rw-r-- 1 ubuntu ubuntu 164 May  2 11:25 my_blog.csv
-rw-rw-r-- 1 ubuntu ubuntu 162 May  2 11:24 my_blog_002.csv
```

それではSpark(`pyspark`)を立ち上げて、CSVファイルを調理していきたいと思います。

```python
$ pyspark
...
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.1
      /_/

Using Python version 3.8.10 (default, Mar 13 2023 10:26:41)
Spark context Web UI available at http://orarm20220116.subnet11231223.vcn11231223.oraclevcn.com:4040
Spark context available as 'sc' (master = local[*], app id = local-1683016297284).
SparkSession available as 'spark'.

### データフレームで２つのCSVファイルを読み込む
>>> df = (
        spark.read.format('csv')
        .option('inferSchema', True)
        .option('Header', True)
        .load('/lake/my_blog*.csv')
    ）

### データフレームの表示
>>> df.show()
+--------------+--------+----+----------+
|      タイトル|カテゴリ|評価|  公開日時|
+--------------+--------+----+----------+
|    山手線一周|    旅行| 3.4|2023-04-01|
|データ分析入門|    分析| 4.3|2023-03-02|
|    昨日のご飯|    生活| 3.0|2023-02-14|
|東武日光線往復|    旅行| 4.8|2023-03-25|
|     Spark入門|    分析| 2.9|2023-04-10|
|    明日の運動|    生活| 1.9|2023-03-09|
+--------------+--------+----+----------+

### データフレームのスキーマを確認
>>> df.printSchema()
root
 |-- タイトル: string (nullable = true)
 |-- カテゴリ: string (nullable = true)
 |-- 評価: double (nullable = true)
 |-- 公開日時: date (nullable = true)
```

Sparkを使って、データフレームに2つのCSVファイルを読み込んでみました。データフレームとは、テーブル形式のデータで、Pandas, R, Sparkなどで用いられるデータ概念です。ここでは、そのまま「テーブルデータを持つ変数」と読み替えても問題ありません。

元のデータは単なるテキストの「CSV」であったはずなのに、上記の結果をみると、ちゃんとしたスキーマ情報をもったテーブルデータになっています。スキーマ情報はどこで与えたのでしょうか。実は、`option('inferSchema', True)`オプションがそれをやってます。オプション名からわかる通り、これはあくまでスキーマを推測する機能ですので、本番システムなどでは、手動でスキーマを定義するのが一般的です。何はともあれ、ここで重要なのは、データ自体はCSVなどで保存しておいて、そのデータを使用するときに(読み込むときに)、スキーマを当てるという考え方を採用しているという点です。これをスキーマ・オン・リードといいいます。

また、スキーマが定義されているので、集約処理なども自由に、かつ、高速にできます。

```python
>>> df.orderBy('公開日時').show()
+--------------+--------+----+----------+
|      タイトル|カテゴリ|評価|  公開日時|
+--------------+--------+----+----------+
|    昨日のご飯|    生活| 3.0|2023-02-14|
|データ分析入門|    分析| 4.3|2023-03-02|
|    明日の運動|    生活| 1.9|2023-03-09|
|東武日光線往復|    旅行| 4.8|2023-03-25|
|    山手線一周|    旅行| 3.4|2023-04-01|
|     Spark入門|    分析| 2.9|2023-04-10|
+--------------+--------+----+----------+


>>> df.count()
6

>>> df.summary().show()
+-------+--------------+--------+------------------+
|summary|      タイトル|カテゴリ|              評価|
+-------+--------------+--------+------------------+
|  count|             6|       6|                 6|
|   mean|          null|    null| 3.383333333333333|
| stddev|          null|    null|1.0419532938988514|
|    min|     Spark入門|    分析|               1.9|
|    25%|          null|    null|               2.9|
|    50%|          null|    null|               3.0|
|    75%|          null|    null|               4.3|
|    max|東武日光線往復|    生活|               4.8|
+-------+--------------+--------+------------------+

### Sparkの場合、SQLも並行して使えます
>>> df.createOrReplaceTempView('my_blog')
>>> spark.sql('SELECT * from my_blog ORDER BY `公開日時`').show()
+--------------+--------+----+----------+
|      タイトル|カテゴリ|評価|  公開日時|
+--------------+--------+----+----------+
|    昨日のご飯|    生活| 3.0|2023-02-14|
|データ分析入門|    分析| 4.3|2023-03-02|
|    明日の運動|    生活| 1.9|2023-03-09|
|東武日光線往復|    旅行| 4.8|2023-03-25|
|    山手線一周|    旅行| 3.4|2023-04-01|
|     Spark入門|    分析| 2.9|2023-04-10|
+--------------+--------+----+----------+
```

最後の部分は、Sparkのデータフレームについて、Temp View名を設定し、それを使ってSQLを実行している例になります。なんと、データレイク上では、(Spark, Hive, Prestoなどを使うと)SQLもそのまま実行できるのです。


若干脱線してしましましたが、このように、データレイクでは、スキーマ・オン・リードに基づいて、以下の方針で運用ができるものになっています。

* とりあえず、大量のログファイルをデータレイクに保存しておく
* データが必要なタイミングになったら、Sparkなどを使って、データを構造化させる
* 構造化させた後に集計をとる、分析をするなど
* 上記の結果を再度ファイルとして保存する、もしくは、データベースに保存する

特に、ログデータ系のデータは、日々大量に生成され、発生することから、自然とデータサイズ
(根本的にはレコード数)が膨大になりすぎて、そのまま最初からデータベースに入れることが難しくなってきてきます。例えば、ウェブアクセスログなどは、1アクセス(ページアクセス単位ではなく、HTTPリクエスト単位、つまりサイトを構成するオブジェクト単位)で1ログ行が出力されるため、一日だけでも数GBになることはあり得ます。

そこで、一度データレイクにログを保存しておいて、Sparkなどでファイルを読み込み、集計処理や分析処理(例えば特定のIPアドレスのみを対象にフィルタリングやグループ集計するなど)を実施し、結果だけをデータベースに入れて、日々のレポート(可視化ツール)などを実施する運用が一般的になっています。

上記のようなデータ処理パターンをExract(データをファイルから抽出する)/Transform(集計処理などを実施)/Load(データベースに入れる)の頭文字をとってETLと呼ばれたりします。

つまり、データレイクは、データベースの前段での生データの保存とその集約処理の目的で使われることが多いのです。

-- ETLの図を入れる

## データ更新とデータレイクの弱み

上記のETLの使い方で一つ疑問が生じたかもしれません。それは、最後の「データベースに入れる」という部分です。データレイク上でテーブルのように扱える、しかもSQLも扱えるのであれば、最後のデータベースは不要で、そのままデータレイク上でデータベース処理を代行すれば良いのではないか、というアイデアです。

実は、上記の方法だといくつか問題が出てくるのです。つまり、後段のデータベースにはできて、データレクには苦手な処理があるということです。

### パフォーマンス

データレイクの場合は、ストレージ上に置かれたファイルがデータ(今回使用しているのはCSVファイル)の実体です。よって、何か処理をしようとする際には、毎回全部のファイルを読み込み、スキーマ・オン・ライト処理をかけて、集約処理や検索を実施する、というステップを踏みます。

一方で、データベースは、こうした処理を実施するのに最適化されたデータの配置をとっています。また、インデクシングのような、高速に特定のレコードにポインタアクセスできる機構も持っています。

### データの品質管理の徹底

データレイクでは、ファイルベースでデータが管理されるため、ずさんな運用だとファイルが意図せずに変更されてり、消されたり、コピーされたり、移動されたり、追加されたりしてしまいます。先ほど見てきた例からもわかる通り、ファイルがデータの実体なので、こうしたファイル操作はテーブルデータに直結します。つまり、テーブルが汚染され、最悪の場合には、破壊されます。

一方で、データベースは、ファイルはデータベース管理システム側で管理されるため、ユーザーが直接触ることはありません。品質を担保した運用がしやすくなっています。


### データの更新

データレイクは、基本的にファイルの追記は対応しますが、ファイルの変更や削除には対応していません。例えば、先ほどの例で言えば、`my_blog*.csv`を読み込んで、集約処理した結果を永続的に保存するには、全く別のファイル(別ファイル名のCSVファイルなど)として保存することになります。

また、元の`my_blog.csv`のレコードに変更(更新、`UPDATE`)があった場合や、レコードを削除したい場合であっても、別のファイルにする必要があります。

一方で、データベースは、`UPDATE`や`DELETE`などにも対応しており、柔軟、シンプル、かつ、高速にレコードの更新が可能になっています。

データレイクには、ここで見てきたような弱点が存在するのです。

### ACIDトランザクションの欠落

データレイクはレコードの更新はできない一方で、追記なら対応できるケースが多いです。これは、レコード追加の場合、ファイルも単純に追加すればいいので、こういったシンプルなケースであれば問題なく対応できるということです。ただし、この場合でも問題が出てきます。それは複数人が同時にデータを追記した場合に、データの一貫性(データ矛盾がない状態)が保たれないという問題です。

一方でデータベースには、こういった複数同時にアクセスがあった場合でも一貫性を保証するトランザクション機能が具備されています。


## データレイクのストレージレイヤの登場とレイクハウス

上記のデータレイクの弱みの原因はなんでしょうか?それは、最初はデータの永続化のみに着目し、従来からあるファイルフォーマットを利用しているところにあると言えます。ということは、データレイクの構成をとりながら、上記のデータレイクが弱い部分、つまりデータベースと比べて不足している機能を提供する永続化の方式・実装にすれば良いのではないか、というアイデアに行き着きます。これがデータレイクで用いらる「ストレージレイヤ」と呼ばれる機能です。

このストレージレイヤが提供する機能は、以下になります。

* パフォーマンス機能
* スキーマ強制
* データ更新
* ACIDトランザクション

さらに上記に加えて、

* タイムトラベル(自動テーブルスナップショット)
* テーブルクローン
* スキーマ進化(Schema Evolution)

など、通常のデータベースにはない機能も提供します。

このストレージレイヤについてもさまざまな実装やオープソースがあります。例えば、Apache Hudi、Apache Iceberg, Linux Fundation Delta Lakeなどがよく使われます。ここでも具体的な例から説明した方がわかりやすいと思いますので、代表してDelta Lakeを使用していきます。ただし、ここで紹介するデータレイクのストレージレイヤの機能は、他の実装でも対応している機能になりますので、理解する上での参考にしてください。

それでは、先ほどのCSVファイルから始めて、Delta Lakeを使ったテーブルデータの操作を実施してみたいと思います。Delta Lakeは、特に事前のインストールをせずに、以下のPysparkのオプション指定から使用することができます。

```sh
$ pyspark --packages io.delta:delta-core_2.12:2.3.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

...
...
SparkSession available as 'spark'.

>>> 
```

まずは、CSVファイルを読み込んで、集計処理して、結果をDelta Lakeを介して保存(永続化)していきます。上で見てきた例で言うと、ETLの最後にデータベースに結果を戻す部分をDelta Lakeでの保存に置き換えると言うことをやります。

```python
>>> df = (
        spark.read.format('csv')
        .option('inferSchema', True)
        .option('Header', True)
        .load('/lake/my_blog*.csv')
)

>>> from pyspark.sql.functions import date_trunc

>>> df_with_month = df.withColumn('公開年月', date_trunc('mon', df['公開日時']).cast('date'))

>>> df_with_month.show()

+--------------+--------+----+-------------------+----------+
|      タイトル|カテゴリ|評価|           公開日時|  公開年月|
+--------------+--------+----+-------------------+----------+
|    山手線一周|    旅行| 3.4|2023-04-01 00:00:00|2023-04-01|
|データ分析入門|    分析| 4.3|2023-03-02 00:00:00|2023-03-01|
|    昨日のご飯|    生活| 3.0|2023-02-14 00:00:00|2023-02-01|
|東武日光線往復|    旅行| 4.8|2023-03-25 00:00:00|2023-03-01|
|     Spark入門|    分析| 2.9|2023-04-10 00:00:00|2023-04-01|
|    明日の運動|    生活| 1.9|2023-03-09 00:00:00|2023-03-01|
+--------------+--------+----+-------------------+----------+

### 上記の処理したデータフレームの永続化(Delta Lakeで保存!)
>>> (
      df_with_month.write.format('delta')
      .mode('overwrite')
      .saveAsTable('my_blog_delta')
)

### Delta Lakeから読み出し
>>> spark.sql('SELECT * FROM my_blog_delta;').show()

+--------------+--------+----+-------------------+----------+
|      タイトル|カテゴリ|評価|           公開日時|  公開年月|
+--------------+--------+----+-------------------+----------+
|東武日光線往復|    旅行| 4.8|2023-03-25 00:00:00|2023-03-01|
|     Spark入門|    分析| 2.9|2023-04-10 00:00:00|2023-04-01|
|    明日の運動|    生活| 1.9|2023-03-09 00:00:00|2023-03-01|
|    山手線一周|    旅行| 3.4|2023-04-01 00:00:00|2023-04-01|
|データ分析入門|    分析| 4.3|2023-03-02 00:00:00|2023-03-01|
|    昨日のご飯|    生活| 3.0|2023-02-14 00:00:00|2023-02-01|
+--------------+--------+----+-------------------+----------+

>>> spark.sql('SELECT * FROM my_blog_delta WHERE `カテゴリ` = "分析";').show()

+--------------+--------+----+-------------------+----------+
|      タイトル|カテゴリ|評価|           公開日時|  公開年月|
+--------------+--------+----+-------------------+----------+
|     Spark入門|    分析| 2.9|2023-04-10 00:00:00|2023-04-01|
|データ分析入門|    分析| 4.3|2023-03-02 00:00:00|2023-03-01|
+--------------+--------+----+-------------------+----------+
```

上記では、実施にDelta Lake形式でテーブルデータの永続化を行なっている。実際に永続化(ファイル書き出し)している部分はどのコードかわかりますか? そうです、`saveAsTable()`の部分が該当します。しかし、ここで疑問が湧くかもしれません。なぜなら、この関数で指定しているのはテーブル名であって、ファイル名ではありません。実は、Delta Lakeでは、もはやファイルはユーザーに感じさせずに、テーブル名だけ意識すれば良いインターフェースを提供しています。これをマネージドテーブルと呼びます。実際にどのファイルパスにデルタのファイルが書かれているかは、以下のコマンドから確認できます。

```python
>>> spark.sql('DESC EXTENDED my_blog_delta').where('col_name'='Location').show(truncate=False)

+----------------------------+---------------------------------------------------+-------+
|col_name                    |data_type                                          |comment|
+----------------------------+---------------------------------------------------+-------+
|タイトル                    |string                                             |       |
|カテゴリ                    |string                                             |       |
|評価                        |double                                             |       |
|公開日時                    |timestamp                                          |       |
|公開年月                    |date                                               |       |
|                            |                                                   |       |
|# Partitioning              |                                                   |       |
|Not partitioned             |                                                   |       |
|                            |                                                   |       |
|# Detailed Table Information|                                                   |       |
|Name                        |default.my_blog_delta                              |       |
|Location                    |file:/home/ubuntu/spark-warehouse/my_blog_delta    |       |
|Provider                    |delta                                              |       |
|Owner                       |ubuntu                                             |       |
|Table Properties            |[delta.minReaderVersion=1,delta.minWriterVersion=2]|       |
+----------------------------+---------------------------------------------------+-------+
```

以上の結果から、ファイルパス`/home/ubuntu/spark-warehouse/my_blog_delta/`に今回永続化したファイルが置かれています。

```sh
$ tree /home/ubuntu/spark-warehouse/my_blog_delta/

/home/ubuntu/spark-warehouse/my_blog_delta/
├── _delta_log
│   └── 00000000000000000000.json
├── part-00000-3761203f-c329-438d-b827-66362847ac38-c000.snappy.parquet
└── part-00001-dffce999-1541-4166-a632-05b861fd9a9b-c000.snappy.parquet

1 directory, 3 files
```

Delta Lakeは、先ほどあげたようなデータベースが持っている機能を一通り実装されています。
ACIDトランザクションやパフォーマンス機能(インデクシングやBloomフィルタなど)が対応していますが、見えづらいので、ここではデータ更新について見ていきます。

```python
### レコードの削除
>>> spark.sql('DELETE FROM my_blog_delta WHERE `タイトル` = "山手線一周"')

### レコードの更新
>>> spark.sql('UPDATE my_blog_delta SET `評価` = 4.8 WHERE `タイトル` = "明日の運動"')

### レコードの追記
>>> spark.sql('''
  INSERT INTO my_blog_delta VALUES
   ("計算機科学の誘い", "科学", 4.5, "2023-04-29", "2023-04-01"),
   ("コーヒーのトレンド", "社会", 3.2, "2023-01-12", "2023-01-01") 
''')

### 最後にテーブルの確認
>>> spark.sql('SELECT * FROM my_blog_delta').show()

+------------------+--------+----+-------------------+----------+
|          タイトル|カテゴリ|評価|           公開日時|  公開年月|
+------------------+--------+----+-------------------+----------+
|コーヒーのトレンド|    社会| 3.2|2023-01-12 00:00:00|2023-01-01|
|    東武日光線往復|    旅行| 4.8|2023-03-25 00:00:00|2023-03-01|
|         Spark入門|    分析| 2.9|2023-04-10 00:00:00|2023-04-01|
|        明日の運動|    生活| 4.8|2023-03-09 00:00:00|2023-03-01|
|  計算機科学の誘い|    科学| 4.5|2023-04-29 00:00:00|2023-04-01|
|    データ分析入門|    分析| 4.3|2023-03-02 00:00:00|2023-03-01|
|        昨日のご飯|    生活| 3.0|2023-02-14 00:00:00|2023-02-01|
+------------------+--------+----+-------------------+----------+
```

このように、データベースのようにレコードが更新されます。
これはDelta Lakeなので、常にテーブルの状態は永続化されています。
テーブルの更新されたヒストリ(スナップショット)や以前のversionのテーブルを参照するタイムトラベルなども標準で実装されています。

```python
### テーブルのヒストリをリストする
>>> (
      spark.sql('DESC HISTORY my_blog_delta')
      .select('version', 'timestamp', 'userId', 'operation', 'operationParameters')
      .show()
)

+-------+--------------------+------+--------------------+----------------------+
|version|           timestamp|userId|           operation|   operationParameters|
+-------+--------------------+------+--------------------+----------------------+
|      3|2023-05-02 18:55:...|  null|               WRITE|  {mode -> Append, ...|
|      2|2023-05-02 18:55:...|  null|              UPDATE|{predicate -> (タイ...|
|      1|2023-05-02 18:55:...|  null|              DELETE|  {predicate -> ["(...|
|      0|2023-05-02 17:47:...|  null|CREATE OR REPLACE...|  {isManaged -> tru...|
+-------+--------------------+------+--------------------+----------------------+

### version:0のテーブルを参照する
>>> spark.sql('SELECT * FROM my_blog_delta version as of 0').show()
+--------------+--------+----+-------------------+----------+                   
|      タイトル|カテゴリ|評価|           公開日時|  公開年月|
+--------------+--------+----+-------------------+----------+
|東武日光線往復|    旅行| 4.8|2023-03-25 00:00:00|2023-03-01|
|     Spark入門|    分析| 2.9|2023-04-10 00:00:00|2023-04-01|
|    明日の運動|    生活| 1.9|2023-03-09 00:00:00|2023-03-01|
|    山手線一周|    旅行| 3.4|2023-04-01 00:00:00|2023-04-01|
|データ分析入門|    分析| 4.3|2023-03-02 00:00:00|2023-03-01|
|    昨日のご飯|    生活| 3.0|2023-02-14 00:00:00|2023-02-01|
+--------------+--------+----+-------------------+----------+
```

テーブルヒストリによって、いつ、誰が、どのレコードに対して、何をしたのかが一覧でき、かつ、任意のVersionをすぐに参照することができます。

以上、見てきたように、データレイクのストレージレイヤはデータベースが持つ基本機能を提供してくれるのです。この事実は、データレイクでありがながら、データウェアハウス(長期間保存用のデータベース)の機能を併せ持つ、別の味方をすると、データ**レイク**とデータウェア**ハウス**のいいところドリなので、これをもって**「(データ)レイクハウス」**と呼ばれるのです。

## ストレージとコンピュートの分離

データベースと比較した場合に、レイクハウスが持つ特徴の一つに「ストレージとコンピュートの分離」と言うものがあります。これまで見てきてた通り、レイクハウスでは、データの保存場所としてオブジェクトストレージ、ストレージレイヤとして論理的なデータベース機能実装(Delta Lakeなど)、計算処理ではSparkなどの分散処理の構成になっています。つまり、データの実体であるストレージと、その上で機能するコンピュートが完全に分離する形の構成になっています。この「分離」と「分散処理」によってシステムのスケーラビリティが生まれてくるのです。

具体例を挙げていきます。
例えば、店舗の売上時系列データをデータベース(またはデータウェアハウス)で管理しているとします。
このデータは日々生まれ、かつ、店舗が拡大するたびに日次のデータ量が増えてきます。そして、ある日を境に、当初予定していたデータベースシステムでは賄いきれないほどのサイズになり、エラーが発生してしまいました。これを解決するにはどうすれば良いのでしょうか?

実は、この主な原因はいくつか考えられます。一つは、テーブルのデータサイズがデータベースシステムのディスク容量の限界に達し、エラーが出た可能性。また、ディスクには収まっているものの、ストレージのI/Oもしくは、集計などで使用するメモリサイズが足りなくなった可能性。他にも、集計処理の自体がマシンのCPUパワーを使い切ってしまっている可能性。などなど。

上記のような状況だと、不足しているリソースを増強するしかありません。ただし、大抵のデータベースシステムの場合、コンピュートの能力(CPU・メモリ)とストレージ容量が密に連携していることが多く、片方だけ変更することが難しい場合があります。





(注: 今回の例ではもっぱらLinuxのローカルストレージを使い、かつ、シングルのLinuxマシン上でSparkを実行しているため、上記のような分離とスケーラビリティは実現できていません。それでも、同じコードをクラウドに持っていけばそのまま動き、かつ、そのスケーラビリティも実現できるので、OSSでローカル環境で試せる意味は大きいはずです！)

## ガバナンスとメタデータ管理



## まとめ

