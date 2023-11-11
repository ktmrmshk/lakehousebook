# MLflow2.8の新しい機能

MLflow2.8では、昨今の大規模言語モデル(Large Language Model, LLM)の広まりを受けて、LLMの構築や運用に関わる機能が追加されました。LLMは、実際にはDNNモデルという意味では従来の機械学習の運用の延長線上にあります。一方で、実際には従来の機械学習とは異なる扱いをする点もいくつかあります。例えば、LLMはモデルサイズが非常に大きく、スクラッチからモデルを作成するよりも既存のモデルを使用する、もしくは、既存のモデルをベースに転移学習やファインチューニングを実施することが現状ではほとんどのケースになっています。

そのため、さまざまなLLMに関わるサービスやツールが開発・公開されており、これらを比較しながら最適なLLMシステムを構築する必要があります。また、モデルの評価方法についても、LLM自体の対象が自然言語であることから、数値的なメトリックに加えて、われわれ人間が確認する場面も多いです。プロンプトエンジニアリングにおいても、人間が入力し、出力を目でみて評価する作業の繰り返しになります。MLflow2.8では、LLMを扱う上で必要な作業を効率的に実施するヘルパー機能が追加されています。

## MLflow AI Gateway

MLflow AI Gatewayは、さまざまなLLMプロバイダやフレームワークとの連携のための統一したインタフェースを提供する機能です。執筆時点ではOpenAI、MosaicML、Cohere、Anthropic、PaLM 2、AWS Bedrock、AI21 Labsなどに対応しています。また、メジャーなSaaS LLMプロバイダーのサポートに加え、MLflowモデルサービングへの統合も実装されており、独自のLLMや微調整された基盤モデルを自社のサービングインフラストラクチャ内でサービングすることができます。

MLflow AI Gatewayを利用することで以下のメリットが享受できます。

1. 統一されたエンドポイントとAPI実装: LLMプロバイダごと独自APIを隠蔽します
1. シンプルな設計: 設定やセットアップが一度で完結
1. 安全なクレデンシャル管理: APIキーの一元管理とハードコーディングの回避
1. ゼロダウンタイムでのLLMプロバイダ切り替え: コード変更なしにLLMプロバイダやルートを変更

### Gatewayのセットアップ

ここではOpenAIをLLMプロバイダとして使用する例を見ていきましょう。最初にMLflow AI Gateway Serviceをインストールします。PyPIレポジトリもしくはMLflowリポジトリから`pip`コマンドでインストールできます。

```bash
pip install 'mlflow[gateway]'
```

続いてOpenAI API Keyを用意します。ここではOpenAIのみを使用する例を見ていきますが、複数のLLMプロバイダを混在させて使用することも可能です。その場合は、それぞれのAPI Keyなどのクレデンシャルが必要になります。環境変数を使用してAPI Keyを登録します。

```bash
export OPENAI_API_KEY='ここにあなたのAPI KEYを記載する'
```

続いてGateway設定ファイルを作成します。この設定ファイルの中にGatewayで受ける経路(Route)をYAML形式で記述します。それでは、補完(Completion)、会話(Chat)、埋め込み(Embeddings)の3経路を定義します。

```yaml
routes:
  - name: completions
    route_type: llm/v1/completions
    model:
      provider: openai
      name: gpt-3.5-turbo
      config:
        openai_api_key: $OPENAI_API_KEY

  - name: chat
    route_type: llm/v1/chat
    model:
      provider: openai
      name: gpt-3.5-turbo
      config:
        openai_api_key: $OPENAI_API_KEY

  - name: embeddings
    route_type: llm/v1/embeddings
    model:
      provider: openai
      name: text-embedding-ada-002
      config:
        openai_api_key: $OPENAI_API_KEY
```

この設定ファイル(`config.yaml`)を使用してGatewayサービスを起動します。

```bash
mlflow gateway start --config-path config.yml
```

起動時にオプションで、サービスのListenホスト・ポート番号やサービスのワーカー数などが変更できます。デフォルトではホストは`localhost`、ポート番号は`5000`、ワーカー数は`2`で起動します。

Gatewayサービス起動後は、エンドポイントとしての機能に加えて、APIドキュメントサービスも提供します。ブラウザを使用して`http://localhost:5000`にアクセスしてみましょう。図XXXにある通り、OpenAPI形式のリファレンスが確認できます。また、リファレンスの各API定義の詳細ペインの中にある`Try Out`メニューからAPIへのリクエストを送信することも可能です(図XXX)。

![MLflow Gateway API](images/MLflow_Gateway_API.png)

![MLflow Gateway API Try Out](images/MLflow_Gateway_API_Try_Out.png)


Gatewayサービスへは、上記のブラウザからのTry Out UIの他に、Fluent API、Client API、そしてRESTful APIでリクエストを送信することが可能です。それぞれ見ていきましょう。

### Fluent API

以下がFluent APIを使用する例です。

```python
from mlflow.gateway import query, set_gateway_uri

set_gateway_uri(gateway_uri="http://localhost:5000")

response = query(
    "chat",
    {"messages": [{"role": "user", "content": "1週間のうち一番良い曜日は何ですか?"}]},
)

print(response)
```

レスポンスは以下のような形式になります。

```
{
  "candidates": [
    {
      "message": {
        "role": "assistant",
        "content": "それは個人の好みや状況によって異なるかもしれません。一般的には、週末の土曜日や日曜日が人気があります。これらの日は多くの人が休みであり、リラックスしたり、趣味や家族との時間を楽しむことができます。ただし、仕事や学校のスケジュールによっては、他の曜日が一番良いと感じることもあります。"
      },
      "metadata": {
        "finish_reason": "stop"
      }
    }
  ],
  "metadata": {
    "input_tokens": 27,
    "output_tokens": 145,
    "total_tokens": 172,
    "model": "gpt-3.5-turbo-0613",
    "route_type": "llm/v1/chat"
  }
}
```

### Client API

Client APIを使用すると、Gatewayについての詳細な操作が可能です。例えば、Gatewayに登録され、利用可能な経路を参照することができます。

```python
from mlflow.gateway import MlflowGatewayClient

gateway_client = MlflowGatewayClient(gateway_uri="http://192.168.64.21:5000")

routes = gateway_client.search_routes()
for route in routes:
    print('----')
    print(route)
```

結果は以下の通りです。

```
----
name='completions' route_type='llm/v1/completions' model=RouteModelInfo(name='gpt-3.5-turbo', provider='openai') route_url='http://192.168.64.21:5000/gateway/completions/invocations'
----
name='chat' route_type='llm/v1/chat' model=RouteModelInfo(name='gpt-3.5-turbo', provider='openai') route_url='http://192.168.64.21:5000/gateway/chat/invocations'
----
name='embeddings' route_type='llm/v1/embeddings' model=RouteModelInfo(name='text-embedding-ada-002', provider='openai') route_url='http://192.168.64.21:5000/gateway/embeddings/invocations'
```

もちろん、Gatewayの経路に対してリクエストを出すこともできます。

```python
response = gateway_client.query(
    "chat", {"messages": [{"role": "user", "content": "1週間のうち一番良い曜日は何ですか?"}]}
)
print(response)
```

出力結果は前述のFluent APIの場合と同じです。


### LangChainとの連携

Client APIを使うメリットは、LangChainとの連携です。Gatewayの経路をLangChainの`llm`として使用することができます。
以下は、Gateway上の`completion`経路をLangChainの`llm`として使用するコード例です。

```python
import mlflow
from langchain import LLMChain, PromptTemplate
from langchain.llms import MlflowAIGateway

gateway = MlflowAIGateway(
    gateway_uri="http://127.0.0.1:5000",
    route="completions",
    params={
        "temperature": 0.0,
        "top_p": 0.1,
    },
)

llm_chain = LLMChain(
    llm=gateway,
    prompt=PromptTemplate(
        input_variables=["adjective"],
        template="{adjective} 冗談を言ってください",
    ),
)
result = llm_chain.run(adjective="面白い")
print(result)
```

出力は以下の通りです。

```
「なぜカエルは車に乗らないのか？　なぜなら、自分で運転するとハンドルが手に合わないからだよ！」
```

### REST API

GatewayはREST APIもサービス提供します。一般的なアプリケーション、既存のアプリケーションとの連携もスムーズになります。ここでは、`curl`クライアントを使ってREST APIでリクエストする例を見ていきます。リクエストのBodyのスキーマは、前述したOpenAPI形式のリファレンスから参照できます。最初に利用可能な経路をリストしてみましょう。

```
curl -X GET http://127.0.0.1:5000/api/2.0/gateway/routes/

[結果]
{
    "routes": [
        {
            "name": "completions",
            "route_type": "llm/v1/completions",
            "model": {
                "name": "gpt-3.5-turbo",
                "provider": "openai"
            },
            "route_url": "/gateway/completions/invocations"
        },
        {
            "name": "chat",
            "route_type": "llm/v1/chat",
            "model": {
                "name": "gpt-3.5-turbo",
                "provider": "openai"
            },
            "route_url": "/gateway/chat/invocations"
        },
        {
            "name": "embeddings",
            "route_type": "llm/v1/embeddings",
            "model": {
                "name": "text-embedding-ada-002",
                "provider": "openai"
            },
            "route_url": "/gateway/embeddings/invocations"
        }
    ],
    "next_page_token": null
}
```

Gatewayの特定の経路へのリクエストは`POST /gateway/{経路名}/invocations`になります。それでは、先ほどFluent APIで実行したものと同等のクエリをREST APIで実行してみましょう。なお、出力結果は先ほどと同じです。

```bash
curl -X POST http://localhost:5000/gateway/chat/invocations \
  -H "Content-Type: application/json" \
  -d '{"messages": [{"role": "user", "content": "1週間のうち一番良い曜日は何ですか?"}]}'
```

### 複数のLLMの比較

Gatewayの強力な側面は、経路を複数定義でき、それらを簡単に切り替えることができるという点です。経路の名前を複数用意してクライアント・アプリケーション側で切り替えることもできますし、Gatewayの設定ファイル(`config.yaml`)の中で同じ経路名でパラメータを変更することでクライアント・アプリケーション側のコードは変更せずにLLMを入れ替えることができます。

ここでは、前者の経路名を複数用意する方法を見ていきます。先ほど設定した`config.yaml`に以下の経路を追加します。

```yaml
routes:
  - name: completions
    route_type: llm/v1/completions
    model:
      provider: openai
      name: gpt-3.5-turbo
      config:
        openai_api_key: $OPENAI_API_KEY
  - name: completions-gpt4-turbo
    route_type: llm/v1/completions
    model:
      provider: openai
      name: gpt-4-1106-preview
      config:
        openai_api_key: $OPENAI_API_KEY
```

この設定ファイルの変更は、既存からある経路`completions`は保持したまま、新しい経路`completions-gpt4-turbo`を追加します。設定ファイルを更新したら、変更を保存するだけです。ゲートウェイはダウンタイムなしで自動的に新しいルートを作成します。

### AI Gatewayのセキュリティ

これまで見てきた通りGatewayサービスは動作確認の簡単化のため通信経路上では暗号化しない`http`プロトコルを使用してきました。Gatewayサービスを本番環境で使用する際にはリバースプロキシを用意し、TLSを使用して通信経路上の暗号化を実施する`https`プロトコルを利用する必要があります。リバースプロキシとしてよく使われるのはNginxです。アプリケーションへのトラフィックを処理することに加えて、Nginxは静的なファイルのサーブもでき、アプリケーションの複数のインスタンスが動作している場合にトラフィックの負荷分散を行うこともできます。

リバースプロキシに加えて、リクエストが MLflow AI ゲートウェイに到達する前に認証レイヤーを追加することも推奨されます。HTTPベーシック認証、OAuth、などニーズにあった方式を検討してください。

