# ndn-tools-seed-python

`ndn-tools-seed/tools/seed/server` のPython移植です。ManagerからNDN Interestで
関数コードを受け取り、KubernetesのConfigMapと関数実行Podを作成・削除します。
元の `.github/workflows/docker-publish.yaml` の3対象を引き継ぎます。

| 元の対象 | Python版 |
| --- | --- |
| `tools/seed/server/{main.cpp,seed.cpp,seed.hpp}` | `seed/{__main__,server}.py` |
| `start_func_in_container.sh` | `seed/backend.py`（Kubernetes API） |
| `Dockerfile.seed` | Python専用の`Dockerfile.seed` |
| `Dockerfile.nfd` | NFDはC++製デーモンのまま引き継ぎ |
| `sidecar/` | 公開版ndnc CLIを使う.ndn実行系とNDN Sidecar |
| Workflow | `.github/workflows/docker-push.yaml` |

旧worker.cppは元のWorkflowのビルドに含まれず、現在のSeedからも呼ばれないため対象外です。
C++の任意のSigningInfo指定とunsolicitedオプションは移植対象外です。
DataはDigestSha256で署名します。freshness、final、timeout、verbose、namespaceは指定できます。

## 起動

Python 3.12以上を使用します。

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
NODE_NAME=worker-1 python -m seed -n default -v
# TCP接続の場合（NFD側のlocalhop_securityが必要）
NODE_NAME=worker-1 python -m seed --transport tcp://192.0.2.10:6363
# プレフィックスを明示する場合
python -m seed /worker-1/seed --node-name worker-1
```

Kubernetesのin-cluster認証、またはローカルkubeconfigを利用します。
`NODE_NAME`から`/{node_name}/seed`を登録し、作成するPodもそのノードに固定します。
`SEED_PREFIX`または位置引数でプレフィックスを変更できます。
`POD_NAMESPACE`は既定で`default`、`FUNCTION_IMAGE`は既定で
`ghcr.io/nakazato-lab/ndn-tools-seed-python/my-edge-function:latest`です。自分の公開先に変更してください。

KubernetesのDaemonSetやRBACなどの定義は`k8s-manifest`で管理します。
FunctionとSidecarにはSeedと同じ`NDN_CLIENT_TRANSPORT`を渡します。
TCP接続時はホストソケットをマウントしません。UNIX接続時は配置先ノードの
`/var/run/nfd-k8s`を両コンテナの`/run`へマウントします。

## Managerとの通信契約

`ndn_resorce_placement-1/manager/ndn_manager.py`は次を送っています。

- 宛先: `/{best_node}/seed`
- `app_param`: UTF-8のJSON
- `must_be_fresh=True`, `can_be_prefix=True`, `lifetime=5000`

```json
{"type":"CREATE","name":"/demo/function/func1","content":"let avg = (arg0 + arg1) / 2\nprint avg","content_type":"ndn"}
```

`content_type`はC++版と同じく無視します。Seedは`content`が文字列であることだけを確認し、
構文や`handle`定義を検査せず、そのまま実行用Podへ渡します。`.ndn`も受け渡しできます。
Functionは`.ndn`コードを保存し、呼び出し時に`ndnc run`を実行します。構文検査もndncに任せます。
CREATE成功応答はKubernetes APIでのリソース作成完了であり、Pod Readyや関数実行の成功保証ではありません。

DELETEは`{"type":"DELETE","name":"/demo/function/func1"}`、一覧取得はapp_paramなしです。
成功時はプレフィックス一覧、空なら`no server created\n`を返します。
応答名は受信Interest名（ParametersSha256Digest込み）と一致します。
一覧はC++版と同じくプロセス内メモリで、再起動すると消えます。
同名CREATEは409エラーになるため、置換時はDELETE後にPodの削除完了を待ってからCREATEしてください。

Manager側の送信形式を変更する必要はありません。Seed応答の`Error:`は登録失敗として扱ってください。
成功応答もリソース作成完了を意味します。API処理が5秒を超えるとManagerはタイムアウトします。
Managerの手計算params-sha256ログは実際のwire名と一致する保証がなく、送信には使われません。

NFDへの登録はUNIX接続なら`/localhost/nfd`、TCPなら`/localhop/nfd`を使用します。
Python側は元と同様にコマンド送信元の認証を追加していません。
複数NFD間の経路は別途必要です。Seedの登録だけでManager側NFDに経路が自動配布されるわけではありません。
APIは[python-ndn公式ドキュメント](https://python-ndn.readthedocs.io/en/latest/src/app.html)に準拠します。

## Function Podでの.ndn実行

Functionイメージは公開パッケージ`ndnc==0.0.6`を`pip install`で導入します。
`sidecar/ndnc/`へのソースコピーは行いません。公開ページ:
https://www.piwheels.org/project/ndnc/ （通常のpipではPyPIから取得します）。

`ndnc 0.0.6`は`lark==1.1.9`を要求するため、Function側の`python-ndn`は互換性のある
`0.4.2`に固定しています。Seed側は`python-ndn==0.5.1`のままです。
Function側は`ndnc`のパーサーやASTをインポートしません。
`sidecar/ndn_runtime.py`はコマンドの起動と標準出力・終了コードの受け取りだけを行います。

処理の流れは次のとおりです。

1. SeedがコードをConfigMapに保存してFunction Podを作成。
2. SidecarがgRPCの`DeployFunction`でソースを登録。Functionは文字列を保持するだけです。
3. 呼び出しInterestの引数をSidecarがgRPCの`ExecuteFunction`へそのまま渡す。
4. Functionが呼び出し専用の一時ファイル`function.ndn`にコードを保存。
5. `ndnc run /tmp/.../function.ndn -- ARG0 ARG1 ...`を実行。
6. 標準出力をgRPC経由でSidecarへ返し、NDN Dataとして応答。

例えば次のコードを`/function`として登録できます。

```text
let avg = (arg0 + arg1) / 2
print avg
```

`/function/(10,20)`は`ndnc run function.ndn -- 10 20`に相当し、結果は`15`です。
位置引数を`arg0`, `arg1`, ...へ割り当てる処理はndnc自身が担当します。

NDNデータ名を渡す場合は、CLIと同じく`.ndn`側で明示的に取得してください。
Function側ではNDN名を自動的にデータ値へ変換しません。

```text
let a = interest arg0
let b = interest arg1
print (a + b) / 2
```

この場合の呼び出し例は`/function/(/data/a/,/data/b/)`です。
`NDN_CLIENT_TRANSPORT`は子プロセスへ引き継がれ、NDN通信や関数合成は公開版ndncが行います。
通信失敗時の動作やキャッシュもndnc CLIの仕様に従います。

`print`の標準出力を結果として返します（最後の改行1つだけ除去）。
終了コードが0以外なら標準エラーを含むgRPCエラーになり、Sidecarは`Error:`のDataを返します。
実行時間は最大20秒、またはgRPCの残り期限です。中断時には子プロセスを停止し、一時ファイルを削除します。
複数の呼び出しは別プロセス・別ファイルで実行します。
登録成功はソースの保存完了を意味し、構文エラーは最初の実行時にndncから返されます。

`/function/code`はソース取得用です。ConfigMapキーとマウント名は互換性のため`func.py`のままですが、
実行時には`.ndn`ファイルとして保存します。Pythonの`handle(args)`自動判定・実行は行いません。

変更の反映には`ndn-seed`と`my-edge-function`の両イメージを再ビルド・公開し、
Seedを更新した後で既存Function Podを削除・再登録してください。

## ビルドと公開

```sh
docker build -f Dockerfile.seed -t ndn-seed .
docker build -f Dockerfile.nfd -t ndn-nfd .
docker build -t my-edge-function sidecar
```

`.github/workflows/docker-push.yaml`はmainへのpushまたは手動実行で3イメージを公開します。
公開先はGitHub Container Registry（GHCR）です。`github.actor`と自動発行される
`GITHUB_TOKEN`で認証し、Workflowに`packages: write`権限を付与しています。
Docker Hub用Secretsの設定は不要です。

タグ形式は`ghcr.io/<owner>/<repository>/<image>:latest`で、現在の公開先は以下です。

- `ghcr.io/nakazato-lab/ndn-tools-seed-python/ndn-nfd:latest`
- `ghcr.io/nakazato-lab/ndn-tools-seed-python/ndn-seed:latest`
- `ghcr.io/nakazato-lab/ndn-tools-seed-python/my-edge-function:latest`

`k8s-manifest`側のイメージ参照と、`FUNCTION_IMAGE`を明示している場合はその値も
上記のGHCRパスに揃えてください。
認証方式は[GitHub公式ドキュメント](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images#publishing-images-to-github-packages)に従っています。

PRではビルドのみ実行します。今回、実際のpushは行っていません。


元コードのライセンスは`COPYING.md`を参照してください。

NFDイメージでは、Dockerでexecを妨げていたパッケージ由来のfile capabilityを除去し、
`localhop_security`の挿入を設定行だけに限定しました。Ethernet Faceの追加権限はこのTCP/UNIX構成の対象外です。
