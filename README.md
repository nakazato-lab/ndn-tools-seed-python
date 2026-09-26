# ndn-tools-seed-python

`ndn-tools-seed/tools/seed/server` のPython移植です。ManagerからNDN Interestで
Pythonコードを受け取り、KubernetesのConfigMapと関数実行Podを作成・削除します。
元の `.github/workflows/docker-publish.yaml` の3対象を引き継ぎます。

| 元の対象 | Python版 |
| --- | --- |
| `tools/seed/server/{main.cpp,seed.cpp,seed.hpp}` | `seed/{__main__,server}.py` |
| `start_func_in_container.sh` | `seed/backend.py`（Kubernetes API） |
| `Dockerfile.seed` | Python専用の`Dockerfile.seed` |
| `Dockerfile.nfd` | NFDはC++製デーモンのまま引き継ぎ |
| `sidecar/` | 既存Python実行系を引き継ぎ、起動パスとgRPC待機を修正 |
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
`ryotaroiwata/my-edge-function:latest`です。自分の公開先に変更してください。

KubernetesのDaemonSetやRBACなどの定義は`k8s-manifest`で管理します。
各ノードでNFDが起動し、ホストの`/var/run/nfd-k8s/nfd.sock`が利用できる構成を前提とします。

## Managerとの通信契約

`ndn_resorce_placement-1/manager/ndn_manager.py`は次を送っています。

- 宛先: `/{best_node}/seed`
- `app_param`: UTF-8のJSON
- `must_be_fresh=True`, `can_be_prefix=True`, `lifetime=5000`

```json
{"type":"CREATE","name":"/demo/function/func1","content":"def handle(args):\n    return '-'.join(args)","content_type":"ndn"}
```

`content_type`はC++版と同じく無視します。**contentはPythonの`handle(args)`を定義する
ソースコードです。** 登録側サンプルの`let avg = ...`という独自.ndn言語は実行できません。
構文とhandle定義を検査し、無効なコードには`Error:`で始まるDataを返します。
CREATE成功応答はKubernetes APIでのリソース作成完了であり、Pod Readyや関数実行の成功保証ではありません。

DELETEは`{"type":"DELETE","name":"/demo/function/func1"}`、一覧取得はapp_paramなしです。
成功時はプレフィックス一覧、空なら`no server created\n`を返します。
応答名は受信Interest名（ParametersSha256Digest込み）と一致します。
一覧はC++版と同じくプロセス内メモリで、再起動すると消えます。
同名CREATEは409エラーになるため、置換時はDELETE後にPodの削除完了を待ってからCREATEしてください。

Manager側の送信形式を変更する必要はありません。ただし既存Managerは任意のData応答を
`Success: Function deployed`として表示します。呼び出し側ではSeed応答の`Error:`を確認し、
成功表示も「作成要求受付」と解釈してください。API処理が5秒を超えるとManagerはタイムアウトします。
Managerの手計算params-sha256ログは実際のwire名と一致する保証がなく、送信には使われません。

NFDへの登録はUNIX接続なら`/localhost/nfd`、TCPなら`/localhop/nfd`を使用します。
Python側は元と同様にコマンド送信元の認証を追加していません。
複数NFD間の経路は別途必要です。Seedの登録だけでManager側NFDに経路が自動配布されるわけではありません。
APIは[python-ndn公式ドキュメント](https://python-ndn.readthedocs.io/en/latest/src/app.html)に準拠します。

## ビルドと公開

```sh
docker build -f Dockerfile.seed -t ndn-seed .
docker build -f Dockerfile.nfd -t ndn-nfd .
docker build -t my-edge-function sidecar
```

`.github/workflows/docker-push.yaml`はmainへのpushまたは手動実行で3イメージを公開します。
必要なGitHub Secretsは既存と同じ`DOCKER_HUB_USER`と`DOCKER_HUB_ACCESS_TOKEN`です。
公開名は`<user>/ndn-seed:latest`、`<user>/ndn-nfd:latest`、`<user>/my-edge-function:latest`。
PRではビルドのみ実行します。今回、実際のpushは行っていません。


元コードのライセンスは`COPYING.md`を参照してください。

NFDイメージでは、Dockerでexecを妨げていたパッケージ由来のfile capabilityを除去し、
`localhop_security`の挿入を設定行だけに限定しました。Ethernet Faceの追加権限はこのTCP/UNIX構成の対象外です。
