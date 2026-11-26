# ASV Mac版の更新を配布する

アプリの「アップデートを確認」から、GitHubの最新リリースに添付した `appcast.xml` を取得します。SparkleがフィードとZIPのEd25519署名を確認し、利用者が同意するとアプリを入れ替えて再起動します。初回リリースの公開前は、更新情報がまだ公開されていないことを表示します。

## 配布物の作成

リポジトリのルートで、公開する版のソースをチェックアウトして実行します。以前の版より大きい `X.Y.Z` を指定してください。次の例ではApple SiliconとIntelの両方で動く0.1.1を作成します。

```sh
python3 mac_app/scripts/prepare_release.py 0.1.1
```

更新の署名には、ASVの開発用Macのログインキーチェーンにある `ASV-Mac-Updates` アカウントを使います。対応する公開鍵は `Info.plist` の `SUPublicEDKey` に登録してあります。ビルドとGitHub Actionsのテストには公開鍵だけで足ります。開発用Macを移行するときはSparkleの手順に従って秘密鍵をバックアップし、新しいキーチェーンに復元してください。[Sparkle公式の鍵の管理手順](https://sparkle-project.org/documentation/#eddsa-ed25519-signatures)

一般配布用にAppleの署名・公証も付ける場合は、キーチェーンのDeveloper ID Application証明書と `notarytool store-credentials` で登録したプロファイル名を指定します。上記の基本コマンドで作成するアプリはアドホック署名のため、インターネットから初めてダウンロードした際にGatekeeperの警告が出る場合があります。

```sh
python3 mac_app/scripts/prepare_release.py 0.1.1 \
  --sign-identity "Developer ID Application: 組織名 (TEAMID)" \
  --notary-profile ASV-Notary
```

`mac_app/dist/mac-v0.1.1/` に配布ZIPと署名済みの更新フィードができます。作成スクリプトは両方の署名を検証します。この2ファイルは生成したまま公開してください。

## GitHubでの公開

公開対象のコミットに `mac-v0.1.1` タグを作成してGitHubへpushした後、同名のリリースに両方のファイルを添付し、最新リリースに設定します。公開前にドラフトで内容を確認できます。

```sh
gh release create mac-v0.1.1 --verify-tag --draft --title "ASV Mac 0.1.1" \
  --generate-notes mac_app/dist/mac-v0.1.1/ASV-0.1.1.zip \
  mac_app/dist/mac-v0.1.1/appcast.xml
gh release edit mac-v0.1.1 --draft=false --latest
```

更新フィードの固定URLはGitHubの `releases/latest/download/appcast.xml` です。ほかの製品のリリースを同じリポジトリに置く場合は、ASV Macのリリースが最新を指すようにしてください。すでに配布した版の更新テストでは、書き込み可能なフォルダ内の古い `ASV.app` を起動し、アプリメニューから新しい版の確認、インストール、再起動後のバージョン表示まで確かめます。
