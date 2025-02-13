//
//  CWSLNats.swift
//  SmiralCamera
//
//  Created by Kentaro Kawai on 2024/09/05.
//

import Foundation
import Nats

class CWSLNats {
  static func send() async {
    var subscription: NatsSubscription?
    let nats: NatsClient

    do {
      // 接続先URLの取得
      guard let ugoUrlString = OSFCModel.config().item(link: .ugoAddress)?.stringValue,
            let url = URL(string: ugoUrlString) else {
        return
      }
      print("ugoUrl: \(ugoUrlString)")

      // NATSクライアントの生成
      nats = NatsClientOptions()
        .url(url)
        .build()

      // （オプション）接続イベントのリスナー設定
      nats.on(.connected) { event in
        print("event: connected")
      }

      // サーバーへの接続
      try await nats.connect()

      // 終了時に必ずクリーンアップを実施（非同期処理のためTask.detachedを利用）
      defer {
        Task.detached {
          if let sub = subscription {
            do {
              try await sub.unsubscribe()
            } catch {
              print("Error unsubscribing: \(error)")
            }
          }
          do {
            try await nats.close()
          } catch {
            print("Error closing connection: \(error)")
          }
        }
      }

      // 送信メッセージの作成
      let message = """
        {
          "id" : "requestId",
          "t" : 1602546613,
          "m": "flow",
          "c" : "flow_start",
          "flow_id" : "FLnzr6Ju-KUyCtWL",
          "index" : 0
        }
      """
      print(message)
      guard let payload = message.data(using: .utf8) else {
        print("Failed to encode message to Data")
        return
      }

      // 一時的なリプライトピックを生成
      let replySubject = "temp_reply_topic_\(UUID().uuidString)"

      // リクエストを送信するトピック
      let requestTopic = "flow.cmd"

      // 一時的なリプライトピックにサブスクライブ
      subscription = try await nats.subscribe(subject: replySubject)

      // リクエストの送信（replySubjectを指定）
      try await nats.publish(payload, subject: requestTopic, reply: replySubject)

      // レスポンスの待機
      for try await msg in subscription! {
        print(msg)
        guard let payload = msg.payload,
              let response = String(data: payload, encoding: .utf8) else {
          continue
        }
        print("Received response: \(response)")
        break  // 応答を受信したらループを抜ける
      }

    } catch {
      print("Error occurred: \(error)")
    }
  }
}
