//
//  CWSLNats.swift
//  SmiralCamera
//
//  Created by Kentaro Kawai on 2024/09/05.
//

import Foundation
import Nats

class CWSLNats {
  // 前回呼び出し時間を保持するための静的プロパティ
  private static var lastCallDate: Date?
  // 一定時間(ここでは5秒)を定義
  private static let minInterval: TimeInterval = 5.0

  static func send() async {
    // ① 前回の呼び出しから一定時間経っていなければ何もしない
    let now = Date()
    if let last = lastCallDate, now.timeIntervalSince(last) < minInterval {
      // 実行をスキップしてリターン
      print("CWSLNats.send() was called too soon. Skipping.")
      return
    }
    // ここで呼び出し記録を更新
    lastCallDate = now

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

      // 接続イベントのリスナー（オプション）
      nats.on(.connected) { event in
        print("event: connected")
      }

      // サーバーへの接続
      try await nats.connect()

      // 終了時に必ずクリーンアップを実施（非同期処理のため Task.detached を利用）
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

      // --- タイムアウト付きでレスポンスを待機（例：5秒） ---
      let response: String = try await withThrowingTaskGroup(of: String.self) { group in
        // メッセージ受信待ちタスク
        group.addTask {
          for try await msg in subscription! {
            guard let payload = msg.payload,
                  let response = String(data: payload, encoding: .utf8) else {
              continue
            }
            return response
          }
          // ループを抜けずに終了した場合はエラー扱い
          throw NSError(domain: "NoMessageReceived", code: 0, userInfo: nil)
        }

        // タイムアウトタスク
        group.addTask {
          try await Task.sleep(nanoseconds: 5_000_000_000)
          throw NSError(domain: "Timeout", code: 1, userInfo: nil)
        }

        // 最初に完了したタスクの結果を採用
        let result = try await group.next()!
        // 他タスクは不要なのでキャンセル
        group.cancelAll()
        return result
      }

      // 応答を受信できた場合のみ到達
      print("Received response: \(response)")

    } catch {
      // タイムアウトやエラー時はこちらに
      print("Error occurred: \(error)")
    }
  }
}
