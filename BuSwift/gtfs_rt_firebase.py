# -*- coding: utf-8 -*-

!pip install pandas
!pip install --upgrade gtfs-realtime-bindings
!pip install notebook
!pip install firebase-admin

from google.colab import drive
drive.mount('/content/drive')

from google.transit import gtfs_realtime_pb2
import urllib.request, urllib.error
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

#firebaseの秘密鍵
cred = credentials.Certificate('秘密鍵のパス')

#firebaseへのアクセス
firebase_admin.initialize_app(cred, {
    'databaseURL':'firebaseのURL',
    'databaseAuthVariableOverride':{
        'uid': 'my-service-worker'
    }
})

#データのリファレンスにアクセス
data_ref = db.reference('gtfs-rt')

#GTFS-RTの公開されているURL
url = 'リアルタイムデータのURL'
#カラム名の宣言
list_df = pd.DataFrame(columns=['trip_id','current_stop_sequence','current_status'])
feed = gtfs_realtime_pb2.FeedMessage()

#データダウンロードとフォーマット変換
with urllib.request.urlopen(url) as res: #データダウンロード
  feed.ParseFromString(res.read()) #プロトコルバッファをDeserialize
  for entity in feed.entity:
    if (entity.vehicle.position.longitude != 0.0 and entity.vehicle.trip.trip_id != ""):
      tmp_se = pd.Series( [
          entity.vehicle.trip.trip_id,          #路線番号
          entity.vehicle.current_stop_sequence, #現在の停車順序インデックス
          entity.vehicle.current_status         #現在の停留所に対するステータス
      ], index=list_df.columns )
      list_df = list_df.append( tmp_se, ignore_index=True)

#データの中身を空にする
data_ref.delete()

#データを格納する
for i in range(len(list_df)):
  name = "data" + str(i)
  data_ref.child(name).set({
      "trip_id": list_df['trip_id'][i],
      "current_stop_sequence": list_df['current_stop_sequence'][i],
      "current_status": list_df['current_status'][i]
})
