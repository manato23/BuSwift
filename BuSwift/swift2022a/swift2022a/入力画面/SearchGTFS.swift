//
//  SearchGTFS.swift
//  swift2022a
//
//  Created by 涌井春那 on 2022/11/05.
//

import Foundation
/*関数名：SearchGTFS
  引数：textField(型:String)
  返り値：stopName(型:String)
  内容：textFieldと同じ名前のstop_name(バス停名)があれば、値を返す
 */
func SearchGTFS(textField:String) -> String{
    //stops.jsonの読み込み
    let stops: [Stops] = Bundle.main.decodeJSON("stops.json")
    //stopNameを保存する変数
    var stopName: [String] = []
    //存在したstopNameを返すための変数
    var stopExist: String = ""
    //stopsからstopNameに格納する
    for stop in stops{
        stopName.append(stop.stop_name)
    }
    //重複を削除する
    let uniqueStopName = Array(Set(stopName))
    //stopNameにtextFieldの値があれば、stopExistにExsitを、なければnoExistを格納する。
    //候補を出すなら、ここのfor文を違う感じに設定する必要がありそう。
    for stopName in uniqueStopName {
        if(textField==stopName){
            stopExist = stopName
            return stopName
        }
        else{
            stopExist = ""
        }
    }
    if(stopExist.isEmpty==true){
        return ""
    }
    return ""
}
