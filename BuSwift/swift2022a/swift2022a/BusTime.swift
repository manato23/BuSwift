//
//  BusTime.swift
//  swift2022a
//
//  Created by 似鳥　亜美 on 2022/11/06.
//

import Foundation


func BusTime(departure:String,arrival:String) -> (output_de_time:[String], output_ar_time:[String],keito:[String],ikisaki:[String],noriba:[String],oriba:[String],oriba_busstop:[String],noriba_busstop:[String],output_first_artime:[String],output_second_detime:[String],keito_norikae:[String],ikisaki_norikae:[String],oriba_norikae:[String],noriba_norikae:[String],norikae:[String],RTinfo:[(trip_id:[String],stop_sequence:[Int])],empty:Bool){
    //departure→de,arrival→ar
    
    let today_tripid:[String]=trip_idToday(day:DAY_OF_WEEK)//今日の曜日のtrip_id（便）
    
    let de_stopid:String=stop_idSearch(input_stop:departure)//出発バス停のstop_id（バス停を表すid）
    let ar_stopid:String=stop_idSearch(input_stop:arrival)//到着バス停のstop_id（バス停を表すid）
    
    let de_tripid:[String]=trip_idSearch(output_stop_id:de_stopid)//出発バス停を通るtrip_id（便）
    let ar_tripid:[String]=trip_idSearch(output_stop_id:ar_stopid)//到着バス停を通るtrip_id（便）
    let de_tripidtoday:[String]=de_tripid.filter{today_tripid.contains($0)}
    let de_tripid_today:[String]=Array(Set(de_tripidtoday))//出発バス停を通る今日のtrip_id（便）
    let ar_tripidtoday:[String]=ar_tripid.filter{today_tripid.contains($0)}
    let ar_tripid_today:[String]=Array(Set(ar_tripidtoday))//到着バス停を通る今日のtrip_id（便）
    let dearthrowtriptoday:[String]=de_tripid_today.filter{ar_tripid_today.contains($0)}//出発・到着バス停両方を通る今日の便
    let dearthrowtrip_today:[String]=Array(Set(dearthrowtriptoday))//出発・到着バス停両方を通る今日の便
    
    
    //直通の便情報
    var directtrip:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]=[]
    var m:Int=0
    var n:Int=0
    var fstopid:String=""
    var sstopid:String=""
    var ftime:String=""
    var stime:String=""
    var sequence:String=""
    var stopheadsign:String=""
    var detripid:String=""
    var artripid:String=""
    
    for i in dearthrowtrip_today{
    first:if(m==0){
        for stoptime in stoptimes{
            if(stoptime.trip_id==i){
                if(stoptime.stop_id.prefix(5)==de_stopid){
                    m=Int(stoptime.stop_sequence)!
                    fstopid=stoptime.stop_id
                    ftime=stoptime.arrival_time
                    sequence=stoptime.stop_sequence
                    stopheadsign=stoptime.stop_headsign
                    detripid=stoptime.trip_id
                    break first
                }
            }
        }
    }
        if(ar_stopid=="19406"){
            if(n==0){
                for stoptime in stoptimes{
                    if(stoptime.trip_id==i){
                        if(stoptime.stop_id.prefix(5)==ar_stopid){
                            n=Int(stoptime.stop_sequence)!
                            sstopid=stoptime.stop_id
                            stime=stoptime.arrival_time
                            artripid=stoptime.trip_id
                        }else{
                            n=0
                        }
                    }
                }
            }
        }else{
        second:if(n==0){
            for stoptime in stoptimes{
                if(stoptime.trip_id==i){
                    if(stoptime.stop_id.prefix(5)==ar_stopid){
                        n=Int(stoptime.stop_sequence)!
                        sstopid=stoptime.stop_id
                        stime=stoptime.arrival_time
                        artripid=stoptime.trip_id
                        break second
                    }
                }
            }
        }
        }
        if(detripid==artripid){
            if(m>0 && n>0){
                if(m<n){
                    directtrip+=[(i,"",ftime,"","",stime,fstopid,"","",sstopid,sequence,"",stopheadsign,"","乗換なし")]
                }
                m=0
                n=0
            }
        }
    }
    
    //乗り換えの便情報
    var transtrip:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]=Trans(de_stopid: de_stopid, ar_stopid: ar_stopid, de_tripid: de_tripid_today, ar_tripid: ar_tripid_today,today_tripid:today_tripid)
    
    //直通・乗り換え合わせたすべての便情報
    var alltrip:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]=directtrip+transtrip
    
    //時間順にソートされた便情報
    let outputtrip:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]=timeSort(date: alltrip)
    
    
    var routeid:[String]=[]//系統を探す際に使うroute_id
    var norikae_routeid:[String]=[]//乗り換え便の系統を探す際に使うroute_id
    for l in outputtrip{
        for i in trips{
            if(i.trip_id==l.ftripid){
                routeid+=[i.route_id]
            }
        }
    }
    var stripidireru:[String]=[]
    for i in outputtrip{
        stripidireru+=[i.stripid]
    }
    
 
    for i in stripidireru{
        if(""==i){
            norikae_routeid+=[i]
        }else{
            for j in trips{
                if(j.trip_id==i){
                    norikae_routeid+=[j.route_id]
                }
            }
        }
    }
    
    var keito:[String]=[]//系統
    var keito_norikae:[String]=[]//乗り換え便の系統
    for route in routeid{
        for i in routes{
            if(i.route_id==route){
                keito+=[i.route_short_name]
            }
        }
    }
    for route in norikae_routeid{
        if(""==route){
            keito_norikae+=[route]
        }else{
            for i in routes{
                if(i.route_id==route){
                    keito_norikae+=[i.route_short_name]
                }
            }
        }
    }
    
    print(keito_norikae)
    
    var ikisaki:[String]=[]//行き先
    var ikisaki_norikae:[String]=[]//乗り換え便の行き先
    for i in outputtrip{
        let arr = i.fstopheadsign.components(separatedBy: "(")
        let ar = i.sstopheadsign.components(separatedBy: "(")
        ikisaki+=[arr[0]]
        ikisaki_norikae+=[ar[0]]
    }
    

    //時間順にソートされた乗り場、降り場を格納
    var noribaireru:[String]=[]
    var oribaireru_norikae:[String]=[]
    for i in outputtrip{
        noribaireru+=[i.destopid]
    }
    for i in outputtrip{
        oribaireru_norikae+=[i.fstopid]
    }
    var noribaireru_norikae:[String]=[]
    var oribaireru:[String]=[]
    for i in outputtrip{
        noribaireru_norikae+=[i.sstopid]
    }
    for i in outputtrip{
        oribaireru+=[i.arstopid]
    }
    
    //乗り換えのバス停名を格納
    let oriba_busstop:[String]=busNamed(date: oribaireru_norikae)
    let noriba_busstop:[String]=busNamed(date: noribaireru_norikae)
    
    let noriba:[String]=noribaNamed(date: noribaireru)//乗り場
    let oriba_norikae:[String]=noribaNamed1(date: noribaireru_norikae)//最初の便の降り場
    let noriba_norikae:[String]=noribaNamed1(date: oribaireru_norikae)//乗り換え便の降り場
    let oriba:[String]=noribaNamed(date: oribaireru)//降り場
    
    var output_detime:[String]=[]
    var output_ftime:[String]=[]
    var output_stime:[String]=[]
    var output_artime:[String]=[]
    for i in outputtrip{
        output_detime+=[i.detime]
    }
    for i in outputtrip{
        output_ftime+=[i.ftime]
    }
    for i in outputtrip{
        output_stime+=[i.stime]
    }
    for i in outputtrip{
        output_artime+=[i.artime]
    }
    
    let output_de_time:[String]=secondDelete(time: output_detime)//出発時間
    let output_first_artime:[String]=secondDelete(time: output_ftime)//最初の便の到着時間
    let output_second_detime:[String]=secondDelete(time: output_stime)//乗り換えの便の出発時間
    let output_ar_time:[String]=secondDelete(time: output_artime)//到着時間
    
    
    var stop_norikae:[String]=[]//乗り換えバス停名
    for i in outputtrip{
        stop_norikae+=[i.fstopid]
    }
    for i in 0..<stop_norikae.count{
        for l in stops{
            if(stop_norikae[i]==l.stop_id){
                stop_norikae[i] = l.stop_name
            }
        }
    }
    
    var norikae:[String]=[]//乗り換え有無
    for i in outputtrip{
        norikae+=[i.norikae]
    }
    
    //RT関連
    var trip_id:[String]=[]
    for i in outputtrip{
        trip_id+=[i.ftripid]
    }
    var stop_sequence:[Int]=[]
    for i in outputtrip{
        var se = i.fstopsequence
        stop_sequence+=[Int(se)!]
    }
    //RTで使う情報
    let RTinfo:[(trip_id:[String],stop_sequence:[Int])] = [(trip_id,stop_sequence)]
    
    //バスがあるかないか
    var empty:Bool=false
    if(alltrip.isEmpty==true){
        empty=true
    }
    
    
    return (output_de_time,output_ar_time,keito,ikisaki,noriba,oriba,oriba_busstop,noriba_busstop,output_first_artime,output_second_detime,keito_norikae,ikisaki_norikae,oriba_norikae,noriba_norikae,norikae,RTinfo,empty)
    
}

/*
 関数名：totalTime
 引数：output_de_time,output_ar_time(型String,配列)
 返り値：totalTime(型String,配列)
 内容：出発時間から到着時間までの合計所要時間を求める
 */
func totalTime(output_de_time:[String],output_ar_time:[String])->(totalHour:[String],totalMin:[String]){
    //時間を入れる空の配列を用意
    var deHour:[Int] = []
    var arHour:[Int] = []
    //分を入れる空の配列を用意
    var deMin:[Int] = []
    var arMin:[Int] = []
    //文字列から時間を分で取得
    for output_de_time in output_de_time{
        deHour.append(Int(output_de_time.prefix(2).description)!*60)//分に直す
    }
    for output_ar_time in output_ar_time{
        arHour.append(Int(output_ar_time.prefix(2).description)!*60)//分に直す
    }
    //文字列から分を取得
    for output_de_time in output_de_time{
        deMin.append(Int(output_de_time.suffix(2).description)!)//分なのでそのまま取得
    }
    for output_ar_time in output_ar_time{
        arMin.append(Int(output_ar_time.suffix(2).description)!)//分なのでそのまま取得
    }
    //出発時間と到着時間の分のの合計を入れる配列
    var de_sum:[Int] = []
    var ar_sum:[Int] = []
    //出発時間の時間と分の合計（分）を求める
    for i in 0..<deHour.count {
        de_sum.append(deHour[i]+deMin[i])
    }
    //到着時間の時間と分の合計（分）を求める
    for i in 0..<arHour.count {
        ar_sum.append(arHour[i]+arMin[i])
    }
    //分で引き算＝所要時間が分で求められる
    var hiki:[Int] = []
    for i in 0..<ar_sum.count{
        hiki.append(ar_sum[i] - de_sum[i])
    }
    //所要時間の合計を求める
    var totalHour:[String] = []
    var totalMin:[String] = []
    for i in 0..<hiki.count{
        totalHour.append(String(hiki[i]/60))
        totalMin.append(String(hiki[i]%60))
    }
    //配列を返す
    return (totalHour,totalMin)
}
/*関数名：stop_idSearch
 引数：input_stop(型String)
 返り値：stop_id(型String)
 内容：stopsファイルから入力されたバス停名と同じstop_name(バス停名)のstop_id(バス停を表すid)を探す */

func stop_idSearch(input_stop:String) -> String{
    var stop_id:String="0"
    for stop in stops {
        if(stop_id=="0"){
            if(stop.stop_name==input_stop){
                stop_id=stop.stop_id
            }
        }
    }
    return stop_id
}

/*関数名：trip_idSearch
 引数：output_stop_id(型[String])
 返り値：trip_id(型[String])
 　内容：stop_id(バス停を表すid)を通るそれぞれのtrip_id(便)を探す */
func trip_idSearch(output_stop_id:String) -> [String]{
    var trip_id:[String]=[]
    for stoptime in stoptimes{
        if(stoptime.stop_id.prefix(5)==output_stop_id){
            trip_id+=[stoptime.trip_id]
        }
    }
    return trip_id
}

/*関数名：getDayOfWeek
 引数：Date型
 返り値：(型Int)
 内容：その日が何曜日なのか取得する
 */
func getDayOfWeek(_ date : Date) -> Int{
    enum WeekDay: Int {
        case sunday = 1
        case monday = 2
        case tuesday = 3
        case wednesday = 4
        case thursday = 5
        case friday = 6
        case saturday = 7
    }
    let weekday = WeekDay(rawValue: Calendar.current.component(.weekday, from: date))!
    
    switch weekday {
    case .sunday:
        return 6
    case .monday:
        return 0
    case .tuesday:
        return 1
    case .wednesday:
        return 2
    case .thursday:
        return 3
    case .friday:
        return 4
    case .saturday:
        return 5
    }
}
/*関数名：trip_idToday
 引数：day(型Int)
 返り値：tripid_today(型[String])
 　　内容：各曜日のtrip_idを抽出して、該当する曜日のtrip_idを渡す
 */
func trip_idToday(day:Int) -> [String]{
    var serviceid_sunday:[String]=[]
    var serviceid_monday:[String]=[]
    var serviceid_tuesday:[String]=[]
    var serviceid_wednesday:[String]=[]
    var serviceid_thursday:[String]=[]
    var serviceid_friday:[String]=[]
    var serviceid_saturday:[String]=[]
    var serviceid_today:[String]=[]
    var tripid_today:[String]=[]
    
    for i in calendar1{
        if(Int(i.monday)==1){
            serviceid_monday+=[i.service_id]
        }
        if(Int(i.tuesday)==1){
            serviceid_tuesday+=[i.service_id]
        }
        if(Int(i.wednesday)==1){
            serviceid_wednesday+=[i.service_id]
        }
        if(Int(i.thursday)==1){
            serviceid_thursday+=[i.service_id]
        }
        if(Int(i.friday)==1){
            serviceid_friday+=[i.service_id]
        }
        if(Int(i.saturday)==1){
            serviceid_saturday+=[i.service_id]
        }
        if(Int(i.sunday)==1){
            serviceid_sunday+=[i.service_id]
        }
    }
    
    if(day==0){
        serviceid_today=serviceid_monday
    }
    if(day==1){
        serviceid_today=serviceid_tuesday
    }
    if(day==2){
        serviceid_today=serviceid_wednesday
    }
    if(day==3){
        serviceid_today=serviceid_thursday
    }
    if(day==4){
        serviceid_today=serviceid_friday
    }
    if(day==5){
        serviceid_today=serviceid_saturday
    }
    if(day==6){
        serviceid_today=serviceid_sunday
    }
    
    for i in trips{
        for l in serviceid_today{
            if(i.service_id==l){
                tripid_today+=[i.trip_id]
            }
        }
    }
    return tripid_today
}

/*関数名：secondDelete
 引数：time(型[String])
 返り値：hm(型[String])
 　　内容：時刻の秒を消去する
 */
func secondDelete(time:[String]) -> [String]{
    var hms=""
    var hm:[String]=[]
    
    for time in time{
        hms=time
        hm.append(String(hms.prefix(5)))
    }
    return hm
}

/*関数名：timeSort
 引数：date(型[[String]])
 返り値：timesorted(型[[String]])
 　　内容：時刻を昇順に並べる関数
 */
func timeSort(date:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]) -> [(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]{
    var timesorted:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)] = []
    
    timesorted = date.sorted {
        if $0.detime == $1.detime{
            return $0.artime < $1.artime
        }else {
            return $0.detime < $1.detime
        }
    }
    return timesorted
}

/*関数名：noribaNamed
 引数：date(型[[String]])
 返り値：noriba(型[String])
 　　内容：stopsからstop_idに対応する乗り場名(stop_desc)を探す
 */
func noribaNamed(date:[String]) -> [String]{
    var noriba:[String]=[]
    
    for i in 0..<date.count{
        for stop in stops{
            if(stop.stop_id==date[i]){
                noriba+=[stop.stop_desc]
            }
        }
    }
    return noriba
}
/*関数名：noribaNamed1
 引数：date(型[[String]])
 返り値：noriba(型[String])
 　　内容：stopsからstop_idに対応する乗り場名(stop_desc)を探す
 */
func noribaNamed1(date:[String]) -> [String]{
    var noriba:[String]=[]
    
    for i in date{
        if(""==i){
            noriba+=[i]
        }else{
            for stop in stops{
                if(stop.stop_id==i){
                    noriba+=[stop.stop_desc]
                }
            }
        }
    }
    return noriba
}
/*関数名：busNamed
 引数：date(型[[String]])
 返り値：busstop(型[String])
 　　内容：stopsからstop_idに対応する乗り場名(stop_desc)を探す
 */
func busNamed(date:[String]) -> [String]{
    var busstop:[String]=[]
    
    for i in date{
        if(""==i){
            busstop+=[i]
        }else{
            for stop in stops{
                if(stop.stop_id==i){
                    busstop+=[stop.stop_name]
                }
            }
        }
    }
    return busstop
}

/*関数名：flowBus
 引数：date(型[[String]])
 返り値：noriba(型[[String]])
 　　内容：出発(first)→到着(last)バス停の順番で通るtrip_id(便)を探す
 */
func flowBus(first:String, last:String, de_tripid_today:[String],today_tripid:[String]) -> [(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String,sequence:String,headsign:String)]{
    
    
    let ar_tripid:[String]=trip_idSearch(output_stop_id:last)//到着バス停を通るtrip_id（便）
    let ar_tripidtoday:[String]=ar_tripid.filter{today_tripid.contains($0)}
    let ar_tripid_today:[String]=Array(Set(ar_tripidtoday))//到着バス停を通る今日のtrip_id（便）
    let dearthrowtriptoday:[String]=de_tripid_today.filter{ar_tripid_today.contains($0)}//出発・到着バス停両方を通る今日の便
    let dearthrowtrip_today:[String]=Array(Set(dearthrowtriptoday))//出発・到着バス停両方を通る今日の便
    
    
    var tripidd:[(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String,sequence:String,headsign:String)]=[]
    var m:Int=0
    var n:Int=0
    var fstopid:String=""
    var sstopid:String=""
    var ftime:String=""
    var stime:String=""
    var sequence:String=""
    var stopheadsign:String=""
    
    for tripid in dearthrowtrip_today{
    first:if(m==0){
        for stoptime in stoptimes{
            if(stoptime.trip_id==tripid){
                if(stoptime.stop_id.prefix(5)==first){
                    m=Int(stoptime.stop_sequence)!
                    fstopid=stoptime.stop_id
                    ftime=stoptime.arrival_time
                    sequence=stoptime.stop_sequence
                    stopheadsign=stoptime.stop_headsign
                    break first
                }
            }
        }
    }
        if(last=="19406"){
            if(n==0){
                for stoptime in stoptimes{
                    if(stoptime.trip_id==tripid){
                        if(stoptime.stop_id.prefix(5)==last){
                            n=Int(stoptime.stop_sequence)!
                            sstopid=stoptime.stop_id
                            stime=stoptime.arrival_time
                        }
                    }
                }
            }
        }else{
        second:if(n==0){
            for stoptime in stoptimes{
                if(stoptime.trip_id==tripid){
                    if(stoptime.stop_id.prefix(5)==last){
                        n=Int(stoptime.stop_sequence)!
                        sstopid=stoptime.stop_id
                        stime=stoptime.arrival_time
                        break second
                    }
                }
            }
        }
        }
        if(m>0 && n>0){
            if(m<n){
                tripidd+=[(tripid,fstopid,sstopid,ftime,stime,sequence,stopheadsign)]
            }
            m=0
            n=0
        }
    }
    return tripidd
}

/*関数名：flowBus
 引数：date(型[[String]])
 返り値：noriba(型[[String]])
 　　内容：出発(first)→到着(last)バス停の順番で通るtrip_id(便)を探す
 */
func flowBus1(first:String, last:String, ar_tripid_today:[String],today_tripid:[String]) -> [(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String,sequence:String,headsign:String)]{
    
    
    let de_tripid:[String]=trip_idSearch(output_stop_id:first)//到着バス停を通るtrip_id（便）
    let de_tripidtoday:[String]=de_tripid.filter{today_tripid.contains($0)}
    let de_tripid_today:[String]=Array(Set(de_tripidtoday))//到着バス停を通る今日のtrip_id（便）
    let dearthrowtriptoday:[String]=de_tripid_today.filter{ar_tripid_today.contains($0)}//出発・到着バス停両方を通る今日の便
    let dearthrowtrip_today:[String]=Array(Set(dearthrowtriptoday))//出発・到着バス停両方を通る今日の便
    
    
    var tripidd:[(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String,sequence:String,headsign:String)]=[]
    var m:Int=0
    var n:Int=0
    var fstopid:String=""
    var sstopid:String=""
    var ftime:String=""
    var stime:String=""
    var sequence:String=""
    var stopheadsign:String=""
    
    for tripid in dearthrowtrip_today{
    first:if(m==0){
        for stoptime in stoptimes{
            if(stoptime.trip_id==tripid){
                if(stoptime.stop_id.prefix(5)==first){
                    m=Int(stoptime.stop_sequence)!
                    fstopid=stoptime.stop_id
                    ftime=stoptime.arrival_time
                    sequence=stoptime.stop_sequence
                    stopheadsign=stoptime.stop_headsign
                    break first
                }
            }
        }
    }
        if(last=="19406"){
            if(n==0){
                for stoptime in stoptimes{
                    if(stoptime.trip_id==tripid){
                        if(stoptime.stop_id.prefix(5)==last){
                            n=Int(stoptime.stop_sequence)!
                            sstopid=stoptime.stop_id
                            stime=stoptime.arrival_time
                        }
                    }
                }
            }
        }else{
        second:if(n==0){
            for stoptime in stoptimes{
                if(stoptime.trip_id==tripid){
                    if(stoptime.stop_id.prefix(5)==last){
                        n=Int(stoptime.stop_sequence)!
                        sstopid=stoptime.stop_id
                        stime=stoptime.arrival_time
                        break second
                    }
                }
            }
        }
        }
        if(m>0 && n>0){
            if(m<n){
                tripidd+=[(tripid,fstopid,sstopid,ftime,stime,sequence,stopheadsign)]
            }
            m=0
            n=0
        }
    }
    return tripidd
}


func Trans(de_stopid:String,ar_stopid:String,de_tripid:[String],ar_tripid:[String],today_tripid:[String]) -> ([(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]){
    
    let calender = Calendar.init(identifier: .gregorian)
    let dateFormatter = DateFormatter()
    dateFormatter.dateFormat = "HH:mm:ss"
    
    /*
     //乗り換えバス停抽出（出発バス停を通る便の全部のバス停、到着バス停を通る便の全部のバス停、重なるバス停を格納）
     var dethrow_stopid:[String]=[]
     var arthrow_stopid:[String]=[]
     
     for i in stoptimes{
     for k in de_tripid{
     if(i.trip_id==k){
     if(!(dethrow_stopid.contains(String(i.stop_id.prefix(5))))){
     dethrow_stopid+=[String(i.stop_id.prefix(5))]
     }
     }
     }
     for k in ar_tripid{
     if(i.trip_id==k){
     if(!(arthrow_stopid.contains(String(i.stop_id.prefix(5))))){
     arthrow_stopid+=[String(i.stop_id.prefix(5))]
     }
     }
     }
     }
     
     var transfer_stopidd:[String]=[]
     for i in dethrow_stopid{
     for l in arthrow_stopid{
     if(i.prefix(5)==l.prefix(5)){
     transfer_stopidd+=[String(i.prefix(5))]
     }
     }
     }*/
    
    //let transfer_stopid:[String]=Array(Set(transfer_stopidd))//重なっているバス停、乗り換えバス停候補
    
    //let trip:[String]=de_tripid+ar_tripid
    //let transfer_tripid:[String]=Array(Set(trip))
    
    var transfer_stopid:[String]=[]
    var zyoho:[(ftripid:String,stripid:String,detime:String,ftime:String,stime:String,artime:String,destopid:String,fstopid:String,sstopid:String,arstopid:String,fstopsequence:String,sstopsequence:String,fstopheadsign:String,sstopheadsign:String,norikae:String)]=[]
    
    
    if((de_stopid=="14013" && ar_stopid=="14007")||(ar_stopid=="14013" && de_stopid=="14007")){
        return zyoho//亀田支所前、五稜郭
    }else if((de_stopid=="14013" && ar_stopid=="14023")||(ar_stopid=="14013" && de_stopid=="14023")){
        return zyoho//亀田支所前、未来大
    }else if((de_stopid=="11003" && ar_stopid=="14007")||(ar_stopid=="11003" && de_stopid=="14007")){
        return zyoho//函館駅前、五稜郭
    }else if(de_stopid=="14007" || ar_stopid=="14007"){
        transfer_stopid=["14013"]
    }else if(de_stopid=="14013" || ar_stopid=="14013"){
        transfer_stopid=["14007"]
    }else{
        transfer_stopid=["14013","14007"]//亀田支所前、五稜郭
    }
    
    
    
    for i in transfer_stopid{
        //最初のバス停→乗り換えバス停の便//出発地→乗り換えバス停を通る便
        let te:[(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String ,sequence:String,headsign:String)]=flowBus(first: de_stopid, last: i, de_tripid_today: de_tripid,today_tripid:today_tripid)
        //乗り換えバス停→最後のバス停の便//乗り換えバス停→到着バス停を通る便
        let tu:[(tripid:String,fstopid:String,sstopid:String,ftime:String,stime:String ,sequence:String,headsign:String)]=flowBus1(first: i, last: ar_stopid, ar_tripid_today: ar_tripid,today_tripid:today_tripid)
        
        for l in te{
            for k in tu{
                if(l.stime<k.ftime){
                    if(calender.dateComponents([.minute], from: dateFormatter.date(from: l.stime)!, to: dateFormatter.date(from: k.ftime)!
                                              ).minute!>1 && calender.dateComponents([.minute], from: dateFormatter.date(from: l.stime)!, to: dateFormatter.date(from: k.ftime)!
                                                                                    ).minute!<20){
                        zyoho+=[(l.tripid,k.tripid,l.ftime,l.stime,k.ftime,k.stime,l.fstopid,l.sstopid,k.fstopid,k.sstopid,l.sequence,k.sequence,l.headsign,k.headsign,"乗換１回")]
                    }
                }
            }
        }
    }
    return zyoho
}
