//
//  Idokeido.swift
//  swift2022a
//
//  Created by 似鳥　亜美 on 2022/11/17.
//

import Foundation

func Idokeido(){
    for stop in stops{
        print(stop.stop_name,stop.stop_desc,stop.stop_id,stop.stop_lat,stop.stop_lon)
            //    バス停名　　,    乗り場名    ,   バス停ID  ,   　緯度     ,     経度
    }
}

func Idokeido1(){
    var idokeido:[(busname:String,ido:String,keido:String)]=[]
    for stop in stops{
        idokeido+=[(stop.stop_name,stop.stop_lat,stop.stop_lon)]
    }
    for i in idokeido{
        print(i.busname,i.ido,i.keido)
    }
    
    print(idokeido)
}


func Idokeido2(){
    var busname:[String]=[]
    var ido:[String]=[]
    var keido:[String]=[]
    
    for stop in stops{
        busname+=[stop.stop_name]
        ido+=[stop.stop_lat]
        keido+=[stop.stop_lon]
    }

    for i in 0..<busname.count{
        print(busname[i],ido[i],keido[i])
    }
    
}
