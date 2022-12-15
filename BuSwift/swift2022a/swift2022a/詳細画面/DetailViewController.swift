//
//  DetailViewController.swift
//  swift2022a
//
//  Created by 涌井春那 on 2022/11/13.
//

import UIKit
let obiHeight = 50 // 帯の高さ設定
let leftPadding = 20 //左端の余白設定
let busInfoHeight = 140 //BusInfoViewの高さ設定
import FirebaseDatabase

class DetailViewController: UIViewController {
    // 乗り換えあるなし共通
    @IBOutlet weak var Modoru: UIImageView! //戻るボタン
    @IBOutlet weak var Home: UIImageView! //ホームボタン
    @IBOutlet weak var BusTotalPrice: UILabel!      // バスの合計料金
    @IBOutlet weak var TotalReTime: UILabel!        // バスの合計所要時間
    @IBOutlet weak var BusTransferNum: UILabel!     // バスの乗換回数
    @IBOutlet weak var dateLabel: UILabel! //日にち
    @IBOutlet weak var keiroLabel: UILabel! //経路情報
    // 乗り換えなしの場合
    @IBOutlet weak var StartBusStopName: UILabel!   // 出発バス停名
    @IBOutlet weak var ArrivalBusStopName: UILabel! // 到着バス停名
    @IBOutlet weak var StartBusTime: UILabel!       // 出発時間
    @IBOutlet weak var ArrivalBusTime: UILabel!     // 到着時間
    @IBOutlet weak var ArivalPlatformName: UILabel! // おりば
    @IBOutlet weak var DeparturePlatFormName: UILabel! //のりば
    @IBOutlet weak var BusLineage: UILabel!         // 系統
    @IBOutlet weak var BusDestination: UILabel!     // バスの行き先
    @IBOutlet weak var BusPrice: UILabel!           // バスの料金
    @IBOutlet weak var BusPredict: UILabel! //「つ前のバス停を出発」
    var DepartureObi = UIView() //出発時間用の帯
    var ArrivalObi = UIView() //到着時間用の帯
    var KeiroInfoView = UIView() // 経路情報が表示されるバックグラウンドView
    var BusInfoView = UIView()  //バスの情報（系統、行先、金額、いつ出発か？）のバックグラウンドView
    // 乗り換えある場合
    var DepartureTransferObi = UIView() // 乗り換えある場合の出発時間用の帯
    var ArrivalTransferObi = UIView() //　乗り換えある場合の到着時間用の帯
    var TransferTimeView = UIView() // 乗り換えある場合の乗り換え時間の帯
    var BusInfoTransferView = UIView()  // 乗り換えある場合のバスの情報（系統、行先、金額、いつ出発か？）のバックグラウンドView
    var StartTransferBusStopName = UILabel()   // 乗り換えある場合の出発バス停名
    var ArrivalTransferBusStopName = UILabel() // 乗り換えある場合の到着バス停名
    var TransferTime = UILabel() // 乗り換え時間
    var StartTransferBusTime = UILabel()       // 乗り換えある場合の出発時間
    var ArrivalTransferBusTime = UILabel()     // 乗り換えある場合の到着時間
    var ArivalTransferPlatformName = UILabel() // 乗り換えある場合のおりば
    var DepartureTransferPlatFormName = UILabel() //乗り換えある場合ののりば
    var TransferBusLineage = UILabel()         // 乗り換えある場合の系統
    var TransferBusDestination = UILabel()    // 乗り換えある場合のバスの行き先
    var TransferBusPrice = UILabel()          // 乗り換えある場合のバスの料金
    var TransferBusPredict = UILabel() // 乗り換えある場合の「つ前のバス停を出発」
    
    // Homeから受け取る用の変数
    var startStr = ""       // 出発バス停受け取り
    var arrStr = ""         // 到着バス停受け取り
    var startTime = ""      // 出発時間受け取り
    var arrTime = ""        // 到着時間受け取り
    var reTimeHour = ""     // 合計所要時間（時間）受け取り
    var reTimeMin = ""      // 合計所要時間（分）受け取り
    var busDes = ""         // 行き先受け取り
    var busLine = ""        // 系統受け取り
    var oPlatformName = ""  // 乗り場受け取り
    var nPlatformName = ""  // 降り場受け取り
    var dateString = "" //日にち受け取り
    var norikaeBool = ""//乗換するか判断用
    //乗換ある時
    var startTraStr = ""       // 乗換ある時の出発バス停受け取り
    var arrTraStr = ""         // 乗換ある時の到着バス停受け取り
    var startTraTime = ""      // 乗換ある時の出発時間受け取り
    var arrTraTime = ""        // 乗換ある時の到着時間受け取り
    var busTraDes = ""         // 乗換ある時の行き先受け取り
    var busTraLine = ""        // 乗換ある時の系統受け取り
    var oTraPlatformName = ""  // 乗換ある時の乗り場受け取り
    var nTraPlatformName = ""  // 乗換ある時の降り場受け取り
    
    //一覧画面から受け取る用の変数
    var tripId = ""
    var stopSequence = 0
    
    //firebaseのリファレンス
    var ref: DatabaseReference! = Database.database().reference()
    //RTのデータを扱うための変数
    var index: Int = 0       //配列ないのデータを指定するインデックス
    var id: [String] = []    //GTFS-RTのtrip_idを入れる
    var sequence: [Int] = [] //GTFS-RTのcurrent_stop_sequenceを入れる
    var status: [Int] = []   //GTFS-RTのcurrent_statusを入れる
    var prediction: Int = 0  //推測結果
    
    /*
     関数名：getData
     引数：ref(型:DatabaseReference)
     返り値：なし
     内容：firebaseのデータをid,sequence,statusにそれぞれ格納する
     */
    func getData(ref: DatabaseReference, completion: @escaping () -> Void){
        ref.child("gtfs-rt").getData(completion: {error, snapshot in
            guard error == nil else{
                print(error!.localizedDescription)
                return
            }
            if let allData = snapshot?.value as? NSDictionary{
                for(_, value) in allData{
                    if let data = value as? [String: AnyObject]{
                        self.id += [data["trip_id"] as? String ?? "Unknown"]
                        self.sequence += [data["current_stop_sequence"] as? Int ?? -1]
                        self.status += [data["current_status"] as? Int ?? -1]
                    }
                }
                completion()
            }
        })
    }
    
    //スクリーンの幅
    let screenWidth = Int(UIScreen.main.bounds.size.width)
    //スクリーンの高さ
    let screenHeight = Int(UIScreen.main.bounds.size.height)
    let labelHeight = 30 //ラベルの高さ設定
    let busInfoLeftPadding = 40 //BusInfoViewの左の余白設定
    let topbottomPadding = 10 //BusInfoViewの上下の余白
    let transferYstart = busInfoHeight+obiHeight*2
    
    override func viewDidLoad() {
        super.viewDidLoad()
        //UIScrollViewのインスタンス作成
        let scrollView = UIScrollView()
        //scrollViewの大きさを設定
        scrollView.frame = .init(x: 0, y: 270,width: screenWidth, height: screenHeight)
        //最後の場所を保存する（最初は検索結果の下のところからスタート）
        //スクロール領域の設定
        scrollView.contentSize = CGSize(width:screenWidth, height:screenHeight+50)
        scrollView.backgroundColor = UIColor(hex: "F0F0FC")
        //scrollViewをviewのSubViewとして追加
        self.view.addSubview(scrollView)
        // View表示と同時に、一時的にスクロールバー表示
        scrollView.flashScrollIndicators()
        //　乗り換えあるなし共通
        //　戻るの場所設定
        Modoru.isUserInteractionEnabled = true
        Modoru.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(ModoruViewTapped(_:))))
        Modoru.frame = CGRect(x:15, y:70,
                              width:70, height:50)
        self.view.addSubview(Modoru)
        Home.isUserInteractionEnabled = true
        Home.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(HomeViewTapped(_:))))
        //　Homeの場所設定
        Home.frame = CGRect(x:screenWidth - 85, y:70,
                              width:70, height:50)
        self.view.addSubview(Home)
        // ラベルの位置とサイズを設定
        keiroLabel.frame = CGRect(x:0, y:120, width:screenWidth, height:labelHeight)
        // 文字を中央にalignする
        keiroLabel.textAlignment = NSTextAlignment.center
        // ラベルの文字サイズを設定
        keiroLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 20)
        // viewにラベルを追加
        self.view.addSubview(keiroLabel)
        // 日にちを受け取る
        dateLabel.text = dateString
        // テキストの色
        dateLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        dateLabel.frame = CGRect(x:0, y:90, width:screenWidth, height:labelHeight)
        // 文字を中央にalignする
        dateLabel.textAlignment = NSTextAlignment.center
        // ラベルの文字サイズを設定
        dateLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        // viewにラベルを追加
        self.view.addSubview(dateLabel)
        // 経路情報 乗り換えあるなし共通
        KeiroInfoView.frame = CGRect(x: 0, y: 160,width: screenWidth, height: 110)//経路情報のフレーム
        KeiroInfoView.backgroundColor = .white
        self.view.addSubview(KeiroInfoView)
        BusTransferNum.frame = CGRect(x:leftPadding, y: topbottomPadding,width: screenWidth-leftPadding, height: labelHeight)
        BusTransferNum.textAlignment = NSTextAlignment.left
        BusTransferNum.textColor = .black
        BusTotalPrice.frame = CGRect(x: leftPadding, y: topbottomPadding+labelHeight,width: screenWidth-leftPadding, height: labelHeight)
        BusTotalPrice.textAlignment = NSTextAlignment.left
        BusTotalPrice.textColor = .black
        TotalReTime.frame = CGRect(x: leftPadding, y: topbottomPadding+labelHeight+labelHeight,width: screenWidth-leftPadding, height: labelHeight)
        TotalReTime.textAlignment = NSTextAlignment.left
        TotalReTime.textColor = .black
        KeiroInfoView.addSubview(BusTransferNum)
        KeiroInfoView.addSubview(BusTotalPrice)
        KeiroInfoView.addSubview(TotalReTime)
        // 乗り換えなしの時
        DepartureObi.frame = CGRect(x:0, y:0, width:screenWidth, height:obiHeight)//出発時間の帯
        DepartureObi.backgroundColor = UIColor(hex:"3F6AB2")
        scrollView.addSubview(DepartureObi)
        BusInfoView.frame = CGRect(x: 0, y: obiHeight,width: screenWidth, height: busInfoHeight)
        BusInfoView.backgroundColor = .white
        scrollView.addSubview(BusInfoView)
        ArrivalObi.frame = CGRect(x:0, y:obiHeight+busInfoHeight, width:screenWidth, height:obiHeight)
        ArrivalObi.backgroundColor = UIColor(hex:"3F6AB2")
        scrollView.addSubview(ArrivalObi)
        StartBusTime.frame = CGRect(x:leftPadding, y:0, width:50, height:obiHeight)
        StartBusTime.textAlignment = NSTextAlignment.left
        StartBusTime.textColor = .white
        ArrivalBusTime.frame = CGRect(x:leftPadding,y:0,width:50,height: obiHeight)
        ArrivalBusTime.textAlignment = NSTextAlignment.left
        ArrivalBusTime.textColor = .white
        StartBusStopName.frame = CGRect(x:75, y:0, width:150, height:obiHeight)
        StartBusStopName.textAlignment = NSTextAlignment.left
        StartBusStopName.textColor = .white
        ArrivalBusStopName.frame = CGRect(x:75, y:0, width:150, height:obiHeight)
        ArrivalBusStopName.textAlignment = NSTextAlignment.left
        ArrivalBusStopName.textColor = .white
        DepartureObi.addSubview(StartBusTime)
        DepartureObi.addSubview(StartBusStopName)
        ArrivalObi.addSubview(ArrivalBusTime)
        ArrivalObi.addSubview(ArrivalBusStopName)
        BusInfoView.drawLine(start: CGPoint(x:leftPadding,y:0), end: CGPoint(x:leftPadding,y:busInfoHeight), color: UIColor(hex:"5A7FBB"), weight: 5, rounded: false)
        BusDestination.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding,width: screenWidth-busInfoLeftPadding, height: labelHeight)
        BusDestination.textAlignment = NSTextAlignment.left
        BusDestination.textColor = .black
        BusLineage.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight,width: screenWidth-busInfoLeftPadding, height: labelHeight)
        BusLineage.textAlignment = NSTextAlignment.left
        BusLineage.textColor = .black
        BusPrice.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight+labelHeight,width: screenWidth-busInfoLeftPadding, height: labelHeight)
        BusPrice.textAlignment = NSTextAlignment.left
        BusPrice.textColor = .black
        BusPredict.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight+labelHeight+labelHeight,width: screenWidth-busInfoLeftPadding, height: labelHeight)
        BusPredict.textAlignment = NSTextAlignment.left
        BusPredict.textColor = .black
        BusInfoView.addSubview(BusDestination)
        BusInfoView.addSubview(BusLineage)
        BusInfoView.addSubview(BusPrice)
        BusInfoView.addSubview(BusPredict)
        DeparturePlatFormName.frame = CGRect(x:160, y:0, width:screenWidth-160, height:obiHeight)
        DeparturePlatFormName.textAlignment = NSTextAlignment.left
        DeparturePlatFormName.textColor = .white
        DepartureObi.addSubview(DeparturePlatFormName)
        ArivalPlatformName.frame = CGRect(x: 160, y: 0,width: screenWidth-150, height: obiHeight)
        ArivalPlatformName.textAlignment = NSTextAlignment.left
        ArivalPlatformName.textColor = .white
        ArrivalObi.addSubview(ArivalPlatformName)
        if(norikaeBool=="乗換なし"){
            // 乗り換えない場合のテキストの設定
            StartBusStopName.text = startStr // 出発バス停名ラベルのtextを設定
            ArrivalBusStopName.text = arrStr // 到着バス停名ラベルのtextを設定
            StartBusTime.text = startTime    // 出発時刻ラベルのtextを設定
            ArrivalBusTime.text = arrTime    // 到着時刻ラベルのtextを設定
            DeparturePlatFormName.text = nPlatformName //のりばラベルのtextを設定
            ArivalPlatformName.text = oPlatformName           // おりばラベルのtextを設定
            BusLineage.text = busLine        // 系統ラベルのtextを設定
            BusDestination.text = busDes + " 行き"     // 行き先ラベルのtextを設定
            BusPrice.text = "料金"      // 料金ラベルのtextを設定
            scrollView.showsVerticalScrollIndicator = false//スクロールバー非表示
            scrollView.isScrollEnabled = false//スクロール禁止
        }
        if(norikaeBool=="乗換１回"){
            // 乗り換えある場合
            TransferTimeView.frame = CGRect(x:0, y:transferYstart, width:screenWidth, height:obiHeight)
            TransferTimeView.backgroundColor = .white
            scrollView.addSubview(TransferTimeView)
            DepartureTransferObi.frame = CGRect(x:0, y:transferYstart+obiHeight, width:screenWidth, height:obiHeight)
            DepartureTransferObi.backgroundColor = UIColor(hex:"3F6AB2")
            scrollView.addSubview(DepartureTransferObi)
            BusInfoTransferView.frame = CGRect(x: 0, y: transferYstart+obiHeight+obiHeight,width: screenWidth, height: busInfoHeight)
            BusInfoTransferView.backgroundColor = .white
            scrollView.addSubview(BusInfoTransferView)
            ArrivalTransferObi.frame = CGRect(x:0, y:transferYstart+obiHeight+obiHeight+busInfoHeight, width:screenWidth, height:obiHeight)
            ArrivalTransferObi.backgroundColor = UIColor(hex:"3F6AB2")
            scrollView.addSubview(ArrivalTransferObi)
            let drawView = DrawView(frame: self.view.bounds)
            TransferTimeView.addSubview(drawView)
            TransferTime.frame = CGRect(x:busInfoLeftPadding, y:0, width:screenWidth-leftPadding, height:obiHeight)
            TransferTime.textAlignment = NSTextAlignment.left
            TransferTime.textColor = .black
            TransferTimeView.addSubview(TransferTime)
            StartTransferBusTime.frame = CGRect(x:leftPadding, y:0, width:50, height:obiHeight)
            StartTransferBusTime.textAlignment = NSTextAlignment.left
            StartTransferBusTime.textColor = .white
            ArrivalTransferBusTime.frame = CGRect(x:leftPadding,y:0,width:50,height: obiHeight)
            ArrivalTransferBusTime.textAlignment = NSTextAlignment.left
            ArrivalTransferBusTime.textColor = .white
            StartTransferBusStopName.frame = CGRect(x:75, y:0, width:150, height:obiHeight)
            StartTransferBusStopName.textAlignment = NSTextAlignment.left
            StartTransferBusStopName.textColor = .white
            ArrivalTransferBusStopName.frame = CGRect(x:75, y:0, width:150, height:obiHeight)
            ArrivalTransferBusStopName.textAlignment = NSTextAlignment.left
            ArrivalTransferBusStopName.textColor = .white
            DepartureTransferObi.addSubview(StartTransferBusTime)
            DepartureTransferObi.addSubview(StartTransferBusStopName)
            ArrivalTransferObi.addSubview(ArrivalTransferBusTime)
            ArrivalTransferObi.addSubview(ArrivalTransferBusStopName)
            BusInfoTransferView.drawLine(start: CGPoint(x:leftPadding,y:0), end: CGPoint(x:leftPadding,y:busInfoHeight), color: UIColor(hex:"5A7FBB"), weight: 5, rounded: false)
            TransferBusDestination.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding,width: screenWidth-busInfoLeftPadding, height: labelHeight)
            TransferBusDestination.textAlignment = NSTextAlignment.left
            TransferBusDestination.textColor = .black
            TransferBusLineage.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight,width: screenWidth-busInfoLeftPadding, height: labelHeight)
            TransferBusLineage.textAlignment = NSTextAlignment.left
            TransferBusLineage.textColor = .black
            TransferBusPrice.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight*2,width: screenWidth-busInfoLeftPadding, height: labelHeight)
            TransferBusPrice.textAlignment = NSTextAlignment.left
            TransferBusPrice.textColor = .black
            TransferBusPredict.frame = CGRect(x: busInfoLeftPadding, y: topbottomPadding+labelHeight*3,width: screenWidth-busInfoLeftPadding, height: labelHeight)
            TransferBusPredict.textAlignment = NSTextAlignment.left
            TransferBusPredict.textColor = .black
            BusInfoTransferView.addSubview(TransferBusDestination)
            BusInfoTransferView.addSubview(TransferBusLineage)
            BusInfoTransferView.addSubview(TransferBusPrice)
            BusInfoTransferView.addSubview(TransferBusPredict)
            DepartureTransferPlatFormName.frame = CGRect(x:160, y:0, width:screenWidth-160, height:obiHeight)
            DepartureTransferPlatFormName.textAlignment = NSTextAlignment.left
            DepartureTransferPlatFormName.textColor = .white
            DepartureTransferObi.addSubview(DepartureTransferPlatFormName)
            ArivalTransferPlatformName.frame = CGRect(x: 160, y: 0,width: screenWidth-150, height: obiHeight)
            ArivalTransferPlatformName.textAlignment = NSTextAlignment.left
            ArivalTransferPlatformName.textColor = .white
            ArrivalTransferObi.addSubview(ArivalTransferPlatformName)
            // テキストの設定
            StartBusStopName.text = startStr // 出発バス停名ラベルのtextを設定
            ArrivalBusStopName.text = startTraStr // 到着バス停名ラベルのtextを設定
            StartBusTime.text = startTime    // 出発時刻ラベルのtextを設定
            ArrivalBusTime.text = startTraTime   // 到着時刻ラベルのtextを設定
            DeparturePlatFormName.text = nPlatformName //のりばラベルのtextを設定
            ArivalPlatformName.text = nTraPlatformName         // おりばラベルのtextを設定
            BusLineage.text = busLine        // 系統ラベルのtextを設定
            BusDestination.text = busDes + " 行き"     // 行き先ラベルのtextを設定
            BusPrice.text = "料金"      // 料金ラベルのtextを設定
            TransferTime.text = "乗換時間：" + String(norikaeTime(output_de_time:startTraTime,output_ar_time:arrTraTime)) + "分"
            StartTransferBusStopName.text = arrTraStr // 出発バス停名ラベルのtextを設定
            ArrivalTransferBusStopName.text = arrStr // 到着バス停名ラベルのtextを設定
            StartTransferBusTime.text = arrTraTime  // 出発時刻ラベルのtextを設定
            ArrivalTransferBusTime.text = arrTime     // 到着時刻ラベルのtextを設定
            DepartureTransferPlatFormName.text = oTraPlatformName // のりばラベルのtextを設定
            ArivalTransferPlatformName.text = oPlatformName  // おりばラベルのtextを設定
            TransferBusPredict.text = "つ前のバス停を出発" // 出発情報の予測のtextを設定
            TransferBusLineage.text = busTraLine // 系統ラベルのtextを設定
            TransferBusDestination.text = busTraDes + "行き"     // 行き先ラベルのtextを設定
            TransferBusPrice.text = "料金"      // 料金ラベルのtextを設定
        }
        // 乗り換えある場合ここまで
        // あるなし共通
        BusTotalPrice.text = "料金：" // 合計料金ラベルのtextを設定
        if reTimeHour == "0" {
            TotalReTime.text = "所要時間" + reTimeMin + "分" // 合計所要時間ラベルのtextを設定（分のみ）
        } else {
            TotalReTime.text = "所要時間" + reTimeHour + "時間" + reTimeMin + "分" // 合計所要時間ラベルのtextを設定（時間＆分）
        }
        BusTransferNum.text = norikaeBool     // バス乗り換え回数ラベルのtextを設定
        BusPredict.text = ""
        
        //クロージャ
        getData(ref: ref){ [weak self] in
            //getDataが行われた後の動作
            self!.index = 0
            while self!.index < self!.id.count{
                if(self!.tripId == self!.id[self!.index]){
                    if(self!.status[self!.index] == 0 || self!.status[self!.index] == 2){
                        self!.prediction = self!.stopSequence - self!.sequence[self!.index] - 1
                        if(self!.prediction > 0){
                            self!.BusPredict.text = String(self!.prediction) + "つ前のバス停を出発"
                            break
                        }else{
                            self!.BusPredict.text = "通過しました"
                            break
                        }
                    }
                    if(self!.status[self!.index] == 1){
                        self!.prediction = self!.stopSequence - self!.sequence[self!.index]
                        if(self!.prediction > 0){
                            self!.BusPredict.text = String(self!.prediction) + "つ前のバス停を出発"
                            break
                        }else{
                            self!.BusPredict.text = "通過しました"
                            break
                        }
                    }
                }
                self!.index += 1
            }
            if(self!.index == self!.id.count){
                self!.BusPredict.text = ""
            }
        }
        
    }
    @objc func ModoruViewTapped(_ sender: UITapGestureRecognizer) {
        self.dismiss(animated: false, completion: nil)
    }
    @objc func HomeViewTapped(_ sender: UITapGestureRecognizer) {
        // storyboardのインスタンス取得（別のstoryboardの場合）
        let storyboard: UIStoryboard = UIStoryboard(name: "Home", bundle: nil)
        // ②遷移先ViewControllerのインスタンス取得
        let nextView = storyboard.instantiateViewController(withIdentifier: "viewHome") as! HomeViewController
        nextView.modalPresentationStyle = .fullScreen
        //nextView.DATE = dateString
        DATE = DATE_FORMATTER.date(from: dateString)!
        // ③画面遷移
        self.present(nextView, animated: false, completion: nil)
    }
}
/*ラインを描くための拡張クラス*/
class BezierView: UIView {

    var start: CGPoint = .zero
    var end: CGPoint = .zero
    var weight: CGFloat = 2.0
    var color: UIColor = .gray
    var isRounded: Bool = true
    //var dashes: [CGFloat] = [.zero,.zero]

    override init(frame: CGRect) {
        super.init(frame: frame);
        self.backgroundColor = UIColor.clear;
    }
    required init?(coder aDecoder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func draw(_ rect: CGRect) {
        let line = UIBezierPath()
        line.move(to: start)
        line.addLine(to: end)
        line.close()
        color.setStroke()
        line.lineWidth = weight
        //line.setLineDash(dashes,count: dashes.count, phase: 0)
        line.lineCapStyle = isRounded ? .round : .square
        line.stroke()
        self.isUserInteractionEnabled = false
    }
}
/*ラインのためのUIViewの拡張*/
extension UIView {
    func drawLine(start: CGPoint, end: CGPoint, color: UIColor, weight: CGFloat, rounded: Bool) {
        let line: BezierView = BezierView(frame: CGRect(x: 0, y: 0, width: max(start.x , end.x)+weight, height: max(start.y, end.y)+weight))
        line.start = start
        line.end = end
        line.color = color
        line.weight = weight
        line.isRounded = rounded
        //line.setLineDash(dashes,count: dashes.count, phase: 0)
        self.addSubview(line)
    }
    func drawLine(points: [CGPoint], color: UIColor, weight: CGFloat, rounded: Bool) {
        guard points.count >= 2 else { fatalError("Line is not drawable because points are less than 2") }
        for i in 0..<points.count-1 {
            self.drawLine(start: points[i], end: points[i+1], color: color, weight: weight, rounded: rounded)
        }
    }
}
/*ドットラインを描くための拡張クラス*/
class DrawView: UIView {
    
    override init(frame: CGRect) {
        super.init(frame: frame);
        self.backgroundColor = UIColor.clear;
    }
    
    required init?(coder aDecoder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    
    override func draw(_ rect: CGRect) {
        // 線
        let line = UIBezierPath()
        // 最初の位置
        line.move(to: CGPoint(x: leftPadding, y: 0))
        // 次の位置
        line.addLine(to:CGPoint(x: leftPadding, y: obiHeight))
        // 終わる
        line.close()
        // 線の色
        UIColor(hex:"3F6AB2").setStroke()
        // 線の太さ
        line.lineWidth = 5.0
        // 点線の大きさ, 点線の隙間
        let dashes : [CGFloat] = [3, 6]
        // 第一引数 点線の大きさ(数値*2になる), 点線の間隔（間隔-大きさになる）
        // 第二引数 第一引数で指定した配列の要素数
        // 第三引数 開始位置
        line.setLineDash(dashes, count: dashes.count, phase: 0)
        // 線を塗りつぶす
        line.stroke()
        
    }
}
func norikaeTime(output_de_time:String,output_ar_time:String)->(Int){
    //時間を入れる空の配列を用意
    var deHour:Int = 0
    var arHour:Int = 0
    //分を入れる空の変数を用意
    var deMin:Int = 0
    var arMin:Int = 0
    //文字列から時間を分で取得
    deHour = Int(output_de_time.prefix(2).description)!*60//分に直す
    arHour = Int(output_ar_time.prefix(2).description)!*60//分に直す
    //文字列から分を取得
    deMin = Int(output_de_time.suffix(2).description)!//分なのでそのまま取得
    arMin = Int(output_ar_time.suffix(2).description)!//分なのでそのまま取得
    //出発時間と到着時間の分のの合計を入れる配列
    var de_sum:Int = 0
    var ar_sum:Int = 0
    //出発時間の時間と分の合計（分）を求める
    de_sum = deHour+deMin
    //到着時間の時間と分の合計（分）を求める
    ar_sum = arHour+arMin
    //分で引き算＝所要時間が分で求められる
    var hiki:Int = 0
    hiki = ar_sum - de_sum
    //値を返す
    return hiki
}
