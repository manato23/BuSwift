//
//  itiranViewController.swift
//  swift2022a
//
//  Created by 涌井春那 on 2022/11/08.
//

import UIKit
var timeSameBool = 0
class ItiranViewController: UIViewController {
    @IBOutlet weak var Modoru: UIImageView!
    var alertController: UIAlertController!
    //ボタンのインスタンス生成
    var buttons = [UIButton()]
    //ボタンに画像をつけるため
    let Image = UIImage()
    //　ラベルのインスタンス生成
    @IBOutlet weak var dateLabel: UILabel!
    @IBOutlet weak var depLabel: UILabel!
    @IBOutlet weak var arrLabel: UILabel!
    @IBOutlet weak var kekkaLabel: UILabel!
    let yazirusiLabel = UILabel()
    var nowLabel = 0
    // Homeから受け取る用の変数（スコープの関係上移動）
    var depString = ""
    var arrString = ""
    var dateString = ""
    var dateBool = 0
    // 出発時間と到着時間用の変数
    var output_de_time: [String]!
    var output_ar_time: [String]!
    //合計所要時間の変数
    var TotalReTimeHour: [String]! //合計所要時間（時間）
    var TotalReTimeMin: [String]!  //合計所要時間（分）
    //行き先の変数
    var busDep: [String]! //行き先
    //系統の変数
    var busLineage: [String]! //系統
    //乗り場の変数
    var nPlatform: [String]!
    //降り場の変数
    var oPlatform: [String]!
    //乗換ある時の出発時間と到着時間の変数
    var output_first_artime:[String]!
    var output_second_detime:[String]!
    //乗換ある時の系統の変数
    var keito_norikae:[String]!
    //乗換ある時の行き先の変数
    var ikisaki_norikae:[String]!
    //乗換ある時の降り場の変数
    var oriba_norikae:[String]!
    //乗換ある時の乗り場の変数
    var noriba_norikae:[String]!
    //乗換ある時の乗り場のバス停
    var oriba_busstop:[String]!
    //乗換ある時の降り場のバス停
    var noriba_busstop:[String]!

    override func viewDidAppear(_ animated: Bool){
            super.viewDidAppear(animated)
            let bustime=BusTime(departure:depString,arrival:arrString)
            let empty = bustime.empty
            if(empty==true){
                alert(title: "エラー",message: "便がありません。")
            }
            if(empty==false){
                let dateFormatter = DateFormatter()
                dateFormatter.locale = Locale(identifier: "ja_JP")//日本語にするため
                dateFormatter.dateFormat = "HH:mm"//これは表示する形を設定
                let nowdate = dateFormatter.string(from: Date())
                let lastdate = output_ar_time.last!
                timeSameChecker(date1: nowdate, date2: lastdate)
                if(timeSameBool == 1){
                    alert(title: "エラー",
                                  message: "運行が終了しています。")
                }
                else if(timeSameBool==2){
                    //何もしない
                }
                else{
                    alert(title: "エラー",
                                  message: "読み込めません")
                }
            }
        }

    //trip_idの変数
    var tripId: [String]!
    //stop_sequenceの変数
    var stopSequence: [Int]!
    //乗換回数
    var norikaeCount: [String]!


    override func viewDidLoad() {
        super.viewDidLoad()
        Modoru.isUserInteractionEnabled = true
        Modoru.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(ModoruViewTapped(_:))))
        // 遷移元からの値の受け取り
        depLabel.text = depString
        arrLabel.text = arrString
        dateLabel.text = dateString
        // Do any additional setup after loading the view.
        self.view.frame = CGRect(x: 0, y: 0, width: 375, height: 667)
        self.view.backgroundColor = UIColor(hex: "F0F0FC")
        //bustimeの情報格納
        let bustime=BusTime(departure:depString,arrival:arrString)
        // 出発時間と到着時間用の変数
        output_de_time = bustime.output_de_time
        output_ar_time = bustime.output_ar_time
        // 時間だけにする
        let unistr = hour(str:output_de_time) // ここの引数に、BusTimeの出発時間の値を渡す。
        // output_de_timeのカウント(button用)と、時刻だけのカウント(label用)
        let buttonCount = output_ar_time.count
        let labelCount = unistr.count
        // buttonのサイズの設定
        let buttonSize: Int = 50
        // labelのサイズの設定
        let labelSize: Int = 30
        //合計所要時間の変数
        TotalReTimeHour = totalTime(output_de_time: output_de_time, output_ar_time: output_ar_time).totalHour
        TotalReTimeMin = totalTime(output_de_time: output_de_time, output_ar_time: output_ar_time).totalMin
        // 行き先の変数
        busDep = bustime.ikisaki
        // 系統の変数
        busLineage = bustime.keito
        //乗り場の変数
        nPlatform = bustime.noriba
        //降り場の変数
        oPlatform = bustime.oriba
        //trip_idの変数
        tripId = bustime.RTinfo[0].trip_id
        //stop_sequenceの変数
        stopSequence = bustime.RTinfo[0].stop_sequence
        //乗換回数の変数
        norikaeCount = bustime.norikae
        //乗換ある時の出発時間と到着時間
        output_first_artime = bustime.output_first_artime
        output_second_detime = bustime.output_second_detime
        //乗換ある時の系統の変数
        keito_norikae = bustime.keito_norikae
        //乗換ある時の行き先の変数
        ikisaki_norikae = bustime.ikisaki_norikae
        //乗換ある時の乗り場の変数
        noriba_norikae = bustime.noriba_norikae
        //乗換ある時の降り場の変数
        oriba_norikae = bustime.oriba_norikae
        //乗換ある時の乗り場のバス停
        oriba_busstop = bustime.oriba_busstop
        //乗換ある時の降り場のバス停
        noriba_busstop = bustime.noriba_busstop
        //スクリーンの幅
        let screenWidth = Int(UIScreen.main.bounds.size.width)
        //スクリーンの高さ
        let screenHeight = Int(UIScreen.main.bounds.size.height)
        //UIScrollViewのインスタンス作成
        let scrollView = UIScrollView()
        //scrollViewの大きさを設定
        scrollView.frame = .init(x: 0, y: 306,width: screenWidth, height: screenHeight)
        //最後の場所を保存する（最初は検索結果の下のところからスタート）
        var last: Int = 0
        //スクロール領域の設定
        scrollView.contentSize = CGSize(width:screenWidth, height:buttonSize*(buttonCount+labelCount+1))
        scrollView.backgroundColor = UIColor(hex: "F0F0FC")
        //scrollViewをviewのSubViewとして追加
        self.view.addSubview(scrollView)
        // View表示と同時に、一時的にスクロールバー表示
        scrollView.flashScrollIndicators()
        
        let backGround = UIView()
        backGround.frame = CGRect(x: 0, y: 0,width: screenWidth, height: 306)
        backGround.backgroundColor = UIColor(hex: "F0F0FC")
        backGround.layer.shadowColor = UIColor.gray.cgColor //影の色を決める
        backGround.layer.shadowOpacity = 0.7 //影の色の透明度
        backGround.layer.shadowRadius = 4 //影のぼかし
        backGround.layer.shadowOffset = CGSize(width: 0, height: 4) //影の方向　width、heightを負の値にすると上の方に影が表示される
        self.view.addSubview(backGround)
        backGround.addSubview(Modoru)
        let itiranBackGround = UIView()
        itiranBackGround.frame = CGRect(x: 0, y: 150,width: screenWidth, height: 126)
        itiranBackGround.backgroundColor = .white
        backGround.addSubview(itiranBackGround)
        
        //戻るの場所設定
        Modoru.frame = CGRect(x:15, y:70,
                              width:70, height:50)
        // テキストの色
        dateLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        dateLabel.frame = CGRect(x:0, y:90, width:screenWidth, height:30)
        // 文字を中央にalignする
        dateLabel.textAlignment = NSTextAlignment.center
        // ラベルの文字サイズを設定
        dateLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        // viewにラベルを追加
        self.view.addSubview(dateLabel)
        // テキストの内容を設定
        kekkaLabel.text = "検索結果"
        // テキストの色
        kekkaLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        kekkaLabel.frame = CGRect(x:0, y:120, width:screenWidth, height:30)
        // 文字を中央にalignする
        kekkaLabel.textAlignment = NSTextAlignment.center
        // ラベルの文字サイズを設定
        kekkaLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 20)
        // viewにラベルを追加
        self.view.addSubview(kekkaLabel)
        // テキストの色
        depLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        depLabel.frame = CGRect(x:0, y:10, width:screenWidth, height:30)
        // 文字を中央にalignする
        depLabel.textAlignment = NSTextAlignment.center
        depLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        //  viewにラベルを追加
        itiranBackGround.addSubview(depLabel)
        //矢印のテキスト
        yazirusiLabel.text = "↓"
        // テキストの色
        yazirusiLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        yazirusiLabel.frame = CGRect(x:0, y:48, width:screenWidth, height:30)
        // 文字を中央にalignする
        yazirusiLabel.textAlignment = NSTextAlignment.center
        yazirusiLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        //  viewにラベルを追加
        itiranBackGround.addSubview(yazirusiLabel)
        // テキストの色
        arrLabel.textColor = UIColor.black
        // ラベルの位置とサイズを設定
        arrLabel.frame = CGRect(x:0, y:86, width:screenWidth, height:30)
        // 文字を中央にalignする
        arrLabel.textAlignment = NSTextAlignment.center
        arrLabel.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        // viewにラベルを追加
        itiranBackGround.addSubview(arrLabel)
        // 下の配列用の変数
        var i:Int = 0
        //出発時間と到着時間を保存するための配列
        var depAndArr:[String] = []
        //乗換回数
        let norikaeCount:[String] = bustime.norikae
        //ボタン用に入れていく
        for i in 0..<min(output_de_time.count, output_ar_time.count) {
            depAndArr.append(output_de_time[i] + "→" + output_ar_time[i])
        }
        //乗換回数のための添字
        var j:Int = 0
        //動的な一覧画面を作る
        for unistr in unistr{
            //　ラベルのインスタンス生成
            let label = CustomLabel()
            // ラベルの文字を設定
            label.text = String(unistr) + "時"
            // テキストの色
            label.textColor = UIColor.white
            // ラベルの位置とサイズを設定
            label.frame = CGRect(x:0, y:last, width:screenWidth, height:labelSize)
            last += labelSize
            // 文字を中央にalignする
            label.textAlignment = NSTextAlignment.left
            // ラベルのフォントサイズを設定
            label.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
            // ラベルの背景色の設定
            label.backgroundColor = UIColor(hex: "3F6AB2")
            // それぞれのラベルを判別
            label.tag = Int(unistr)! //時間を選ぶために役立つかも？
            if(label.text==dateHour()){
                nowLabel = Int(label.top)
            }
            if(dateBool==1){
                scrollView.contentOffset = CGPoint.init(x: 0, y: nowLabel)
            }else{
                scrollView.contentOffset = CGPoint.init(x: 0, y: 0)
            }
            // scrollViewにラベルを追加
            scrollView.addSubview(label)
            // 時間ごとに動的なボタンを生成する
            for str in depAndArr{
                //unistrの最初の2文字とstrの最初の2文字が同じだった時
                if(str.prefix(2).description==unistr.prefix(2).description){
                    i+=1
                    // ボタンのインスタンス生成
                    let button = UIButton()
                    let norikaeTime = UILabel()
                    //　ボタンのタイトルを設定
                    button.setTitle(String(str),for: .normal)
                    //　タイトルの色
                    button.setTitleColor(.black, for: .normal)
                    //　ボタンの背景色を定義する
                    button.backgroundColor = UIColor.white
                    // ボタンのテキストの位置を変更する
                    button.contentHorizontalAlignment = .left
                    //ボタンにイメージをつける
                    button.setImage(type(of: self.Image).init(systemName: "chevron.right"), for: .normal)
                    button.tintColor = UIColor(hex:"D9D9D9")
                    button.imageEdgeInsets = UIEdgeInsets(top: 0, left: CGFloat(screenWidth-30), bottom: 0, right: 0)
                    // タイトルに余白をつける
                    button.titleEdgeInsets = UIEdgeInsets(top: 0, left: 40,bottom: 0,right: 0)
                    //ボタンに枠線を定義する
                    button.layer.borderColor = UIColor(hex:"D9D9D9").cgColor
                    button.layer.borderWidth = 1
                    button.titleLabel?.font = UIFont(name: "ヒラギノ角ゴシック W3", size: 18)
                    // ラベルの文字を設定
                    norikaeTime.text = norikaeCount[j]
                    j+=1
                    // テキストの色
                    norikaeTime.textColor = UIColor.black
                    // 文字を中央にalignする
                    norikaeTime.textAlignment = NSTextAlignment.left
                    // ラベルのフォントサイズを設定
                    norikaeTime.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 18)
                    // ラベルの背景色の設定
                    norikaeTime.backgroundColor = UIColor.clear
                    //　ボタンの位置とサイズを設定
                    button.frame = CGRect(x:0, y:last, width:screenWidth, height:buttonSize)
                    norikaeTime.frame = CGRect(x:220, y:last, width:screenWidth-220, height:buttonSize)
                    last += buttonSize
                    //　タップされた時のアクション
                    button.addTarget(self, action: #selector(ItiranViewController.tap), for: .touchUpInside)
                    //　それぞれのボタンを判別　これを時間か何かにする？ 07:32を732とかにすると良いかも。したらわかりやすい？
                    button.tag = Int(i)
                    //　ボタンを表示
                    scrollView.addSubview(button)
                    scrollView.addSubview(norikaeTime)
                    self.buttons.append(button) // 追加
                }
            }
        }
    }
    func alert(title:String, message:String) {
        alertController = UIAlertController(title: title,
                                            message: message,
                                            preferredStyle: .alert)
        alertController.addAction(UIAlertAction(title: "OK",
                                                style: .default,
                                                handler: nil))
        present(alertController, animated: true)
    }
    @objc func ModoruViewTapped(_ sender: UITapGestureRecognizer) {
        //画面遷移
        /*let storyboard = UIStoryboard(name: "Home", bundle: nil)
        let nextVC = storyboard.instantiateViewController(withIdentifier: "viewHome") as! HomeViewController
        nextVC.DATE = dateString
        self.present(nextVC, animated: false, completion: nil)*/
        self.dismiss(animated: false, completion: nil)
    }
    //タップされた時のための動作
    @objc func tap(_ sender:UIButton) {
        //画面遷移
        let storyboard = UIStoryboard(name: "Detail", bundle: nil)
        let nextVC = storyboard.instantiateViewController(withIdentifier: "viewDetail") as! DetailViewController
        //値を入れる(追記)
        nextVC.startStr = depLabel.text!
        nextVC.arrStr = arrLabel.text!
        nextVC.startTime = output_de_time[sender.tag-1]//出発時間
        nextVC.arrTime = output_ar_time[sender.tag-1]//到着時間
        nextVC.startTraTime = output_first_artime[sender.tag-1]//1本目の到着時間
        nextVC.arrTraTime = output_second_detime[sender.tag-1]//2本目の到着時間
        nextVC.reTimeHour = TotalReTimeHour[sender.tag-1]
        nextVC.reTimeMin = TotalReTimeMin[sender.tag-1]
        nextVC.busDes = busDep[sender.tag-1]
        nextVC.busLine = busLineage[sender.tag-1]
        nextVC.nPlatformName = nPlatform[sender.tag-1]
        nextVC.oPlatformName = oPlatform[sender.tag-1]
        nextVC.dateString = dateLabel.text!
        nextVC.tripId = tripId[sender.tag-1]
        nextVC.stopSequence = stopSequence[sender.tag-1]
        nextVC.norikaeBool = norikaeCount[sender.tag-1]
        nextVC.startTraStr = oriba_busstop[sender.tag-1]
        nextVC.arrTraStr = noriba_busstop[sender.tag-1]
        nextVC.busTraDes = ikisaki_norikae[sender.tag-1]
        nextVC.busTraLine = keito_norikae[sender.tag-1]
        nextVC.oTraPlatformName = oriba_norikae[sender.tag-1]
        nextVC.nTraPlatformName = noriba_norikae[sender.tag-1]
        self.present(nextVC, animated: false, completion: nil)
        //buttons関連
        if !sender.isSelected {
            buttons.forEach({element in element.isSelected = false})
        }
        sender.isSelected = !sender.isSelected
        // isSelectedがFalseのボタンを選ぶ場合はすべてのボタンを解除してから選択したボタンのisSelectedをTrueにして、
        // Tureのボタンを選んだ場合は単に選択をisSelectedをFalseにする
    }
}

/*配列から重複を削除*/
extension Array where Element: Hashable {
    func unique() -> [Element] {
        return NSOrderedSet(array: self).array as! [Element]
    }
}
/*関数名：hour
 引数：String型の配列
 返り値：String型の配列
 　　内容：時間だけを取り出す
 */
func hour(str:[String])->[String]{
    //空の配列を用意
    var twoStr : [String] = []
    /*文字列の最初から2文字取得する*/
    for i in str{
        twoStr.append(i.prefix(2).description)
    }
    //重複を削除
    let unistr = twoStr.unique()
    return unistr
}
/*カラーコードで指定するための関数*/
extension UIColor {
    convenience init(hex: String, alpha: CGFloat = 1.0) {
        let v = Int("000000" + hex, radix: 16) ?? 0
        let r = CGFloat(v / Int(powf(256, 2)) % 256) / 255
        let g = CGFloat(v / Int(powf(256, 1)) % 256) / 255
        let b = CGFloat(v / Int(powf(256, 0)) % 256) / 255
        self.init(red: r, green: g, blue: b, alpha: min(max(alpha, 0), 1))
    }
}
/*ラベルに余白を入れるためのクラス*/
class CustomLabel: UILabel {
    
    @IBInspectable var topPadding: CGFloat = 10
    @IBInspectable var bottomPadding: CGFloat = 10
    @IBInspectable var leftPadding: CGFloat = 20//10から20に変更
    @IBInspectable var rightPadding: CGFloat = 10
    
    override func drawText(in rect: CGRect) {
        super.drawText(in: rect.inset(by: UIEdgeInsets.init(top: topPadding, left: leftPadding, bottom: bottomPadding, right: rightPadding)))
    }
    
    override var intrinsicContentSize: CGSize {
        var size = super.intrinsicContentSize
        size.height += (topPadding + bottomPadding)
        size.width += (leftPadding + rightPadding)
        return size
    }
}
/**UIViewの拡張**/
extension UIView {
    
    var top : CGFloat{
        get{
            return self.frame.origin.y
        }
        set{
            var frame = self.frame
            frame.origin.y = newValue
            self.frame = frame
        }
    }
    
    var bottom : CGFloat{
        get{
            return frame.origin.y + frame.size.height
        }
        set{
            var frame = self.frame
            frame.origin.y = newValue - self.frame.size.height
            self.frame = frame
        }
    }
    
    var right : CGFloat{
        get{
            return self.frame.origin.x + self.frame.size.width
        }
        set{
            var frame = self.frame
            frame.origin.x = newValue - self.frame.size.width
            self.frame = frame
        }
    }
    
    var left : CGFloat{
        get{
            return self.frame.origin.x
        }
        set{
            var frame = self.frame
            frame.origin.x = newValue
            self.frame = frame
        }
    }
}
func timeSameChecker(date1:String, date2:String){
    //最初の2文字
    let date1_first = date1.prefix(2).description
    //最後の2文字
    let date1_last = date1.suffix(2).description
    //最初の2文字
    let date2_first = date2.prefix(2).description
    //最後の2文字
    let date2_last = date2.suffix(2).description
    //全く同じ時刻
    if(Int(date1_first) == Int(date2_first)){
        if(Int(date1_last)! >= Int(date2_last)!){
            timeSameBool = 1
        }
        else if(Int(date1_last)! < Int(date2_last)!){
            timeSameBool = 2
        }
        else{
            timeSameBool = 3
        }
    }
    else if(Int(date1_first)! > Int(date2_first)!){
        timeSameBool = 1
    }
    else if(Int(date1_first)! < Int(date2_first)!){
        timeSameBool = 2
    }
    else{
        timeSameBool = 3
    }
}
