//
//  ViewController.swift
//  swift2022a
//
//  Created by Mugi on 2022/10/07.
//

import UIKit
import CoreLocation

let YEAR_TO_DATE = Date()
let DATE_FORMATTER = DateFormatter()
var DATE = Date()
var DEPARTURE_TEXT_FIELD = "出発バス停は？" //保存用変数
var ARRIVAL_TEXT_FIELD = "到着バス停は？" //保存用変数
var DAY_OF_WEEK = getDayOfWeek(Date()) //保存用変数

class HomeViewController: UIViewController {
    var alertController: UIAlertController!
    let locationManager = CLLocationManager()
    //ばすうぃふとアイコンimage
    @IBOutlet weak var iconImage: UIImageView!
    //出発地button
    @IBOutlet weak var departureButton: UIButton!
    //”出発”テキスト
    @IBOutlet weak var departureText: UILabel!
    //”出発”地図ボタン
    @IBOutlet weak var mapdepartureButton: UIButton!
    //"出発"地図アイコン
    @IBOutlet weak var departuremapImage: UIImageView!
    //到着地button
    @IBOutlet weak var arrivalButton: UIButton!
    //”到着”テキスト
    @IBOutlet weak var arrivalText: UILabel!
    //”到着”地図ボタン
    @IBOutlet weak var maparrivalButton: UIButton!
    //"到着"地図アイコン
    @IBOutlet weak var arrivalmapImage: UIImageView!
    //”日付”テキスト
    @IBOutlet weak var dateText: UILabel!
    //日付を設定するための変数
    @IBOutlet weak var dateTextField: UITextField!
    //UIDatePickerを定義するための変数
    var datePicker: UIDatePicker = UIDatePicker()
    //検索button
    @IBOutlet weak var kensakuButton: UIButton!
    
    override func viewDidLayoutSubviews(){
        super.viewDidLayoutSubviews()
        self.requestCurrentLocation()

        //画面の中心を表す
        let ViewWidth = Float(UIScreen.main.bounds.size.width)
        //アイコンを中心に表示
        let IconWidthGap = (ViewWidth - Float(iconImage.frame.width)) / 2
        iconImage.frame = CGRect.init(x: CGFloat(IconWidthGap),
                                      y: iconImage.frame.minY,
                                      width: iconImage.frame.width,
                                      height: iconImage.frame.height)
        //検索ボタンの中心に表示
        let KensakuWidthGap = (ViewWidth - Float(kensakuButton.frame.width)) / 2
        kensakuButton.frame = CGRect.init(x: CGFloat(KensakuWidthGap),
                                          y: kensakuButton.frame.minY,
                                          width: kensakuButton.frame.width,
                                          height: kensakuButton.frame.height)
        //”出発”マップアイコンをマップ用ボタンの中心に位置する
        let MapDeButtonWidth = Int(mapdepartureButton.bounds.width) //地図選択ボタンの幅
        let DeTextWidth = Int(departureButton.frame.size.width) //テキスト出発ボタンの幅
        let MapDeButtonY = Int(mapdepartureButton.frame.minY) //地図選択ボタンのY座標
        let MapDeButtonHeight = Int(mapdepartureButton.frame.size.height) //地図選択ボタンの高さ
        departuremapImage.center = CGPoint(x: MapDeButtonWidth/2 + DeTextWidth + 14, y: MapDeButtonY + MapDeButtonHeight/2)
        mapdepartureButton.drawLine(start: CGPoint(x:0, y: 0), end: CGPoint(x: 0, y:50), color: UIColor(hex:"c6c6c6"), weight: 2, rounded: false)
        //”到着”マップアイコンをマップ用ボタンの中心に位置する
        let MapArButtonWidth = Int(maparrivalButton.bounds.width) //地図選択ボタンの幅
        let ArTextWidth = Int(arrivalButton.frame.size.width) //テキスト出発ボタンの幅
        let MapArButtonY = Int(maparrivalButton.frame.minY) //地図選択ボタンのY座標
        let MapArButtonHeight = Int(maparrivalButton.frame.size.height) //地図選択ボタンの高さ
        arrivalmapImage.center = CGPoint(x: MapArButtonWidth/2 + ArTextWidth + 14, y: MapArButtonY + MapArButtonHeight/2)
        maparrivalButton.drawLine(start: CGPoint(x:0, y: 0), end: CGPoint(x: 0, y:50), color: UIColor(hex:"c6c6c6"), weight: 2, rounded: false)
        //日付のテキストフィールドの枠線を消す
        dateTextField.borderStyle = .none
        dateTextField.backgroundColor = .white
        dateTextField.textAlignment = NSTextAlignment.center
    }
    //出発地・到着地にバス停名入れば黒文字に表示
    override func viewDidAppear(_ animated: Bool){
        super.viewDidAppear(animated)
        if(departureButton.currentTitle != "出発バス停は？"){
            departureButton.setTitleColor(UIColor.black, for: .normal)
        }
        if(arrivalButton.currentTitle != "到着バス停は？"){
            arrivalButton.setTitleColor(UIColor.black, for: .normal)
        }
    }
    
    override func viewDidLoad() {
        super.viewDidLoad()
        departureButton.setTitle(DEPARTURE_TEXT_FIELD, for: .normal)
        arrivalButton.setTitle(ARRIVAL_TEXT_FIELD, for: .normal)
        if(departureButton.currentTitle=="出発バス停は？" || arrivalButton.currentTitle=="到着バス停は？"){
            //以下は出発地バス停と到着地バス停を両方入力されていない場合のエラー確認用です。
            kensakuButton.addTarget(self, action: #selector(self.tapDameKensakuButton(_:)), for: UIControl.Event.touchUpInside)
        }
        else if(departureButton.currentTitle==arrivalButton.currentTitle){
            kensakuButton.addTarget(self, action: #selector(self.tapOnaziDameKensakuButton(_:)), for: UIControl.Event.touchUpInside)
        }
        else{
            kensakuButton.addTarget(self, action: #selector(self.tapKensakuButton(_:)), for: UIControl.Event.touchUpInside)
        }
        // DateFormatter を使用して書式とロケールを指定する
        DATE_FORMATTER.locale = Locale(identifier: "ja_JP")//日本語にするため
        DATE_FORMATTER.dateFormat = "y年M月d日(EE)"
        //DATE = DATE_FORMATTER.string(from: YEAR_TO_DATE)//最初に今日の日付を入れておく。
        //dateTextField.text = DATE//曜日も表示
        // ピッカー設定
        datePicker.datePickerMode = UIDatePicker.Mode.date
        datePicker.timeZone = NSTimeZone.local
        datePicker.locale = Locale(identifier: "ja_JP")//日本語にするため
        //選択の範囲を制限
        datePicker.minimumDate = Calendar.current.date(byAdding: .year, value: 0, to: Date())!//当日
        datePicker.preferredDatePickerStyle = .wheels//ドラムロール //ドラムロールじゃないとだめ
        dateTextField.inputView = datePicker
        datePicker.date = DATE
        dateTextField.text = DATE_FORMATTER.string(from: DATE)
        // 決定バーの生成
        let toolbar = UIToolbar(frame: CGRect(x: 0, y: 0, width: view.frame.size.width, height: 35))//
        let spacelItem = UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: self, action: nil)
        let doneItem = UIBarButtonItem(barButtonSystemItem: .done, target: self, action: #selector(done))
        toolbar.setItems([spacelItem, doneItem], animated: true)
        // インプットビュー設定(紐づいているUITextfieldへ代入)
        dateTextField.inputView = datePicker
        dateTextField.inputAccessoryView = toolbar
        dateTextField.font = UIFont(name: "ヒラギノ角ゴシック W3", size: 18)
        dateTextField.adjustsFontSizeToFitWidth = true
    }
    // ①セグエ実行前処理
    override func prepare(for segue: UIStoryboardSegue, sender: Any?) {
        // ②Segueの識別子確認
        if segue.identifier == "toDep" {
            // ③遷移先ViewCntrollerの取得
            let nextView = segue.destination as! DepartureViewController
            // ④値の設定
            nextView.dateString = dateTextField.text!
        }
        // ②Segueの識別子確認
        if segue.identifier == "toArr" {
            // ③遷移先ViewCntrollerの取得
            let nextView = segue.destination as! ArrivalViewController
            // ④値の設定
            nextView.dateString = dateTextField.text!
        }
    }
    //以下は出発地バス停と到着地バス停を両方入力されていない場合のエラー確認用です。
    @objc func tapDameKensakuButton(_ sender: UIButton){
        alert(title: "エラー",
              message: "出発バス停と到着バス停を両方入力してください。")
    }
    @objc func tapOnaziDameKensakuButton(_ sender: UIButton){
        alert(title: "エラー",
              message: "出発バス停と到着バス停は違う名前にしてください")
    }
    //検索ボタンがタップされるたびに呼び出される。
    @objc func tapKensakuButton(_ sender: UIButton){
        // インジケータビューの背景
        let indicatorBackgroundView = UIView(frame: UIScreen.main.bounds)
        indicatorBackgroundView.backgroundColor = UIColor(red: 0, green: 0, blue: 0, alpha: 0.5)
        //インジケータの設定
        let indicator = UIActivityIndicatorView(frame: CGRect(x: 0, y: 0, width: 100, height: 100))
        indicator.style = UIActivityIndicatorView.Style.large
        indicator.center = self.view.center
        indicator.color = UIColor.white
        // アニメーション停止と同時に隠す設定
        indicator.hidesWhenStopped = true
        // 作成したviewを表示
        indicatorBackgroundView.addSubview(indicator)
        self.view.addSubview(indicatorBackgroundView)
        indicator.startAnimating()
        //ラベルに文字を表示
        let label = UILabel(frame: CGRect(x: 0, y: 0, width: 300, height: 30))
        label.center = CGPoint(x: indicator.frame.origin.x + indicator.frame.size.width / 2, y: indicator.frame.origin.y + 90)
        label.textColor = UIColor.white
        label.textAlignment = .center
        label.text = "ただいま検索中です..."
        //ローディングビューにラベルを表示
        indicatorBackgroundView.addSubview(label)
        //storyboardのインスタンス取得（別のstoryboardの場合）
        let storyboard: UIStoryboard = UIStoryboard(name: "Itiran", bundle: nil)
        // ②遷移先ViewControllerのインスタンス取得
        let nextView = storyboard.instantiateViewController(withIdentifier: "viewItiran") as! ItiranViewController
        nextView.modalPresentationStyle = .fullScreen
        //値を入れる
        if(dateOnazi(datePicker.date)==true){
            nextView.dateBool = 1
        }
        //getDayOfWeekに値を入れる
        DAY_OF_WEEK = getDayOfWeek(datePicker.date)
        nextView.depString = departureButton.currentTitle!
        nextView.arrString = arrivalButton.currentTitle!
        nextView.dateString = dateTextField.text!
        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(2), execute: {
            // viewにローディング画面が出ていれば閉じる
            indicatorBackgroundView.removeFromSuperview()
            // ③画面遷移
            self.present(nextView, animated: false, completion: nil)
        })
    }
    
    // UIDatePickerのDoneを押したらTextFieldもそれに変わる。
    @objc func done() {
        dateTextField.endEditing(true)
        //(from: datePicker.date))を指定してあげることで
        //datePickerで指定した日付が表示される
        dateTextField.text = DATE_FORMATTER.string(from: datePicker.date)
    }
    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
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
    //出発地の地図アイコンタップ
    @IBAction func tapMapButtonDep(_ sender: Any) {
        let mapBoardA: UIStoryboard = UIStoryboard(name: "Map", bundle: nil)
        let nextMapViewA = mapBoardA.instantiateViewController(withIdentifier: "viewmap") as! MapViewController
        if DEPARTURE_TEXT_FIELD != "出発バス停は？" {
            nextMapViewA.inDeparture = DEPARTURE_TEXT_FIELD
        }
        if ARRIVAL_TEXT_FIELD != "到着バス停は？" {
            nextMapViewA.inArrival = ARRIVAL_TEXT_FIELD
        }
        nextMapViewA.modalPresentationStyle = .fullScreen
        nextMapViewA.checkNum = 1
        nextMapViewA.dateString = dateTextField.text!//日付が変わらないようにするため
        //地図画面に遷移
        self.present(nextMapViewA, animated: false, completion: nil)
    }
    //目的地の地図アイコンタップ
    @IBAction func tapMapButtonArr(_ sender: Any) {
        let mapBoardB: UIStoryboard = UIStoryboard(name: "Map", bundle: nil)
        let nextMapViewB = mapBoardB.instantiateViewController(withIdentifier: "viewmap") as! MapViewController
        if DEPARTURE_TEXT_FIELD != "出発バス停は？" {
            nextMapViewB.inDeparture = DEPARTURE_TEXT_FIELD
        }
        if ARRIVAL_TEXT_FIELD != "到着バス停は？" {
            nextMapViewB.inArrival = ARRIVAL_TEXT_FIELD
        }
        nextMapViewB.modalPresentationStyle = .fullScreen
        nextMapViewB.checkNum = 2
        nextMapViewB.dateString = dateTextField.text!//日付が変わらないようにするため
        //地図画面に遷移
        self.present(nextMapViewB, animated: false, completion: nil)
    }
    func requestCurrentLocation(){
        //位置情報サービスの確認
        DispatchQueue.global().async {
            if CLLocationManager.locationServicesEnabled(){
                let status = self.locationManager.authorizationStatus
                if status == CLAuthorizationStatus.notDetermined{
                    self.locationManager.requestWhenInUseAuthorization()
                }
                self.locationManager.startUpdatingLocation()
                DispatchQueue.global().asyncAfter(deadline: .now()+2) {
                    self.locationManager.stopUpdatingLocation()
                }
            }
        }
    }
}

/*関数名：dateOnazi
 引数：Date型
 返り値：Bool型（trueかfalse）
 　　内容：日付が現在の日付と同じ判断する
 */
func dateOnazi(_ date : Date)->Bool{
    if DATE_FORMATTER.string(from: date) == DATE_FORMATTER.string(from: YEAR_TO_DATE) {
        return true
    } else {
        return false
    }
}
/*関数名：dateHour
 引数：なし
 返り値：String型
 　　内容：今日の今の時間(何時か？)だけ出す
 */
func dateHour() -> String {
    let dateFormatter = DateFormatter()
    dateFormatter.locale = Locale(identifier: "ja_JP")//日本語にするため
    dateFormatter.dateFormat = "HH時"
    return dateFormatter.string(from: Date())//時間を出す
}
