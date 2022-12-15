//
//  ArrivalViewController.swift
//  swift2022a
//
//  Created by 涌井春那 on 2022/11/05.
//

import UIKit

class ArrivalViewController: UIViewController {
    //TextField
    @IBOutlet weak var Batsu: UIImageView!
    @IBOutlet weak var textField: UITextField!
    //label
    let label = CustomLabel()
    //button
    let kouhoButton = UIButton()
    //保存用変数
    var textFieldString = ""
    var dateString = ""//日付が変わらないようにするため
    //スクリーンの幅
    let screenWidth = Int(UIScreen.main.bounds.size.width)
    //スクリーンの高さ
    let screenHeight = Int(UIScreen.main.bounds.size.height)
    override func viewDidLoad() {
        super.viewDidLoad()
        //UIButtonを無効化
        kouhoButton.isEnabled = false
        textField.addTarget(self, action: #selector(self.yobidashi(_:)), for: UIControl.Event.editingChanged)
        textField.font = UIFont(name:"ヒラギノ角ゴシック W3",size:18)
        textField.frame = CGRect(x:0, y:150, width:screenWidth, height:50)
        textField.textAlignment = .center
        // ボタンのテキストの位置を変更する
        kouhoButton.contentHorizontalAlignment = .left
        //表示したい文字列の変更
        label.text = "検索候補"
        // ラベルの背景色の設定
        label.backgroundColor = UIColor(hex: "3F6AB2")
        // テキストの色
        label.textColor = UIColor.white
        // ラベルの位置とサイズを設定
        label.frame = CGRect(x:0, y:250, width:screenWidth, height:35)
        // 文字を中央にalignする
        label.textAlignment = NSTextAlignment.left
        // 文字のフォント
        label.font = UIFont(name:"ヒラギノ角ゴシック W3", size: 16)
        self.view.addSubview(label)
        //ボタンの位置とサイズを設定
        kouhoButton.frame = CGRect(x:0, y:285 , width:screenWidth, height:50)
        //ボタンの余白を出す
        kouhoButton.titleEdgeInsets = UIEdgeInsets(top: 0, left: 20,bottom: 0,right: 0)
        //タップされた時のアクション
        kouhoButton.addTarget(self, action: #selector(self.tapButton(_:)), for: UIControl.Event.touchUpInside)
        self.view.addSubview(kouhoButton)
        // タイトルに余白をつける
        //kouhoButton.titleEdgeInsets = UIEdgeInsets(top: 0, left: 40,bottom: 0,right: 0)
        Batsu.isUserInteractionEnabled = true
        Batsu.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(BatsuViewTapped(_:))))
    }
    
    @objc func BatsuViewTapped(_ sender: UITapGestureRecognizer) {
        self.dismiss(animated: true, completion: nil)
    }
    //ボタンがタップされるたびに呼び出される。
    @objc func tapButton(_ sender: UIButton){
        // storyboardのインスタンス取得（別のstoryboardの場合）
        let storyboard: UIStoryboard = UIStoryboard(name: "Home", bundle: nil)
        // ②遷移先ViewControllerのインスタンス取得
        let nextView = storyboard.instantiateViewController(withIdentifier: "viewHome") as! HomeViewController
        nextView.modalPresentationStyle = .fullScreen
        //ARRIVAL_TEXT_FIELDに値を入れる
        ARRIVAL_TEXT_FIELD = kouhoButton.currentTitle!
        DATE = DATE_FORMATTER.date(from: dateString)!
        // ③画面遷移
        self.present(nextView, animated: false, completion: nil)
    }
    //textFieldに値が設定されるたびに呼び出される。
    @objc func yobidashi(_ textField: UITextField) {
        if(SearchGTFS(textField: textField.text!).isEmpty == false){
            //UIButtonを有効化
            kouhoButton.isEnabled = true
            kouhoButton.titleLabel?.font = UIFont(name:"ヒラギノ角ゴシック W3",size:18)
            kouhoButton.setTitleColor(UIColor.black, for: .normal)
            //ボタンの背景色
            kouhoButton.backgroundColor = .white
            kouhoButton.setTitle("\(SearchGTFS(textField: textField.text!))", for: .normal)
        }
        else{
            kouhoButton.isEnabled = false
            kouhoButton.titleLabel?.font = UIFont(name:"ヒラギノ角ゴシック W3",size:18)
            kouhoButton.setTitleColor(UIColor.black, for: .normal)
            //ボタンの背景色
            kouhoButton.backgroundColor = .white
            kouhoButton.setTitle("なし", for: .normal)
        }
    }
    
    @IBAction func didTapView(_ sender: UITapGestureRecognizer) {
        view.endEditing(true)
    }
    
    /*
     // MARK: - Navigation
     
     // In a storyboard-based application, you will often want to do a little preparation before navigation
     override func prepare(for segue: UIStoryboardSegue, sender: Any?) {
     // Get the new view controller using segue.destination.
     // Pass the selected object to the new view controller.
     }
     */
    
}
