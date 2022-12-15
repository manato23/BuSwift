//
//  didSelectViewController.swift
//  swift2022a
//
//  Created by 児玉拓海 on 2022/11/24.
//

import UIKit
import MapKit

class didSelectViewController: UIViewController {
    
    
    @IBOutlet weak var SelectPop: UIView!
    
    @IBOutlet weak var BusNameLabel: UILabel!
    
    //スクリーンの幅
    let screenWidth = Int(UIScreen.main.bounds.size.width)
    //スクリーンの高さ
    let screenHeight = Int(UIScreen.main.bounds.size.height)
    
    var busname = ""
    var ADcheckNum = 0
    var backNum = 1
    
    override func viewDidLoad() {
        super.viewDidLoad()
        BusNameLabel.text = busname
        
    }
    
    @IBAction func tapDecisiobButton(_ sender: Any) {
        // storyboardのインスタンス取得（別のstoryboardの場合）
        let storyboard: UIStoryboard = UIStoryboard(name: "Home", bundle: nil)
        // ②遷移先ViewControllerのインスタンス取得
        let nextView = storyboard.instantiateViewController(withIdentifier: "viewHome") as! HomeViewController
        nextView.modalPresentationStyle = .fullScreen
        switch ADcheckNum {
        case 0:
            print("失敗!!!")
        case 1:
            DEPARTURE_TEXT_FIELD = busname
            // ③画面遷移
            self.present(nextView, animated: false, completion: nil)
        case 2:
            
            ARRIVAL_TEXT_FIELD = busname
            // ③画面遷移
            self.present(nextView, animated: false, completion: nil)
        default:
            print("失敗!!!")
        }
    }
    
    @IBAction func tapCloseButton(_ sender: Any) {
        let backMap = presentingViewController as? MapViewController
        backMap?.backNum = self.backNum
        self.dismiss(animated: true, completion: nil)
    }
}
extension didSelectViewController {
    override func dismiss(animated flag: Bool, completion: (() -> Void)? = nil) {
        super.dismiss(animated: flag, completion: completion)
        guard let presentationController = presentationController else {
            return
        }
        presentationController.delegate?.presentationControllerDidDismiss?(presentationController)
    }
}
