//
//  MapViewController.swift
//  swift2022a
//
//  Created by 児玉拓海 on 2022/11/24.
//

import UIKit
import MapKit
import CoreLocation

class MapViewController: UIViewController, MKMapViewDelegate, UITextFieldDelegate, CLLocationManagerDelegate, UISearchBarDelegate {
    
    let locationManager = CLLocationManager()
    
    //地図のインスタンス
    @IBOutlet weak var mapView: MKMapView!
    //地図上のテキストフィールドのインスタンス
    @IBOutlet weak var searchField: UISearchBar!
    
    @IBOutlet weak var modoru: UIImageView!
    //バス停名と座標を格納する変数
    var BusStopPosition: [(id:String, BusStopNames:String, lat:String, lon:String)]=[]
    var checkNum = 0
    var backNum = 0
    var inDeparture = ""
    var inArrival = ""
    var clusteringIdentifier: String = ""
    //スクリーンの幅
    let screenWidth = Int(UIScreen.main.bounds.size.width)
    //スクリーンの高さ
    let screenHeight = Int(UIScreen.main.bounds.size.height)
    var dateString = ""//日付が変わらないようにするため
    
    override func viewDidLoad() {
        super.viewDidLoad()
        self.requestCurrentLocation()
        mapView.setUserTrackingMode(MKUserTrackingMode.follow, animated: false)
        
        
        modoru.isUserInteractionEnabled = true
        modoru.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(ModoruViewTapped(_:))))
        searchField.text = ""
        searchField.delegate = self
        mapView.delegate = self
        if checkNum == 1 {
            if self.locationManager.location == nil ||  self.locationManager.location?.coordinate.longitude == nil {
                let center = CLLocationCoordinate2DMake(41.786520, 140.744559) //函館中央辺り
                let span = MKCoordinateSpan(latitudeDelta: 0.005, longitudeDelta: 0.005) //表示範囲
                let region = MKCoordinateRegion(center: center, span: span)
                mapView.setRegion(region, animated: false)
            } else {
                //出発地入力の地図の開始地点の設定
                let center = CLLocationCoordinate2DMake((self.locationManager.location?.coordinate.latitude)!, (self.locationManager.location?.coordinate.longitude)!) //函館中央辺り
                let span = MKCoordinateSpan(latitudeDelta: 0.007, longitudeDelta: 0.007) //表示範囲
                let region = MKCoordinateRegion(center: center, span: span)
                self.mapView.setRegion(region, animated: false)
            }
        }
        if checkNum == 2 {
            //目的地入力の地図の開始地点の設定
            let center = CLLocationCoordinate2DMake(41.786520, 140.744559) //函館中央辺り
            let span = MKCoordinateSpan(latitudeDelta: 0.045, longitudeDelta: 0.045) //表示範囲
            let region = MKCoordinateRegion(center: center, span: span)
            mapView.setRegion(region, animated: false)
        }
        
        //バス停名と緯度経度を格納
        for stop in stops {
            BusStopPosition += [(stop.stop_id, stop.stop_name, stop.stop_lat, stop.stop_lon)]
        }
        //マップにバス停をプロットする
        for Inf in BusStopPosition {
            PlotBusStop(id: Inf.id, busname: Inf.BusStopNames, ido: Inf.lat, keido: Inf.lon)
        }
    }
    //ピンをプロットする関数
    func PlotBusStop(id: String, busname:String, ido:String, keido:String){
        let lat = Double(ido)
        let lon = Double(keido)
        let busName = busname
        let pin = MKPointAnnotation()
        if id.contains("_") {
            let coordnate = CLLocationCoordinate2DMake(lat!, lon!)
            //ピンの生成
            pin.title = busName
            pin.coordinate = coordnate
        }
        mapView.addAnnotation(pin)
        mapView.delegate = self
    }
    
    //ピンの設定
    func mapView(_ mapView: MKMapView, viewFor annotation: MKAnnotation) -> MKAnnotationView? {
        let annotationView = MKMarkerAnnotationView(annotation: annotation, reuseIdentifier: nil)
        //        annotationView.glyphText = ""
        if annotation.title == inArrival {
            annotationView.markerTintColor = UIColor(hex: "dc143c")
            annotationView.displayPriority = .required
            annotationView.clusteringIdentifier = "A"
        } else if annotation.title == inDeparture {
            annotationView.markerTintColor = UIColor(hex: "dc143c")
            annotationView.displayPriority = .required
            annotationView.clusteringIdentifier = "B"
        } else if annotation.title == "My Location"{
            annotationView.displayPriority = .required
            return nil
        } else {
            annotationView.markerTintColor = UIColor(hex: "3F6AB2")
            annotationView.displayPriority = .required
            annotationView.clusteringIdentifier = annotation.title!!
        }
        return annotationView
    }
    
    //ピンがタップされた時の処理
    func mapView(_ mapView:MKMapView, didSelect view:MKAnnotationView) {
        if let annotation = view.annotation{
            //画面遷移
            let storyboard = UIStoryboard(name: "didSelect", bundle: nil)
            let nextVC = storyboard.instantiateViewController(withIdentifier: "viewdidSelect") as! didSelectViewController
            nextVC.busname = annotation.title!!
            nextVC.ADcheckNum = checkNum
            DATE = DATE_FORMATTER.date(from: dateString)!//日付が変わらないようにするため
            nextVC.presentationController?.delegate = self
            self.present(nextVC, animated: true, completion: nil)
        } else {
            print("no")
        }
    }
    //キーボードの検索ボタンが押された時の処理
    func searchBarSearchButtonClicked(_ searchBar: UISearchBar) {
        let searchCenter = CLLocationCoordinate2DMake(41.786520, 140.744559) //函館中央辺り
        let searchSpan = MKCoordinateSpan(latitudeDelta: 0.1, longitudeDelta: 0.1) //表示範囲
        let searchRegion = MKCoordinateRegion(center: searchCenter, span: searchSpan)
        //キーボード閉じる
        searchField.resignFirstResponder()
        //検索条件を作成
        let request = MKLocalSearch.Request()
        request.naturalLanguageQuery = searchField.text
        //検索範囲を指定
        request.region = searchRegion
        //ローカル検索の実行
        let localSearch:MKLocalSearch = MKLocalSearch(request: request)
        localSearch.start(completionHandler: {(result, error) in
            if result == nil {
                return;
            }
            for placemark in (result?.mapItems)! {
                if(error == nil) {
                    let newCenter = CLLocationCoordinate2D(latitude: placemark.placemark.coordinate.latitude, longitude: placemark.placemark.coordinate.longitude)
                    let newSpan = MKCoordinateSpan(latitudeDelta: 0.0045, longitudeDelta: 0.0045)
                    let newRegion = MKCoordinateRegion(center: newCenter, span: newSpan)
                    self.mapView.setRegion(newRegion, animated: true)
                } else {
                    print("エラー")
                }
            }
        })
    }
    @IBAction func didTapView(_ sender: UITapGestureRecognizer) {
        view.endEditing(true)
    }
    @objc func ModoruViewTapped(_ sender: UITapGestureRecognizer) {
        self.dismiss(animated: false, completion: nil)
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

extension MapViewController: UIAdaptivePresentationControllerDelegate {
    func presentationControllerDidDismiss(_ presentationController: UIPresentationController) {
        for annotation in self.mapView.selectedAnnotations {
            self.mapView.deselectAnnotation(annotation, animated: true)
        }
    }
}
