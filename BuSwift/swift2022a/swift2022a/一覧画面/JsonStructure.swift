//
//  JsonStructure.swift
//  JsonLook
//
//  Created by 涌井春那 on 2022/10/22.
//

import Foundation

struct Agency:Codable{
    let agency_id: String
    let agency_name: String
    let agency_url: String
    let agency_timezone: String
    let agency_lang: String
    let agency_phone: String
    let agency_fare_url: String
    let agency_email: String
}

struct AgencyJp:Codable{
    let agency_id: String
    let agency_official_name: String
    let agency_zip_number: String
    let agency_address: String
    let agency_president_pos: String
    let agency_president_name: String
}

struct CalendarDates: Codable{
    let service_id: String
    let date: String
    let exception_type: String
}

struct Calendar1: Codable{ //Calendarというswiftで使える関数？を使うため,区別として「Calendar1に変更」
    let service_id: String
    let monday: String
    let tuesday: String
    let wednesday: String
    let thursday: String
    let friday: String
    let saturday: String
    let sunday: String
    let start_date: String
    let end_date: String
}
struct FareAttributes:Codable{
    let fare_id: String
    let price: String
    let currency_type: String
    let payment_method: String
    let transfers: String
    let transfer_duration: String
}


struct FareRules:Codable{
    let fare_id: String
    let route_id: String
    let origin_id: String
    let destination_id: String
    let contains_id: String
}

struct FeedInfo:Codable{
    let feed_publisher_name: String
    let feed_publisher_url: String
    let feed_lang: String
    let feed_start_date: String
    let feed_end_date: String
    let feed_version: String
}
struct OfficeJp:Codable{
    let office_id: String
    let office_name: String
    let office_url: String
    let office_phone: String
}

struct Routes:Codable{
    let route_id: String
    let agency_id: String
    let route_short_name: String
    let route_long_name: String
    let route_desc: String
    let route_type: String
    let route_url: String
    let route_color: String
    let route_text_color: String
    let jp_parent_route_id: String
}

struct StopTimes:Codable{
    let trip_id: String
    let arrival_time: String
    let departure_time: String
    let stop_id: String
    let stop_sequence: String
    let stop_headsign: String
    let pickup_type: String
    let drop_off_type: String
    let shape_dist_traveled: String
    let timepoint: String
}
struct Stops:Codable{
    let stop_id: String
    let stop_code: String
    let stop_name: String
    let stop_desc: String
    let stop_lat: String
    let stop_lon: String
    let zone_id: String
    let stop_url: String
    let location_type: String
    let parent_station: String
    let stop_timezone: String
    let wheelchair_boarding: String
}
struct Translations:Codable{
    let trans_id: String
    let lang: String
    let translation: String
}
struct Trips:Codable{
    let route_id: String
    let service_id: String
    let trip_id: String
    let trip_headsign: String
    let trip_short_name: String
    let direction_id: String
    let block_id: String
    let shape_id: String
    let wheelchair_accessible: String
    let bikes_allowed: String
    let jp_trip_desc: String
    let jp_trip_desc_symbol: String
    let jp_office_id: String
}
extension Bundle {
   func decodeJSON<T: Codable>(_ file: String) -> T {
       guard let url = self.url(forResource: file, withExtension: nil) else {
           fatalError("Faild to locate \(file) in bundle.")
       }
       guard let data = try? Data(contentsOf: url) else {
           fatalError("Failed to load \(file) from bundle.")
       }
       let decoder = JSONDecoder()
       guard let loaded = try? decoder.decode(T.self, from: data) else {
           fatalError("Failed to decode \(file) from bundle.")
       }
       return loaded
   }
}

let agency_jp: [AgencyJp] = Bundle.main.decodeJSON("agency_jp.json")
let agency: [Agency] = Bundle.main.decodeJSON("agency.json")
let calendar_dates: [CalendarDates] = Bundle.main.decodeJSON("calendar_dates.json")
let calendar1: [Calendar1] = Bundle.main.decodeJSON("calendar.json")//Calendarというswiftで使える関数？を使うため,区別として「Calendar1に変更」
let fare_attributes: [FareAttributes] = Bundle.main.decodeJSON("fare_attributes.json")
let fare_rules: [FareRules] = Bundle.main.decodeJSON("fare_rules.json")
let feed_info: [FeedInfo] = Bundle.main.decodeJSON("feed_info.json")
let office_jp: [OfficeJp] = Bundle.main.decodeJSON("office_jp.json")
let routes: [Routes] = Bundle.main.decodeJSON("routes.json")
let stoptimes: [StopTimes] = Bundle.main.decodeJSON("stop_times.json")
let stops: [Stops] = Bundle.main.decodeJSON("stops.json")
let translations: [Translations] = Bundle.main.decodeJSON("translations.json")
let trips: [Trips] = Bundle.main.decodeJSON("trips.json")
