/**
 * Copyright IBM Corporation 2016, 2017
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import Foundation
import KituraNet

// MARK: CouchDBClient

#if os(OSX)
    /// Represents a CouchDB configuration value.
    public typealias CouchDBValue = AnyObject
#else
    public typealias CouchDBValue = Any
#endif

/// Callback for _session requests, containing the session cookie, the JSON response,
/// and NSError if one occurred.
public typealias SessionCallback = (String?, JSON?, NSError?) -> ()

/// Represents a CouchDB connection.
public class CouchDBClient {

    /// Connection properties for the `CouchDBClient`.
    public let connProperties: ConnectionProperties

    /// Initialize a `CouchDBClient`.
    ///
    /// - parameter connectionProperties: The connection properties for the CouchDB connection.
    public init(connectionProperties: ConnectionProperties) {
        self.connProperties = connectionProperties
    }

    /// Returns a `Database` instance by name.
    ///
    /// - parameter dbName: String name of the desired `Database`.
    public func database(_ dbName: String) -> Database {
        return Database(connProperties: self.connProperties, dbName: dbName)
    }

    /// Returns a `UsersDatabase` instance.
    public func usersDatabase() -> UsersDatabase {
        return UsersDatabase(connProperties: self.connProperties, dbName: "_users")
    }

    /// Create a new `Database`.
    ///
    /// - parameters:
    ///     - dbName: String name of the database
    ///     - callback: Callback containing the newly created `Database`, or an NSError on failure.
    public func createDB(_ dbName: String, callback: @escaping (Database?, NSError?) -> ()) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties, method: "PUT",
                                                         path: "/\(HTTP.escape(url: dbName))", hasBody: false)
        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var db: Database?
            if let response = response {
                if response.statusCode == .created {
                    db = Database(connProperties: self.connProperties, dbName: dbName)
                } else {
                    if let descOpt = try? response.readString(), let desc = descOpt {
                        guard let errorData = desc.data(using: .utf8) else { return }
                        if let errorDesc = try? JSONSerialization.jsonObject(with: errorData, options: .allowFragments) as? JSON {
                            error = CouchDBUtils.createError(response.statusCode,
                                                         errorDesc: errorDesc, id: nil, rev: nil)
                        }
                    } else {
                        error = CouchDBUtils.createError(response.statusCode, id: nil, rev: nil)
                    }
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(db, error)
        }
        req.end()
    }

    /// Returns a Bool indicating whether a `Database` with the given name exists.
    ///
    /// - parameters:
    ///     - dbName: String name of the `Database` to look up.
    ///     - callback: Callback containing the result of the lookup or an NSError if one occurred.
    public func dbExists(_ dbName: String, callback: @escaping (Bool, NSError?) -> ()) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties, method: "GET",
                                                         path: "/\(HTTP.escape(url: dbName))", hasBody: false)
        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var exists = false
            if let response = response {
                if response.statusCode == HTTPStatusCode.OK {
                    exists = true
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(exists, error)
        }
        req.end()
    }

    /// Delete a `Database` given a local instance of it.
    ///
    /// - parameters:
    ///     - database: An instance of the `Database` to delete.
    ///     - callback: Callback containing an NSError if one occurred.
    public func deleteDB(_ database: Database, callback: @escaping (NSError?) -> ()) {
        deleteDB(database.name, callback: callback)
    }

    /// Delete a `Database` given its name.
    ///
    /// - parameters:
    ///     - dbName: String name of the `Database` to delete.
    ///     - callback: Callback containing an NSError if one occurred.
    public func deleteDB(_ dbName: String, callback: @escaping (NSError?) -> ()) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties, method: "DELETE",
                                                         path: "/\(HTTP.escape(url: dbName))", hasBody: false)
        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            if let response = response {
                if response.statusCode != HTTPStatusCode.OK {
                    if let descOpt = try? response.readString(), let desc = descOpt {
                        guard let errorData = desc.data(using: .utf8) else { return }
                        if let errorDesc = try? JSONSerialization.jsonObject(with: errorData, options: .allowFragments) as? JSON {
                            error = CouchDBUtils.createError(response.statusCode,
                                                         errorDesc: errorDesc, id: nil, rev: nil)
                        }
                    } else {
                        error = CouchDBUtils.createError(response.statusCode, id: nil, rev: nil)
                    }
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(error)
        }
        req.end()
    }

    /// Returns some UUIDs created by CouchDB.
    ///
    /// - parameters:
    ///     - count: The number of UUIDs to get.
    ///     - callback: Callback containing an array of UUIDs or an NSError if one occured.
    public func getUUIDs(count : UInt, callback : @escaping ([String]?, NSError?) -> Void) {

        let url = "/_uuids?count=\(count)"

        let requestOptions = CouchDBUtils.prepareRequest(connProperties, method: "GET",
                                                         path: url, hasBody: false)
        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var uuids: [String]?
            if let response = response {
                if response.statusCode == HTTPStatusCode.OK {

                    var data = Data()
                    do {
                        try response.readAllData(into: &data)

                        let responseJSON = try JSONDecoder().decode(JSON.self, from: data)

                        let uuidsJSON = responseJSON.uuids

                        #if swift(>=4.1)
                        guard let uuidJSON = uuidsJSON else { return }
                        uuids = uuidJSON.compactMap({ (uuidJSON) -> String? in
                            return uuidJSON
                        })
                        #else
                        uuids = uuidsJSON.flatMap({ (uuidJSON) -> String? in
                            return uuidJSON
                        })
                        #endif
                    } catch let caughtError {
                        #if os(Linux)
                            error = NSError(domain: caughtError.localizedDescription, code: -1)
                        #else
                            error = caughtError as NSError
                        #endif
                    }
                } else {
                    error = CouchDBUtils.createError(response.statusCode, id: nil, rev: nil)
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(uuids, error)
        }
        req.end()
    }

    /// Returns a UUID created by CouchDB.
    ///
    /// - parameter callback: Callback containing the UUID or an NSError if one occurred.
    public func getUUID(callback : @escaping (String?, NSError?) -> Void) {
        getUUIDs(count: 1) { (uuids, error) in
            var uuid : String?
            if let uuids = uuids,
                uuids.count > 0 {
                uuid = uuids.first
            }
            callback(uuid, error)
        }
    }

    /// Set a CouchDB configuration parameter to a new value.
    ///
    /// - parameters:
    ///     - keyPath: The configuration parameter String to update.
    ///     - value: The `CouchDBValue` to set the configuration parameter to.
    ///     - callback: Callback containing an NSError if one occurred.
    public func setConfig(keyPath: String, value: CouchDBValue, callback: @escaping (NSError?) -> ()) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties,
                                                         method: "PUT",
                                                         path: "/_config/\(keyPath)",
                                                         hasBody: true,
                                                         contentType: "application/json")
        let req = HTTP.request(requestOptions) { response in
            var configError: NSError?
            if let response = response {
                if response.statusCode != .OK {
                    configError = CouchDBUtils.createError(response.statusCode, id: nil, rev: nil)
                }
            }
            callback(configError)
        }
        let valueString = "\"\(value)\""
        if let valueData = try? JSONSerialization.data(withJSONObject: valueString, options: .prettyPrinted) {
            if let decoded = try? JSONSerialization.jsonObject(with: valueData, options: []) {
                if let valueDesc = decoded as? JSON {
                    let body = valueDesc
                    do {
                        let body = try JSONEncoder().encode(body)
                        req.end(body)
                    } catch {
                        req.end()
                    }
                } else {
                    req.end()
                }
            } else {
                req.end()
            }
        } else {
            req.end()
        }
    }

    /// Get the value for a CouchDB configuration parameter.
    ///
    /// - parameters:
    ///     - keyPath: The configuration parameter String to get the value for.
    ///     - callback: Callback containing the JSON return value for the configuration parameter,
    ///                 or an NSError if one occurred.
    public func getConfig(keyPath: String, callback: @escaping (JSON?, NSError?) -> ()) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties,
                                                         method: "GET",
                                                         path: "/_config/\(keyPath)",
                                                         hasBody: false,
                                                         contentType: "application/json")
        let req = HTTP.request(requestOptions) { response in
            var configError: NSError?
            var configJSON: JSON?
            if let response = response {
                do {
                    var responseData = Data()
                    try response.readAllData(into: &responseData)
                    let body = try JSONDecoder().decode(JSON.self, from: responseData)
                    configJSON = body
                } catch {
                    configError = CouchDBUtils.createError(response.statusCode, id: nil, rev: nil)
                }
            }
            callback(configJSON, configError)
        }

        req.end()
    }

    /// Create a new session for the given user credentials.
    ///
    /// - parameters:
    ///     - name: Username String.
    ///     - password: Password String.
    ///     - callback: `SessionCallback` containing the session cookie and JSON response,
    ///                 or an NSError if one occurred.
    public func createSession(name: String, password: String, callback: @escaping SessionCallback) {
        let requestOptions = CouchDBUtils.prepareRequest(connProperties,
                                                         method: "POST",
                                                         path: "/_session",
                                                         hasBody: true,
                                                         contentType: "application/x-www-form-urlencoded")
        let body = "name=\(name)&password=\(password)"
        let id = "org.couchdb.user:\(name)"

        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var document: JSON?
            var cookie: String?
            if let response = response {
                document = CouchDBUtils.getBodyAsJson(response)

                if response.statusCode != HTTPStatusCode.OK {
                    error = CouchDBUtils.createError(response.statusCode, errorDesc: document, id: id, rev: nil)
                }

                cookie = response.headers["Set-Cookie"]?.first
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: id, rev: nil)
            }
            callback(cookie, document, error)
        }
        req.end(body)
    }

    /// Verify a session cookie.
    ///
    /// - parameters:
    ///     - cookie: String session cookie.
    ///     - callback: `SessionCallback` containing the cookie, JSON response,
    ///                 and an NSError if the user is not authenticated or an error occurred.
    public func getSession(cookie: String, callback: @escaping SessionCallback) {
        var requestOptions: [ClientRequest.Options] = []
        requestOptions.append(.hostname(connProperties.host))
        requestOptions.append(.port(connProperties.port))
        requestOptions.append(.method("GET"))
        requestOptions.append(.path("/_session"))

        var headers = [String : String]()
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = cookie
        requestOptions.append(.headers(headers))

        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var document: JSON?
            if let response = response {
                document = CouchDBUtils.getBodyAsJson(response)

                if response.statusCode != HTTPStatusCode.OK {
                    error = CouchDBUtils.createError(response.statusCode, errorDesc: document, id: nil, rev: nil)
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(cookie, document, error)
        }
        req.end()
    }

    /// Logout a session.
    ///
    /// - parameters:
    ///     - cookie: String session cookie.
    ///     - callback: `SessionCallback` containing the cookie, JSON response,
    ///                 and NSError if one occurred.
    public func deleteSession(cookie: String, callback: @escaping SessionCallback) {
        var requestOptions: [ClientRequest.Options] = []
        requestOptions.append(.hostname(connProperties.host))
        requestOptions.append(.port(connProperties.port))
        requestOptions.append(.method("DELETE"))
        requestOptions.append(.path("/_session"))

        var headers = [String : String]()
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = cookie
        requestOptions.append(.headers(headers))

        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            var document: JSON?
            var cookie: String?
            if let response = response {
                document = CouchDBUtils.getBodyAsJson(response)

                if response.statusCode != HTTPStatusCode.OK {
                    error = CouchDBUtils.createError(response.statusCode, errorDesc: document, id: nil, rev: nil)
                }

                cookie = response.headers["Set-Cookie"]?.first
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: nil, rev: nil)
            }
            callback(cookie, document, error)
        }
        req.end()
    }
}

public struct JSON: Codable {
    public var name: String?
    public var rev: String?
    public var underscoreRev: String?
    public var id: String?
    public var underscoreId: String?
    public var doc: Doc?
    public var value: Value
    public var error: String?
    public var reason: String?
    public var uuids: [String]?
    
    init(name: String?, rev: String?, underscoreRev: String?, id: String?, underscoreId: String?, doc: Doc?, value: Value, error: String?, reason: String?, uuids: [String]?) {
        self.name = name
        self.rev = rev
        self.id = id
        self.doc = doc
        self.value = value
        self.error = error
        self.reason = reason
        self.uuids = uuids
        self.underscoreRev = underscoreRev
        self.underscoreId = underscoreId
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        name = try values.decode(String.self, forKey: .name)
        rev = try values.decode(String.self, forKey: .rev)
        underscoreRev = try values.decode(String.self, forKey: .underscoreRev)
        id = try values.decode(String.self, forKey: .id)
        underscoreId = try values.decode(String.self, forKey: .underscoreId)
        doc = try values.decode(Doc.self, forKey: .doc)
        value = try values.decode(Value.self, forKey: .value)
        error = try values.decode(String.self, forKey: .error)
        reason = try values.decode(String.self, forKey: .reason)
        uuids = try values.decode([String].self, forKey: .uuids)
    }
    
    public enum CodingKeys: String, CodingKey {
        case name
        case rev
        case id
        case doc
        case value
        case error
        case reason
        case uuids
        case underscoreRev = "_rev"
        case underscoreId = "_id"
    }
}

public struct Value: Codable {
    public var value: String?
}

public struct Document: Codable {
    public var rows: [JSON]
    public var id: String?
    public var rev: String?
    public var underscoreRev: String?
    
    public enum CodingKeys: String, CodingKey {
        case rows
        case id
        case rev
        case underscoreRev = "_rev"
    }
}

public struct Doc: Codable {
    public var value: String?
    public var underscoreId: String?
    
    public enum CodingKeys: String, CodingKey {
        case value
        case underscoreId = "_id"
    }
}
