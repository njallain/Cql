//
//  SqlValue.swift
//  Sql
//
//  Created by Neil Allain on 4/14/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public enum SqlValue: Equatable, CustomStringConvertible {
	case null
	case int(Int)
	case real(Double)
	case text(String)
	case bool(Bool)
	case uuid(UUID)
	case data(Data)
	case date(Date)

	public var description: String {
		switch self {
		case .null:
			return "null"
		case .int(let v):
			return v.description
		case .real(let v):
			return v.description
		case .text(let v):
			return v
		case .bool(let v):
			return v ? "true" : "false"
		case .uuid(let v):
			return v.uuidString
		case .data(let v):
			return v.base64EncodedString()
		case .date(let v):
			return v.description
		}
	}
//	var differentValue: SqlValue {
//		switch self {
//		case .int(let v):
//			return .int(v.differentValue)
//		case .real(let v):
//			return .real(v.differentValue)
//			case .
//		}
//	}
}
public struct SqlArgument {
	var name: String
	var value: SqlValue
}

public protocol SqlConvertibleProtocol {
	var sqlValue: SqlValue {get}
	static var defaultSqlValue: SqlValue {get}
}

public protocol SqlConvertible: SqlConvertibleProtocol {
	var differentValue: Self {get}
	static var defaultValue: Self {get}
}

public protocol SqlComparable: SqlConvertible, Comparable, Hashable {
}


public protocol SqlIntRepresentible {
	var intValue: Int {get}
	static var defaultIntValue: Int {get}
	var differentIntValue: Int {get}
	static func value(for: Int) -> SqlIntRepresentible
}
public protocol SqlStringRepresentible {
	var stringValue: String {get}
	static var defaultStringValue: String {get}
	var differentStringValue: String {get}
	static func value(for: String) -> SqlStringRepresentible
}

public typealias SqlIntEnum = SqlIntRepresentible & CaseIterable & Equatable & Codable & SqlComparable
public typealias SqlStringEnum = SqlStringRepresentible & CaseIterable & Equatable & Codable & SqlComparable

public extension SqlIntRepresentible where Self: RawRepresentable & CaseIterable & Equatable, Self.RawValue == Int {
	var intValue: Int { return self.rawValue }
	static var defaultIntValue: Int { return self.defaultValue.rawValue }
	var differentIntValue: Int { return self.differentValue.rawValue }
	static func value(for rawValue: Int) -> SqlIntRepresentible {
		return Self(rawValue: rawValue) ?? defaultValue
	}

	static func <(_ lhs: Self, _ rhs: Self) -> Bool {
		return lhs.rawValue < rhs.rawValue
	}
}
public extension SqlStringRepresentible where Self: RawRepresentable & CaseIterable & Equatable, Self.RawValue == String {
	var stringValue: String { return self.rawValue }
	static var defaultStringValue: String { return self.defaultValue.rawValue }
	var differentStringValue: String { return self.differentValue.rawValue }
	static func value(for rawValue: String) -> SqlStringRepresentible {
		return Self(rawValue: rawValue) ?? defaultValue
	}
	static func <(_ lhs: Self, _ rhs: Self) -> Bool {
		return lhs.rawValue < rhs.rawValue
	}
}


public extension SqlConvertible {
	static var defaultSqlValue: SqlValue { return defaultValue.sqlValue }
}

public extension CaseIterable where Self: Equatable & RawRepresentable {
	var differentValue: Self {
		let another = Self.allCases.first(where: { $0 != self })
		return another ?? self
	}
	static var defaultValue: Self {
		return Self.allCases.first!
	}
}
public extension RawRepresentable where RawValue: SqlConvertible {
	var sqlValue: SqlValue { return self.rawValue.sqlValue }
}

extension SqlValue {
	static func convert(_ value: Any) -> SqlValue? {
		if let v = value as? SqlConvertibleProtocol {
			return v.sqlValue
		}
		return nil
	}
	static func convert<T: Encodable>(encodable: T) throws -> SqlValue {
		let encoded = try JSONEncoder().encode(encodable)
		return .data(encoded)
	}
}

extension Bool: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.bool(self) }
	public var differentValue: Bool { return !self }
	public static var defaultValue: Bool { return false }
}

extension Double: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.real(self) }
	public var differentValue: Double { return self + 1.0 }
	public static var defaultValue: Double { return 0 }
}
extension Float: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.real(Double(self)) }
	public var differentValue: Float { return self + 1 }
	public static var defaultValue: Float { return 0 }
}

extension Int: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(self) }
	public var differentValue: Int { return self + 1 }
	public static var defaultValue: Int { return 0 }
}

extension Int8: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: Int8 { return self + 1 }
	public static var defaultValue: Int8 { return 0 }
}

extension Int16: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: Int16 { return self + 1 }
	public static var defaultValue: Int16 { return 0 }
}

extension Int32: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: Int32 { return self + 1 }
	public static var defaultValue: Int32 { return 0 }
}

extension Int64: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: Int64 { return self + 1 }
	public static var defaultValue: Int64 { return 0 }
}

extension UInt: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: UInt { return self + 1 }
	public static var defaultValue: UInt { return 0 }
}

extension UInt8: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: UInt8 { return self + 1 }
	public static var defaultValue: UInt8 { return 0 }
}

extension UInt16: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: UInt16 { return self + 1 }
	public static var defaultValue: UInt16 { return 0 }
}

extension UInt32: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: UInt32 { return self + 1 }
	public static var defaultValue: UInt32 { return 0 }
}

extension UInt64: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.int(Int(self)) }
	public var differentValue: UInt64 { return self + 1 }
	public static var defaultValue: UInt64 { return 0 }
}

extension String: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.text(self) }
	public var differentValue: String { return self + "a" }
	public static var defaultValue: String { return "" }
}

extension UUID: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.uuid(self) }
	public var differentValue: UUID { return UUID() }
	public static var defaultValue: UUID { return UUID(uuid: uuid_t(UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0), UInt8(0))) }
}

extension Date: SqlComparable {
	public var sqlValue: SqlValue { return SqlValue.date(self) }
	public var differentValue: Date { return self.addingTimeInterval(60) }
	public static var defaultValue: Date { return Date.init(timeIntervalSinceReferenceDate: 0) }
}

extension Data: SqlConvertible {
	public var sqlValue: SqlValue { return SqlValue.data(self) }
	public var differentValue: Data {
		var cp = self
		cp.append(contentsOf: [1])
		return cp
	}
	public static var defaultValue: Data { return Data() }
}

extension SqlValue {
	var intValue: Int? {
		switch self {
		case .int(let n):
			return n
		default:
			return nil
		}
	}
	var realValue: Double? {
		switch self {
		case .real(let n):
			return n
		default:
			return nil
		}
	}
	var textValue: String? {
		switch self {
		case .text(let n):
			return n
		default:
			return nil
		}
	}
	var boolValue: Bool? {
		switch self {
		case .bool(let n):
			return n
		default:
			return nil
		}
	}
	var uuidValue: UUID? {
		switch self {
		case .uuid(let n):
			return n
		default:
			return nil
		}
	}
	var dateValue: Date? {
		switch self {
		case .date(let n):
			return n
		default:
			return nil
		}
	}
	var dataValue: Data? {
		switch self {
		case .data(let n):
			return n
		case .date(let d):
			var data = Data()
			withUnsafeBytes(of: d.timeIntervalSinceReferenceDate) {
				data.append(contentsOf: $0)
			}
			return data
		case .uuid(let id):
			var data = Data()
			withUnsafeBytes(of: id) {
				data.append(contentsOf: $0)
			}
			return data
		default:
			return nil
			}
	}
}

extension Data {
	private static let hexAlphabet = "0123456789abcdef".unicodeScalars.map { $0 }
	private static let alphaValues = mapCharToValue(hexAlphabet)
	public var hexEncodedString: String {
		return String(self.reduce(into: "".unicodeScalars, { (result, value) in
			result.append(Data.hexAlphabet[Int(value/16)])
			result.append(Data.hexAlphabet[Int(value%16)])
		}))
	}
	init?(hexEncodedString: String) {
		var bytes = [UInt8]()
		var highByte: UInt8 = 0
		var highByteRead = false
		for c in hexEncodedString.unicodeScalars {
			guard let v = Data.alphaValues[c] else { return nil }
			
			if !highByteRead {
				highByte = v * 16
				highByteRead = true
			} else {
				let b = v + highByte
				bytes.append(b)
				highByteRead = false
			}
		}
		self.init(bytes)
	}
	private static func mapCharToValue(_ chars: [Unicode.Scalar]) -> [Unicode.Scalar: UInt8] {
		var map = [Unicode.Scalar:UInt8]()
		for (v, c) in chars.enumerated() {
			map[c] = UInt8(v)
		}
		return map
	}
}

extension UUID {
	init?(data: Data) {
		if data.count != 16 { return nil }
		self.init(uuid: uuid_t(
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
			data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]))
	}
}
