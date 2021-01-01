//
//  SqlValueRecorder.swift
//  Sql
//
//  Created by Neil Allain on 4/14/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

/**
SqlPropertyPath provides functions to bridge the gap between a type safe Codable and the stringly typed SQL world.
*/
enum SqlPropertyPath {
	/**
	Returns the property (column) name of the given keyPath.
	It does this by:
	- encoding the values of an instance of T and recording the values
	- using the key path to change the property of the instance
	- encode the instance again and determine what property changed
	This will only work properly on flat objects with SqlConvertible properties
	*/
	static func path<T: Codable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V>) -> String? {
		return path(SqlCoder<T>(), row, keyPath: keyPath)
	}

	/**
	Returns the property (column) name of the given keyPath.
	It does this by:
	- encoding the values of an instance of T and recording the values
	- using the key path to change the property of the instance
	- encode the instance again and determine what property changed
	This will only work properly on flat objects with SqlConvertible properties
	*/
	static func path<T: Codable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V?>) -> String? {
		return path(SqlCoder<T>(), row, keyPath: keyPath)
	}
	
	/**
	Returns the property (column) name of the given keyPath.
	It does this by:
	- encoding the values of an instance of T and recording the values
	- using the key path to change the property of the instance
	- encode the instance again and determine what property changed
	This will only work properly on flat objects with SqlConvertible properties
	*/
	static func path<T: SqlTable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V>) -> String? {
		return path(T.sqlCoder, row, keyPath: keyPath)
	}
	
	/**
	Returns the property (column) name of the given keyPath.
	It does this by:
	- encoding the values of an instance of T and recording the values
	- using the key path to change the property of the instance
	- encode the instance again and determine what property changed
	This will only work properly on flat objects with SqlConvertible properties
	*/
	static func path<T: SqlTable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V?>) -> String? {
		return path(T.sqlCoder, row, keyPath: keyPath)
	}
	
	/**
	Returns the property name of the join property.  This will be will be used to determine table alias names
	- Parameter row: a sample row of the join
	- Parameter keyPath: the key path to the joined table
	- Parameter valueKeyPath: a key path of any property on the joined table that can be changed so the changed property can be detected
	*/
	static func path<T: Codable, V: Codable, P: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T,V>, value: V, valueKeyPath: WritableKeyPath<V, P>) -> String? {
		if let cachedName = cachedNames[keyPath] {
			return cachedName
		}
		do {
			if let path = try SqlCoder<T>().path(row, keyPath: keyPath, valueKeyPath: valueKeyPath) {
				cachedNames[keyPath] = path
				return path
			}
		} catch {
			print("unable to determine path of \(String(describing: keyPath))")
		}
		return nil
	}

	/**
	Returns the property name of the join property.  This will be will be used to determine table alias names
	- Parameter row: a sample row of the join
	- Parameter keyPath: the key path to the joined table
	- Parameter valueKeyPath: a key path of any property on the joined table that can be changed so the changed property can be detected
	*/
	static func optionalPath<T: Codable, V: Codable, P: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T,V>, value: V, valueKeyPath: WritableKeyPath<V, P?>) -> String? {
		if let cachedName = cachedNames[keyPath] {
			return cachedName
		}
		do {
			if let path = try SqlCoder<T>().path(row, keyPath: keyPath, valueKeyPath: valueKeyPath) {
				cachedNames[keyPath] = path
				return path
			}
		} catch {
			print("unable to determine path of \(String(describing: keyPath))")
		}
		return nil
	}
	private static func path<T: Codable, V: SqlConvertible>(_ coder: SqlCoder<T>, _ row: T, keyPath: WritableKeyPath<T, V>) -> String? {
		if let cachedName = cachedNames[keyPath] {
			return cachedName
		}
		do {
			if let path = try coder.path(row, keyPath: keyPath) {
				cachedNames[keyPath] = path
				return path
			}
		} catch {
			print("unable to determine path of \(String(describing: keyPath))")
		}
		return nil
	}
	private static func path<T: Codable, V: SqlConvertible>(_ coder: SqlCoder<T>, _ row: T, keyPath: WritableKeyPath<T, V?>) -> String? {
		if let cachedName = cachedNames[keyPath] {
			return cachedName
		}
		do {
			if let path = try coder.path(row, keyPath: keyPath) {
				cachedNames[keyPath] = path
				return path
			}
		} catch {
			print("unable to determine path of \(String(describing: keyPath))")
		}
		return nil
	}

	private static var cachedNames = [AnyKeyPath: String]()
}

/**
Scrambles the values in a Codable so a change can be detected with a changed codable
*/
fileprivate class SqlValueScrambler: SqlReader {
	let startingValues: [String: SqlValue]
	let intRepresentibles: [String: SqlIntRepresentible.Type]
	let stringRepresentibles: [String: SqlStringRepresentible.Type]
	init(startingValues: [String: SqlValue], intRepresentibles: [String: SqlIntRepresentible.Type], stringRepresentibles: [String: SqlStringRepresentible.Type]) {
		self.startingValues = startingValues
		self.intRepresentibles = intRepresentibles
		self.stringRepresentibles = stringRepresentibles
	}
	func getNullableInt(name: String) throws -> Int? {
		let rawValue = startingValues[name]?.intValue
		if let type = intRepresentibles[name] {
			return type.differentOrDefault(from: rawValue)
		}
		return rawValue?.differentValue ?? Int.defaultValue
	}
	
	func getNullableReal(name: String) throws -> Double? {
		return startingValues[name]?.realValue?.differentValue ?? Double.defaultValue
	}
	
	func getNullableText(name: String) throws -> String? {
		let rawValue = startingValues[name]?.textValue
		if let type = stringRepresentibles[name] {
			return type.differentOrDefault(from: rawValue)
		}
		return rawValue?.differentValue ?? String.defaultValue
	}
	
	func getNullableBool(name: String) throws -> Bool? {
		return startingValues[name]?.boolValue?.differentValue ?? Bool.defaultValue
	}
	
	func getNullableDate(name: String) throws -> Date? {
		return startingValues[name]?.dateValue?.differentValue ?? Date.defaultValue
	}
	
	func getNullableUuid(name: String) throws -> UUID? {
		return startingValues[name]?.uuidValue?.differentValue ?? UUID.defaultValue
	}
	
	func getNullableBlob(name: String) throws -> Data? {
		return startingValues[name]?.dataValue?.differentValue ?? Data.defaultValue
	}
	
	func contains(name: String) throws -> Bool {
		return true
	}
}

fileprivate extension SqlIntRepresentible {
	static func differentOrDefault(from value: Int?) -> Int {
		if let value = value {
			return self.value(for: value).differentIntValue
		} else {
			return self.defaultIntValue
		}
	}
}

fileprivate extension SqlStringRepresentible {
	static func differentOrDefault(from value: String?) -> String {
		if let value = value {
			return self.value(for: value).differentStringValue
		} else {
			return self.defaultStringValue
		}
	}
}

extension SqlCoder {
	fileprivate func path<V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V>) throws -> String? {
		let v = row[keyPath: keyPath]
		
		let currentVals = try record(row).valuesByName
		var changedRow = row
		changedRow[keyPath: keyPath] = v.differentValue
		let changedVals = try record(changedRow).valuesByName
		
		for (k,v) in changedVals {
			if let pv = currentVals[k] {
				if v != pv {
					return k
				}
			}
		}
		return nil
	}
	fileprivate func path<P: Codable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, P>, valueKeyPath: WritableKeyPath<P, V>) throws -> String? {
		let p = row[keyPath: keyPath]
		let v = p[keyPath: valueKeyPath]
		let currentVals = try record(row).valuesByName
		var changedRow = row
		var changedCodable = changedRow[keyPath: keyPath]
		changedCodable[keyPath: valueKeyPath] = v.differentValue
		changedRow[keyPath: keyPath] = changedCodable
		let changedVals = try record(changedRow).valuesByName
		
		for (k,v) in changedVals {
			if let pv = currentVals[k] {
				if v != pv {
					return k
				}
			}
		}
		return nil
	}
	fileprivate func path<V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, V?>) throws -> String? {
		var nilRow = row
		let val = V.self.defaultValue
		nilRow[keyPath: keyPath] = val
		let currentVals = try record(nilRow).valuesByName
		
		var changedRow = row
		changedRow[keyPath: keyPath] = val.differentValue
		let changedVals = try record(changedRow).valuesByName
		
		for (k,v) in changedVals {
			if let pv = currentVals[k] {
				if v != pv {
					return k
				}
			}
		}
		return nil
	}
	fileprivate func path<P: Codable, V: SqlConvertible>(_ row: T, keyPath: WritableKeyPath<T, P>, valueKeyPath: WritableKeyPath<P, V?>) throws -> String? {
		let p = row[keyPath: keyPath]
		let v = p[keyPath: valueKeyPath] ?? V.defaultValue
		let currentVals = try record(row).valuesByName
		var changedRow = row
		var changedCodable = changedRow[keyPath: keyPath]
		changedCodable[keyPath: valueKeyPath] = v.differentValue
		changedRow[keyPath: keyPath] = changedCodable
		let changedVals = try record(changedRow).valuesByName
		
		for (k,v) in changedVals {
			if let pv = currentVals[k] {
				if v != pv {
					return k
				}
			}
		}
		return nil
	}

	private func record(_ row: T) throws -> ArgumentBuilder {
		let builder = ArgumentBuilder(collectTypes: true)
		try self.encode(row, builder)
		return builder
	}

	
	/**
	Determines what properties the given change closure sets on an instance of T
	It does this by:
	- Using the change function on a sample instance
	- recording the values of the instance by encoding it
	- scrambling all the properties of the instance (to make sure all of the values are different than what the change function set)
	- using the change function again on the sample instance
	- record the values of the values the instance again
	- detect the differences between the first set of recorded values and the second set
	*/
	func changes(for createRow: () -> T, change: (inout T) -> Void) throws -> [String : SqlValue] {
		var r = createRow()
		// set the values with the change method
		change(&r)
		// scramble those values
		r = try scrambled(r)
		// record scrambled values
		let currentVals = try record(r)
		
		// set the values again with the change method
		change(&r)
		
		// record changes again with a scrambled object so
		let changedVals = try record(r)
		
		return detectDifferences(source: currentVals.valuesByName, target: changedVals.valuesByName)
	}
	
	private func scrambled(_ obj: T) throws -> T {
		let recorded = try record(obj)
		let scrambler = SqlValueScrambler(startingValues: recorded.valuesByName, intRepresentibles: recorded.intRepresentibles!, stringRepresentibles: recorded.stringRepresentibles!)
		return try self.decode(scrambler, "")
	}
	private func detectDifferences(source: [String:SqlValue], target: [String: SqlValue]) -> [String: SqlValue] {
		var changes = [String: SqlValue]()
		for (k,v) in target {
			if let pv = source[k] {
				if v != pv {
					changes[k] = v
				}
			} else {
				changes[k] = v
			}
		}
		for (k,_) in source {
			if target[k] == nil {
				changes[k] = .null
			}
		}
		return changes
	}
}
