//
//  Models.swift
//  SqlTests
//
//  Created by Neil Allain on 4/27/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation
@testable import Cql

struct EncodedObj: Codable {
	var name: String
}

enum StringEnum: String, SqlStringEnum {
	case val1 = "v1"
	case val2 = "v2"
}

enum IntEnum: Int, SqlIntEnum {
	case val1 = 1
	case val2 = 2
}

struct AllTable : Codable, SqlTable {
	var id: UUID = UUID()
	var nid: UUID? = nil
	var n: Int = 0
	var nn: Int? = nil
	var s: String = "text"
	var ns: String? = nil
	var dt: Date = Date()
	var ndt: Date? = nil
	var o: EncodedObj = EncodedObj(name: "obj")
	var no: EncodedObj? = nil
	var ia: [Int] = [1,2,3]
	var nsa: [String]? = nil
	var oa: [EncodedObj] = [EncodedObj(name: "obj")]
	var noa: [EncodedObj]? = nil
	var b: Bool = false
	var nb: Bool? = nil
	var d: Double = 1.1
	var nd: Double? = nil
	var se: StringEnum = .val1
	var nse: StringEnum? = nil
	var ie: IntEnum = .val1
	var nie: IntEnum? = nil
	typealias Key = UUID
	static let children = toMany(\JoinTable.allId)
}


struct JoinTable: SqlTable {
	var id: Int = 0
	var allId: UUID = UUID()
	var childId: Int = 0
	var description: String = ""
	
	static let parent = toOne(AllTable.self, \JoinTable.allId)
	static let child = toOne(ChildTable.self, \JoinTable.childId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent, child]
}

struct ChildTable: SqlTable {
	var id: Int = 0
	var firstName: String = ""
	var lastName: String = ""
	static let parents = toMany(\JoinTable.childId)
	static let tableIndexes = [TableIndex(columnNames: ["firstName", "lastName"], isUnique: true)]
}

extension SqlTable {
	static func renamedSchema<T2: SqlTable>(to newTable: T2.Type) -> TableSchemaProtocol {
		let schema = self.buildSchema()
		let newName = String(describing: newTable)
		return UnknownTableSchema(
			name: newName,
			columns: schema.columns,
			primaryKey: schema.primaryKey,
			indexes: schema.indexes,
			foreignKeys: schema.foreignKeys)
	}
}
