//
//  MigrationModels.swift
//  SqlTests
//
//  Created by Neil Allain on 5/27/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation
@testable import Cql

struct OldModel: SqlTable {
	var id: Int = 0
	var name: String = ""
	var notes: String = ""
	static let tableIndexes = [TableIndex(columnNames: ["name"], isUnique: true)]
	static let children = toMany(\OldJoinModel.parentId)
}

struct NewModel: SqlTable {
	var id: Int = 0
	var other: UUID = UUID()
	var notes: String = ""
	static let tableIndexes = [TableIndex(columnNames: ["other"], isUnique: false)]
}

struct OldModelRename: SqlTable {
	var id: Int = 0
	var name: String = ""
	var notes: String = ""
}
struct NewModelRename: SqlTable {
	var id: Int = 0
	var fullName: String = ""
	var notes: String = ""
}
struct OldJoinModel: SqlTable {
	var id: Int = 0
	var parentId: Int = 0
	var childId: Int = 0
	static let parent = toOne(OldModel.self, \OldJoinModel.parentId)
	static let child = toOne(ChildModel.self, \OldJoinModel.childId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent, child]
}

struct NewJoinModel: SqlTable {
	var id: Int = 0
	var parentId: Int = 0
	var childId: Int = 0
	static let parent = toOne(NewModel.self, \NewJoinModel.parentId)
	static let child = toOne(ChildModel.self, \NewJoinModel.childId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent, child]
}

struct ChildModel: SqlTable {
	var id: Int = 0
	static let parents = toMany(\OldJoinModel.childId)
}

struct OldDefaultChange: SqlTable {
	var id: Int = 0
	var name: String = ""
	var description: String? = nil
}

struct NewDefaultChange: SqlTable {
	var id: Int = 0
	var name: String = "test"
	var description: String = ""
}

struct OldPrimaryKeyChange: SqlTable {
	var id: Int = 0
	var id1: Int = 0
	var id2: Int = 0
}

struct NewPrimaryKeyChange: SqlTable {
	var id: Int = 0
	var id1: Int = 0
	var id2: Int = 0
}
