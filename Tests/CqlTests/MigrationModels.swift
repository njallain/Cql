//
//  MigrationModels.swift
//  SqlTests
//
//  Created by Neil Allain on 5/27/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation
@testable import Cql

struct OldModel: CqlPrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	var notes: String = ""
	static let tableIndexes = [TableIndex(columnNames: ["name"], isUnique: true)]
	static let primaryKey = \OldModel.id
	static let children = toMany(\OldJoinModel.parentId)
}

struct NewModel: CqlPrimaryKeyTable {
	var id: Int = 0
	var other: UUID = UUID()
	var notes: String = ""
	static let tableIndexes = [TableIndex(columnNames: ["other"], isUnique: false)]
	static let primaryKey = \NewModel.id
}

struct OldModelRename: CqlPrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	var notes: String = ""
	static let primaryKey = \OldModelRename.id
}
struct NewModelRename: CqlPrimaryKeyTable {
	var id: Int = 0
	var fullName: String = ""
	var notes: String = ""
	static let primaryKey = \NewModelRename.id
}
struct OldJoinModel: CqlPrimaryKeyTable2 {
	var parentId: Int = 0
	var childId: Int = 0
	static let primaryKey = (\OldJoinModel.parentId, \OldJoinModel.childId)
	static let parent = toOne(OldModel.self, \OldJoinModel.parentId)
	static let child = toOne(ChildModel.self, \OldJoinModel.childId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent, child]
}

struct NewJoinModel: CqlPrimaryKeyTable2 {
	var parentId: Int = 0
	var childId: Int = 0
	static let primaryKey = (\NewJoinModel.parentId, \NewJoinModel.childId)
	static let parent = toOne(NewModel.self, \NewJoinModel.parentId)
	static let child = toOne(ChildModel.self, \NewJoinModel.childId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent, child]
}

struct ChildModel: CqlPrimaryKeyTable {
	var id: Int = 0
	static let primaryKey = \ChildModel.id
	static let parents = toMany(\OldJoinModel.childId)
}

struct OldDefaultChange: CqlPrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	var description: String? = nil
	static let primaryKey = \OldDefaultChange.id
}

struct NewDefaultChange: CqlPrimaryKeyTable {
	var id: Int = 0
	var name: String = "test"
	var description: String = ""
	static let primaryKey = \NewDefaultChange.id
}

struct OldPrimaryKeyChange: CqlPrimaryKeyTable {
	var id1: Int = 0
	var id2: Int = 0
	static let primaryKey = \OldPrimaryKeyChange.id1
}

struct NewPrimaryKeyChange: CqlPrimaryKeyTable {
	var id1: Int = 0
	var id2: Int = 0
	static let primaryKey = \NewPrimaryKeyChange.id2
}
