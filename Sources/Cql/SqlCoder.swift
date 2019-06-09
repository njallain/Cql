//
//  SqlCoder.swift
//  Skiff
//
//  Created by Neil Allain on 6/9/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public struct SqlCoder<T: Codable> {
	let decode: (SqlReader, String) throws -> T
	let encode: (T, SqlBuilder) throws -> Void

	init() {
		self.decode = SqlCoder<T>.standardDecode
		self.encode = SqlCoder<T>.standardEncode
	}
	private static func standardEncode(_ object: T, to builder: SqlBuilder) throws {
		try object.encode(to: SqlEncoder(to: builder))
	}
	private static func standardDecode(from reader: SqlReader, prefix: String) throws -> T {
		return try T(from: SqlDecoder(from: reader, prefix: prefix))
	}
	
}
