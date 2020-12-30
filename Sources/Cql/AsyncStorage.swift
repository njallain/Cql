//
//  File.swift
//
//
//  Created by Neil Allain on 6/14/19.
//

import Foundation
import Combine


public struct NotFoundError<Key>: Error {
	public let type: String
	public let id: Key
	public init<T>(_ type: T.Type, id: Key) {
		self.type = String(describing: type)
		self.id = id
	}
	public var localizedDescription: String {
		"\(type) with id: \(id) not found"
	}
}
public enum StorageError: Error {
	case released
}
public class AsyncStorage: ChangeSetSource {
	let responseQueue: DispatchQueue
	private let worker: StorageWorker
	public var ready: Result<Bool, Error> { worker.ready }

	/**
	Creates the main async storage that responds on the main queue
	
	- Parameter synchronous: if true, the internal storage will be created synchronously
	- Parameter createStorage: the function the creates the underlying storage object
	*/
	public init(synchronous: Bool = false, createStorage: @escaping () throws -> Storage) {
		self.responseQueue = .main
		self.worker = StorageWorker(createStorage: createStorage, synchronous: synchronous)
		self.responseQueue.setSpecific(key: cqlRoleKey, value: "response")
	}
	/**
	Create an AsyncStorage that uses the same worker queue as storage but publishes it's responses on a different queue
	
	- Parameter responseQueue: the queue that all publishers returned will recieve their events on
	- Parameter storage: the main queue storage that will be shared
	*/
	public init(responseQueue: DispatchQueue, storage: AsyncStorage) {
		self.responseQueue = responseQueue
		self.responseQueue.setSpecific(key: cqlRoleKey, value: "response")
		self.worker = storage.worker
	}
	public func transaction(_ action: @escaping (StorageConnection) throws -> Void) -> AnyPublisher<Void, Error> {
		return worker.transaction(queue: responseQueue, action: action)
	}
	public func save(_ storable: Storable) -> AnyPublisher<Void, Error> {
		return worker.transaction(queue: responseQueue) {
			try storable.save(to: $0)
		}
	}
	public func get<T: PrimaryKeyTable>(_ type: T.Type, _ key: T.Key) -> AnyPublisher<T, Error> {
		return worker.get(queue: responseQueue, type, key)
	}
	public func get<T: PrimaryKeyTable2>(_ type: T.Type, _ id1: T.Key1, _ id2: T.Key2) -> AnyPublisher<T, Error> {
		return worker.get(queue: responseQueue, type, id1, id2)

	}

	public func query<T: Codable>(where query: Query<T>) -> AnyPublisher<[T], Error> {
		return worker.query(queue: responseQueue, where: query)
	}
	public func query<T: Codable>(where query: JoinedQuery<T>) -> AnyPublisher<[T], Error> {
		return worker.query(queue: responseQueue, where: query)
	}
	public func changeSet<T: PrimaryKeyTable>(for type: T.Type) -> ChangeSet<T> {
		return self.worker.changeSet(for: type)
	}
	public func changeSet<T: PrimaryKeyTable2>(for type: T.Type) -> ChangeSet2<T> {
		return self.worker.changeSet(for: type)
	}
}

fileprivate class StorageWorker {
	public private(set) var ready: Result<Bool, Error> = .success(false)
	private var storage: Storage?
	let workQueue: DispatchQueue
	private var initializing: AnyPublisher<Bool, Error>?
	private var initCancel: AnyCancellable?
	private var connectionPool: [StorageConnection] = []
	public init(createStorage: @escaping () throws -> Storage, synchronous: Bool) {
		self.workQueue = DispatchQueue(label: "Cql", qos: .userInitiated)
		self.workQueue.setSpecific(key: cqlRoleKey, value: "work")
		//dlog("storage init, queue: \(queueRole)")
		if (synchronous) {
			do {
				try self.storage = createStorage()
				ready = .success(true)
			} catch {
				ready = .failure(error)
			}
		} else {
			initializing = Future { [weak self] promise in
				guard let self = self else { return }
				defer { self.initializing = nil }
				do {
					try self.storage = createStorage()
					promise(.success(true))
				} catch {
					promise(.failure(error))
				}
			}.eraseToAnyPublisher()
			
			initCancel = initializing?.subscribe(on: workQueue).sink(
				receiveCompletion: { completion in
					if case let .failure(error) = completion {
						self.ready = .failure(error)
					}
			}, receiveValue: { val in
				self.ready = .success(val)
			})
		}
	}

	public func transaction(queue: DispatchQueue, action: @escaping (StorageConnection) throws -> Void) -> AnyPublisher<Void, Error> {
		let pub = future(resultType: Void.self, responseQueue: queue) { connection in
			let txn = try connection.beginTransaction()
			try action(connection)
			try txn.commit()
			return .success(())
		}
		return pub
	}
	public func get<T: PrimaryKeyTable>(queue: DispatchQueue, _ type: T.Type, _ key: T.Key) -> AnyPublisher<T, Error> {
		let pub = future(resultType: type, responseQueue: queue) { conn in
			guard let res = try conn.get(type, key) else {
				throw NotFoundError(T.self, id: key)
			}
			return .success(res)
		}
		return pub
	}
	public func get<T: PrimaryKeyTable2>(queue: DispatchQueue, _ type: T.Type, _ id1: T.Key1, _ id2: T.Key2) -> AnyPublisher<T, Error> {
		let pub = future(resultType: type, responseQueue: queue) { conn in
			//dlog("sending get value, queue: \(queueRole)")
			guard let r = try conn.get(type, id1, id2) else { throw NotFoundError(type, id: (id1, id2)) }
			return .success(r)
		}
		return pub
//		let get = AsyncResult(publisher: pub, initialValue: nil, queue: responseQueue)
//		return get
	}
	/**
	Executes the given action on the work queue and sends the result back on the response queue.
	If
	*/
	public func query<T: Codable>(queue: DispatchQueue, where query: Query<T>) -> AnyPublisher<[T], Error> {

		let pub = future(resultType: [T].self, responseQueue: queue) { conn in
			var results = [T]()
			try conn.fetch(query: query) { result in
				results.append(contentsOf: result)
				return true
			}
			return Result.success(results)
		}
		return pub
//		let q = AsyncResult(publisher: pub, initialValue: [], queue: responseQueue)
//		return q
	}

	public func query<T: Codable>(queue: DispatchQueue, where query: JoinedQuery<T>) -> AnyPublisher<[T], Error> {
		let pub = future(resultType: [T].self, responseQueue: queue) { conn in
			var results = [T]()
			try conn.fetch(query: query) { result in
				results.append(contentsOf: result)
				return true
			}
			return Result.success(results)
		}
		return pub
	}
	public func changeSet<T: PrimaryKeyTable>(for type: T.Type) -> ChangeSet<T> {
		guard let storage = self.storage else {
			// initialization is not yet complete, create a change set with a lazy key allocator
			// that won't so a the allocator will not be needed until an actual insert occurs
			let nextKey: () -> T.Key = { [weak self] in
				guard let storage = self?.storage else {
					fatalError("initialization not complete at time of next key request")
				}
				return storage.keyAllocator(for: type).next()
			}
			let allocator = AnyKeyAllocator(nextKey: nextKey)
			return ChangeSet<T>(allocator)
		}
		return storage.changeSet(for: type)
	}
	public func changeSet<T: PrimaryKeyTable2>(for type: T.Type) -> ChangeSet2<T> {
		guard let storage = self.storage else {
			fatalError("initialization not complete")
		}
		return storage.changeSet(for: type)
	}
	/**
	Returns either a connection from the connection pool or a new connection
	to the underlying storage
	This should only be called inside the work queue and the connection should
	be return to the pool with pool()
	*/
	private func open() throws -> StorageConnection {
		if let conn = connectionPool.popLast() {
			return conn
		}
		dlog("getting connection, queue: \(queueRole)")
//		defer { self.initializing?.leave() }
		switch self.ready {
		case .success:
			break
		case .failure(let err):
			throw err
		}
		guard let storage = self.storage else {
			fatalError("initialzing succeed but storage is nil")
		}
		return try storage.open()
	}

	private func pool(_ conn: StorageConnection) {
		connectionPool.append(conn)
	}

	/**
	Creates a future for the given function that will execute on the work queue and respond on the given response queue.
	*/
	private func future<T>(resultType: T.Type, responseQueue: DispatchQueue, action: @escaping (StorageConnection) throws -> Result<T, Error>) -> AnyPublisher<T, Error> {
		let safeAction: () -> Result<T, Error> = { [weak self] in
			guard let self = self else { return .failure(StorageError.released) }
			do {
				let conn = try self.open()
				defer { self.pool(conn) }
				return try action(conn)
			} catch {
				return .failure(error)
			}
		}
		let future = Future<T, Error> { promise in
			promise(safeAction())
		 }
		switch initializing {
		case .none:
			return future
				.subscribe(on: self.workQueue)
				.receive(on: responseQueue)
				.eraseToAnyPublisher()
		case .some(let initing):
			return initing.flatMap(maxPublishers: .unlimited) { _ in return future }
				.subscribe(on: self.workQueue)
				.receive(on: responseQueue)
				.eraseToAnyPublisher()
		}
	}
}
fileprivate extension Publisher {
	/**
	Starts the work item only once the first request for values has been received.
	This seems to be the latest point the work can be started to avoid a race condition where the downstream subscribers
	on (that are receiving values on the other queue) miss receiving any values generated by the work
	*/
	func startWork(queue: DispatchQueue, work: DispatchWorkItem) -> Publishers.HandleEvents<Self> {
		var started = false
		return self.handleEvents(
			receiveSubscription: nil,
			receiveOutput: nil,
			receiveCompletion: nil,
			receiveCancel: nil,
			receiveRequest: {sub in
				if !started {
					dlog("starting work")
					queue.async(execute: work)
				}
				started = true
		})
	}
}

// debugging help

var logEnabled = true
func dlog(_ msg: String) {
	if logEnabled {
		print(msg)
	}
}
fileprivate let cqlRoleKey = DispatchSpecificKey<String>()
fileprivate var queueRole: String { DispatchQueue.getSpecific(key: cqlRoleKey) ?? "unknown" }


public extension Publisher {
	/**
	Ignores errors and only recieves the values of a query
	*/
	func querySink(recieveValue: @escaping (Output) -> Void) -> AnyCancellable {
		return self.sink(receiveCompletion: { completion in }, receiveValue: recieveValue)
	}
	/**
	Ignores values and only recieves the completion of the transaction
	*/
	func transactionSink(recieveCompletion: @escaping (Subscribers.Completion<Failure>) -> Void) -> AnyCancellable {
		return self.sink(receiveCompletion: recieveCompletion, receiveValue: { val in })
	}
}
