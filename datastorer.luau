-- settings
local DEBUG = false

local AUTO_SAVE_WHEN_CLOSE = false
local AUTO_SAVING = true

-- services
local ReplicatedStorage = game:GetService('ReplicatedStorage')
local StoreService = game:GetService('DataStoreService')
local Run = game:GetService('RunService')
local Players = game:GetService('Players')
local Http = game:GetService('HttpService')

-- modules

local checksum = require('./checksum') -- 	https://github.com/minecoiii2/checksum.lua
local Signal = require('./Signal') -- 		https://github.com/minecoiii2/bettersignal or any signal module

-- deepcopy tables
function deepCopy<T>(t: T): T
	local seen = {}
	
	local function walk(t)
		local c = {}
		seen[t] = true
		
		for k, v in t do
			if type(v) == 'table' then
				if seen[v] then continue end
				v = deepCopy(v)
			end

			c[k] = v
		end
		
		return c
	end

	return walk(t)
end

-- UTC+0 epoch
function epoch(): number
	return os.time(os.date('!*t'))
end

-- simple hash
function hash()
	local full = Http:GenerateGUID(false)
	-- remove the dashes
	return full:sub(1, 8) 
		.. full:sub(10, 13) 
		.. full:sub(15, 18)
		.. full:sub(20, 23)
		.. full:sub(25, 35)
end

-- constants
local MAX_WAIT_EXPONENT = 5 -- max 2^X val, default: 5
local MAX_ATTEMPTS = MAX_WAIT_EXPONENT + 7  -- default: 12 which is 5mins+
local REVESERVE_REQUESTS_FOR_EMERGENCY = 10
local AUTO_SAVE_EVERY = 90 -- every 1 and 1/2 mins

local LOG_HEADER = '[DATASTORER]'

-- variables
local ALIVE = true

-- types
type defaultMetaData = {
	Session: string,
	SessionStartedAt: number,
	DataVersion: number,
}

export type Store = {
	Name: string,
	KeyStores: {[string]: KeyStore},
	Instance: DataStore,

	AutoSave: boolean,

	TemplateData: {any},
	DataVersions: {(any) -> any},
	LatestDataVersion: number,
	TemplateMeta: defaultMetaData & {any},

	CreateKey: (Store, key: string, userIds: {number}?) -> KeyStore,
	ReleaseAllKeys: (Store, emergency: boolean?) -> nil,
	GetKey: (Store, key: string) -> KeyStore,
	IsKeyLoaded: (Store, key: string) -> boolean,
}

type PrivateStore = Store & {
	print: (PrivateStore, message: string, ...any) -> nil,
}

export type KeyStore = {
	Store: Store,
	Key: string,
	UserIds: {number}?,
	Data: {any},
	Meta: {any},
	Loaded: boolean,
	Session: string,
	SessionStartedAt: number,
	Closed: boolean,

	onClosed: Signal.Signal,

	AddPlayer: (KeyStore, player: Player) -> nil,
	Close: (KeyStore) -> nil,
	Load: (KeyStore) -> boolean,
	Update: (KeyStore, emergency: boolean?) -> Signal.Signal<boolean>,
	Release: (KeyStore, emergency: boolean?) -> nil,
}

type PrivateKeyStore = KeyStore & {
	LastChecksum: number,
	LastProcessed: number,

	error: (PrivateKeyStore, err: string, critical: boolean?) -> nil,
	print: (PrivateKeyStore, message: string, ...any) -> nil,
}

type queueItem = {
	keystore: KeyStore,
	onProcessed: Signal.Signal<boolean>,
	emergency: boolean,
	attempt: number,
	lastAttempt: number,
	processing: boolean,
}

type queue = {queueItem} & {
	hasItemForKeyStore: (queue, keystore: KeyStore) -> boolean,
	addItem: (queue, keystore: KeyStore, emergency: boolean?) -> queueItem,
	removeFirstItem: (queue, processed: boolean) -> nil,
	moveFirstItemLast: (queue) -> nil,
	emergencyFlush: (queue) -> nil,
	flush: (queue) -> nil,
}

-- globals
local module = {}
local heartbeat = nil :: thread

function printStd(printFunction, printType: string, message: string, 
	critical: boolean?, location: string?, ...)

	local str = ''

	if critical then
		str ..= 'CRITICAL '
	end
	str ..= printType:upper()
	if location then
		str ..= ` @ {location}`
	end
	str ..= ':'

	printFunction(LOG_HEADER, str, message, ...)
end

local storeList: {[string]: Store} = {}

local store = {}
local keystore = {}

-- queue class
local queueMethods = {}

function queueMethods.addItem(self, keystore: KeyStore, emergency: boolean?)
	local item: queueItem = {
		keystore = keystore,
		attempt = 0,
		lastAttempt = -math.huge,
		onProcessed = Signal.new(),
		emergency = emergency or false,
		processing = false,
	}

	table.insert(self, if emergency then 1 else (#self + 1), item)
	return item
end

function queueMethods.removeFirstItem(self, processed: boolean)
	local item = self[1] :: queueItem
	if item == nil then return end

	table.remove(self, 1)
	item.onProcessed:Fire(processed)

	task.delay(0.1, function()
		item.onProcessed:Destroy()
		table.clear(item)
	end)

	item.keystore.LastProcessed = os.clock()
end

function queueMethods.moveFirstItemLast(self)
	local item = self[1] :: queueItem
	if item == nil then return end
	if #self == 1 then return end

	table.remove(self, 1)
	table.insert(self, item)
end

function queueMethods.emergencyFlush(self)
	repeat -- remove everything, WITHOUT PROCESSING!!!!
		table.remove(self, 1)
	until #self == 0 
end

-- remove non-emergency items
function queueMethods.flush(self)
	for i = #self, 1, -1 do
		local item: queueItem = self[i]
		if item.emergency == true then continue end
		if item.processing == true then continue end
		table.remove(self, i)
	end
end

function queueMethods.hasItemForKeyStore(self, keystore)
	for i, item in self do
		if item.keystore == keystore then
			return true
		end
	end
	return false
end

local queue: queue = setmetatable({}, {__index = queueMethods}) 

-- store class
function store.new(
	name: string, 
	templateData: {any}, 
	dataVersions: {(any) -> any}?,
	templateMeta: {any}?,
	autoSave: boolean?
): Store
	assert(type(name) == 'string', `storeName expected string got {typeof(name)}`)
	if templateData == nil then error('templateData is required') end
	
	local success, result = pcall(StoreService.GetDataStore, StoreService, name)

	if not success then
		printStd(warn, 'ERROR', result, true, `Store {name}`)
		return
	end

	local self = setmetatable({
		Name = name,
		Instance = result,
		KeyStores = {},

		AutoSave = (if autoSave == nil then AUTO_SAVING else autoSave),

		TemplateData = templateData,
		DataVersions = dataVersions or {},
		TemplateMeta = templateMeta or {},
		LatestDataVersion = (if dataVersions == nil then 0 else #dataVersions)
	}, {__index = store})

	storeList[name] = self
	self:print('Creating')
	return self
end

-- private

function store.print(self, message: string, ...)
	if not DEBUG then return end
	printStd(print, 'PRINT', message, false, `Store {self.Name}`, ...)
end

-- public

function store.CreateKey(self: Store, key: string, userIds: {number}?): KeyStore
	assert(type(key) == 'string' or type(key) == 'number', 
		`key expected a string instead got {typeof(key)}`)
	assert(self:GetKey(key) == nil, `key {key} already instanciated`)
	return keystore.new(self, tostring(key), userIds)
end

function store.ReleaseAllKeys(self: Store, emergency: boolean?)
	for i, keystore in self.KeyStores do
		if keystore.Closed then continue end
		task.spawn(keystore.Release, keystore, emergency) -- true for emergency
	end
end

function store.GetKey(self: Store, key: string)
	assert(type(key) == 'string' or type(key) == 'number', 
		`key expected a string instead got {typeof(key)}`)
	return self.KeyStores[tostring(key)]
end

function store.IsKeyLoaded(self: Store, key: string)
	return (self:GetKey(key) or {}).Loaded == true
end

-- keystore class
function keystore.new(store: Store, key: string, userIds: {number}?): KeyStore
	assert(ALIVE, `Cannot create new keystore while shutting down`)
	assert(type(key) == 'string', `key expected a string instead got {typeof(key)}`)
	
	local self = setmetatable({
		Store = store,
		Key = key,
		UserIds = userIds,
		Data = nil,
		Meta = nil,
		Closed = false,
		Loaded = false,
		Session = hash(),
		SessionStartedAt = epoch(),
		LastProcessed = os.clock(),
		LastChecksum = 0,

		onClosed = Signal.new(),

	}, {__index = keystore})

	store.KeyStores[key] = self
	self:print('Creating')
	return self	
end

-- private

function keystore.error(self: KeyStore, err: string, critical: boolean?)
	printStd(warn, 'ERROR', err, critical, `KeyStore {self.Key}`)
end

function keystore.print(self: KeyStore, message: string, ...)
	if not DEBUG then return end
	printStd(print, 'PRINT', message, false, `KeyStore {self.Key}`, ...)
end

-- public

function keystore.AddUserId(self: KeyStore, userId: number)
	if self.UserIds == nil then
		self.UserIds = {}
	end
	assert(table.find(self.UserIds, userId) == nil, `UserId {userId} already added`)
	table.insert(self.UserIds, userId)
end

function keystore.Close(self: KeyStore)
	if self.Closed then return end
	self.Closed = true
	self.Store.KeyStores[self.Key] = nil
	self.onClosed:Fire()
	self:print('Closing')

	task.delay(0.1, function()
		self.onClosed:Destroy()
	end)
end

function keystore.Update(self: KeyStore, emergency: boolean?): Signal.Signal?
	assert(not self.Closed, `Key {self.Key} is closed and cannot update`)
	-- already has on-going request
	if queue:hasItemForKeyStore(self) then return end

	local item = queue:addItem(self, emergency)

	item.onProcessed:Once(function(processed)
		if not processed then
			self:error('Failed to update')
			self:Close()
		end
	end)

	return item.onProcessed
end

function keystore.Load(self: KeyStore)
	assert(not self.Loaded, `Key {self.Key} is already loaded`)
	if queue:hasItemForKeyStore(self) then return end
	local onProcessed: Signal.Signal = self:Update()	

	self:print('Loading')
	local success = onProcessed:Wait()
	self.Loaded = success
	return success
end

function keystore.Release(self: store, emergency: boolean?)
	if not self.Loaded then
		warn(`Attempted to release unloaded key {self.Key}, closing instead`)
		self:Close()
		return
	end
	if queue:hasItemForKeyStore(self) then return end
	
	local onProcessed: Signal.Signal = self:Update(emergency)

	onProcessed:Once(function(processed: boolean)
		if not processed then
			self:error('Failed to save')
		else
			self:print('Release success')
		end

		self:Close()
	end)

	self:print('Releasing')
	return onProcessed:Wait()
end

-- logic

function autoSavingHeartbeat()
	if not AUTO_SAVING then return end
	-- if server is closing, no need to check for auto saving
	if not ALIVE then return end

	local clock = os.clock()

	for i, store in storeList do
		if not store.AutoSave then continue end
		for i, keystore in store.KeyStores do
			if clock < keystore.LastProcessed + AUTO_SAVE_EVERY then continue end
			if queue:hasItemForKeyStore(keystore) then continue end
			local sum = checksum.any(keystore.Data)
			if keystore.LastChecksum == sum then continue end
			keystore.LastChecksum = sum
			keystore.LastProcessed = clock
			keystore:print('Auto-saving')
			keystore:Update()
		end
	end
end

function queueHeartbeat()
	local item: queueItem = queue[1]
	if item == nil then return end

	-- check API budget
	local budget = StoreService:GetRequestBudgetForRequestType(
		Enum.DataStoreRequestType.UpdateAsync)

	if budget <= (if item.emergency then 0 else REVESERVE_REQUESTS_FOR_EMERGENCY) then
		return
	end

	--
	if item.processing then
		-- move first to last
		queue:moveFirstItemLast()
		return
	end

	-- rate limits
	local waitFor = 2 ^ math.min(item.attempt, MAX_WAIT_EXPONENT)

	if os.clock() < item.lastAttempt + waitFor then
		queue:moveFirstItemLast()
		return
	end

	local keystore = item.keystore
	local store = keystore.Store

	-- sanity checks
	if keystore == nil then
		queue:removeFirstItem(false)
		return
	end
	if keystore.Closed then
		queue:removeFirstItem(false)
		return
	end

	--
	item.processing = true
	keystore:print('Processing')

	-- execute the UpdateAsync
	local isLoad = false
	local transformSuccess = true
	local pcallSuccess, result = pcall(
		store.Instance.UpdateAsync, store.Instance, 
		keystore.Key,
		function (old, keyInfo: DataStoreKeyInfo)
			local data: templateData.data
			local meta: templateMeta.metadata
			local userIds: {number}

			if old == nil then
				-- new key
				data = deepCopy(store.TemplateData)
				meta = deepCopy(store.TemplateMeta)
				userIds = keystore.UserIds or {}

				-- create metadata
				meta.Session = keystore.Session
				meta.SessionStartedAt = epoch()
				meta.DataVersion = store.LatestDataVersion
			else
				data = old
				meta = keyInfo:GetMetadata()
				userIds = keyInfo:GetUserIds()

				keystore.UserIds = keystore.UserIds or userIds
			end

			if meta.Session ~= keystore.Session then
				-- resolve conflict

				-- the new session started after ours
				if keystore.SessionStartedAt < meta.SessionStartedAt then
					transformSuccess = false -- we need to terminate our session
				else
					-- if we started after that means we have the priority, take session
					meta.Session = keystore.Session
					meta.SessionStartedAt = epoch()
				end
			end

			-- only if checks passed
			if transformSuccess then
				local dataVersion = meta.DataVersion

				if dataVersion ~= store.LatestDataVersion then
					for i = dataVersion + 1, store.LatestDataVersion, 1 do
						local t = store.DataVersions[i]
						t(data)
						meta.DataVersion = i -- if we fail mid-way it saves our version
					end
				end
			end

			-- if transform success is false then abort basically, 
			-- we load instead of use our data
			isLoad = keystore.Data == nil or transformSuccess == false

			local resultingData = if isLoad then data else keystore.Data
			keystore.Data = resultingData
			keystore.Meta = meta

			assert(transformSuccess, 'transform not successful')
			return resultingData, keystore.UserIds or userIds, meta
		end
	)

	local success = transformSuccess and pcallSuccess

	keystore:print('Processing ended', success)

	if success then
		if isLoad then
			keystore.LastChecksum = checksum.any(keystore.Data)
		end

		-- remove the first queue item, we're done with it
		queue:removeFirstItem(true)
		return
	else
		-- increment the attempt count and check attempt limit
		item.attempt += 1
		item.lastAttempt = os.clock()

		-- check error code family
		local critical = not transformSuccess

		if type(result) == 'string' then
			local matchedCode = result:match('%d%d%d')

			if matchedCode ~= nil then
				local code = tonumber(matchedCode)
				local family = tonumber(matchedCode:sub(1, 1))

				-- 4XX or 1XX is bad request
				if family == 4 or family == 1 then
					critical = true
				end
			end

			keystore:error(result, critical)
		end

		if critical then
			queue:removeFirstItem(false)
			return
		else
			if item.attempt >= MAX_ATTEMPTS then
				queue:removeFirstItem(false)
				return
			end
			
			item.processing = false
			queue:moveFirstItemLast()
		end
	end
end

-- we need to yield
heartbeat = task.spawn(function()
	while true do
		autoSavingHeartbeat()
		queueHeartbeat()
		Run.Heartbeat:Wait()
	end
end)

game:BindToClose(function()
	ALIVE = false

	if AUTO_SAVE_WHEN_CLOSE == true then
		module:FlushQueue() -- flush all non-essentiel items
		task.wait(0) -- wait for custom implementations to finish up

		-- flush queue and release keystores
		for i, store in storeList do
			store:ReleaseAllKeys(true)
		end

		module:WaitForEmptyQueue()
		task.cancel(heartbeat) -- goodbye.
	end
end)

-- return

-- removes all queue items without processing
function module:EmergencyFlushQueue()
	queue:emergencyFlush()
end

-- flush all non-emergency queue items, keeps processing items
function module:FlushQueue()
	queue:flush()
end

-- wait for request queue to empty
function module:WaitForEmptyQueue()
	repeat
		Run.Heartbeat:Wait() -- finish the queue
	until #queue == 0
end

function module:GetDatastore(name: string, templateData: {any}?, 
	dataVersions: {(any) -> any}?, templateMeta: {any}?, autoSave: boolean?): Store
	return storeList[name] or store.new(name, templateData, dataVersions, templateMeta, autoSave)
end

return module