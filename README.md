# DataStorer

Lightweight Queue-based Datastore module.

* **Managed Queue Workflow** – All DataStore operations are handled through a queue to better adhere to rate-limits
* **Rate-Limit Adherence** – Dynamically throttles requests to stay within DataStore rate limits
* **Seamless Metadata Integration** - Stores metadata inside built-in `DataStoreKeyInfo` without affecting keystore data structure 
* **Transparent Data Manipulation** – Work with `KeyStore.Data` directly, no functional interface to deal with here
* **Session Locking & Conflict Resolution** – Basic locking and last-write wins to prevent simultaneous session overwrites
* **Incremental Data Migration** – Supports versioned transformations to evolve data
* **Auto-Saving** – Detects data changes using checksums and periodicly saves
* **Built-in Debugger** - Overview every tiny operation being performed by enabling the `DEBUG` flag

## Requirements

* [checksum.lua](https://github.com/minecoiii2/checksum.lua) for change detection
* A Signal implementation (e.g., [bettersignal](https://github.com/minecoiii2/bettersignal))

## Usage

Require the datastorer module, ensure dependencies are added aswell

```lua
local DataStorer = require('./Datastorer')
```

### Creating a Store

**Creating**
To create a new DataStore, you give a dataTemplate - an object defining the default data for every new key:

```lua
local store = DataStorer:GetDatastore(
    "MyDataStore",
    {
        Coins = 0,
    }
)
```

**Optional: Data Versioning**
If your dataTemplate ever changes, pass a dataVersions array of functions to dynamically update older keys - where each function migrates from one version to the next. 
New keys start at version #dataVersions (or 0 if you ignore this):

```lua
local store = DataStorer:GetDatastore(
    "MyDataStore",
    {
        Coins = 0,
    },
    { 
        function(data) 
            data.Gems = 0 
        end 
    },
)
```

**Optional: Metadata Template**
To attach default metadata to each key, provide a metadataTemplate object:

```lua
local store = DataStorer:GetDatastore(
    "MyDataStore",
    {
        Coins = 0,
    },
    {
        function(data)
            data.Gems = 0 
        end
    },
    {
        TimePlayed = 0,
        LastLogin = os.time(),
    }
)
```

### Working with Keys

Each key holds data. Create and load a key:

```lua
-- numbers are converted to string
local keyStore = store:CreateKey(player.UserId, {player.UserId})

local success = keyStore:Load() -- yields
if success then
    print("Loaded data:", keyStore.Data)
end
```

Update and save data:

```lua
keyStore.Data.Coins += 100

-- wait for auto-save
-- or
-- bypass auto-save & force update
local updateSignal = keyStore:Update()
local success = updateSignal:Wait()

if success then
    print('saved!')
end
```

### Releasing and Closing

When done, release (save and close) a key:

```lua
keyStore:Release() -- yields
```

You can also force-close without saving:

```lua
keyStore:Close()
```

## Advanced Options

### Queue System

The module uses an internal queue to manage all DataStore API requests. Each operation (load, update, release) is wrapped as a queue item with:

* **Sequential Processing**: Ensures only one request per key at a time.
* **Exponential Backoff**: Retries on failure with increasing delays up to a configurable exponent (`MAX_WAIT_EXPONENT`).
* **Emergency Priority**: Critical saves can bypass standard queue order to minimize data loss.

### Auto-Saving

Enable periodic auto-saves based on data changes:

* Set `AUTO_SAVING` to `true`.
* The module checksums data and auto-saves every `AUTO_SAVE_EVERY` seconds when modified.

### Shutdown Behavior

Bind to Roblox shutdown:

* If `AUTO_SAVE_WHEN_CLOSE` is `true`, all pending saves flush and keys are released on `game:BindToClose`.

## API Reference

### Store Methods

| Method                        | Description                                |
| ----------------------------- | ------------------------------------------ |
| `GetDatastore(name, ...)`     | Create or retrieve a Store instance        |
| `:CreateKey(key, userIds?)`   | Allocate a new KeyStore for a specific key |
| `:GetKey(key)`                | Retrieve an existing KeyStore              |
| `:ReleaseAllKeys(emergency?)` | Save/close all open KeyStores              |
| `:IsKeyLoaded(key)`           | Check if a key is loaded                   |

### KeyStore Methods

| Method                 | Description                         |
| ---------------------- | ----------------------------------- |
| `:Load()`              | Load data from DataStore (blocking) |
| `:Update(emergency?)`  | Schedule a save; returns Signal     |
| `:Release(emergency?)` | Save and close key                  |
| `:Close()`             | Close without saving                |
| `:AddUserId(userId)`   | Add a user ID to metadata           |
| `onClosed`             | Signal fired when key is closed     |

### Module Utilities

| Function                 | Description                          |
| ------------------------ | ------------------------------------ |
| `:FlushQueue()`          | Remove all non-emergency queue items |
| `:EmergencyFlushQueue()` | Remove all queue items immediately   |
| `:WaitForEmptyQueue()`   | Yield until queue is fully processed |

## License

do whatever
