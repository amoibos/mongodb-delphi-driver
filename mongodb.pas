unit mongodb;

interface

uses Bson;


type
  TMongoClient = class
    handle: Pointer;
    constructor Create(url: string='mongodb://localhost:27017/');
    destructor Destroy; override;
  end;

  TReadMode = (READ_PRIMARY, READ_SECONDARY, READ_PRIMARY_PREFERRED, READ_SECONDARY_PREFFERED, READ_NEAREST);
  TReadPrefs = packed record
    mode: TReadMode;
    tags: TBson;
  end;
  PTReadPrefs= ^TReadPrefs;

  TQUERY_FLAGS  = (QUERY_NONE, QUERY_TAILABLE_CURSOR, QUERY_SLAVE_OK, QUERY_OPLOG_REPLAY, QUERY_NO_CURSOR_TIMEOUT, QUERY_AWAIT_DATA, QUERY_EXHAUST, QUERY_PARTIAL);
  TREPLY_FLAGS  = (REPLY_NONE, REPLAY_CURSOR_NOT_FOUND, REPLY_QUERY_FAILURE, REPLY_SHARD_CONFIG_STALE, REPLY_AWAIT_CAPABLE);
  TUPDATE_FLAGS = (UPDATE_NONE, UPDATE_INSERT, UPDATE_MULTI_UPDATE);
  TINSERT_FLAGS = (INSERT_NONE, INSERT_CONTINUE_ON_ERROR);
  TREMOVE_FLAGS = (REMOVE_NONE, REMOVE_SINGLE_REMOVE);
  TDELETE_FLAGS = (DELETE_NONE, DELETE_SINGLE_REMOVE);

  TMongoCursor = class
    handle: pointer;
    destructor Destroy; override;
    function next(var document: TBson): boolean;
    function has_next: boolean;
    function current: TBson;
  end;
  PTBson = ^TBson;

  TIndexOptGeo = record
    twod_sphere_version: byte;
    twod_bits_precision: byte;
    two_location_min: double;
    two_location_max: double;
    //padding byte *[32]
  end;


  TIndexOpt = packed record
    is_initalized: boolean;
    background: boolean;
    unique: boolean;
    name: PAnsiString;
    drop_dups: boolean;
    sparse: boolean;
    expire_after_seconds: integer;
    v: integer;
    weight: PTBson;
    default_language: PAnsiString;
    language_override: PAnsiString;
    geo_options: TIndexOptGeo;
    storage_options: ^Integer;
    partial_filter_expression: PTBson;
    //padding void *[5]
  end;

  TWriteConcern = packed record
   fsync: shortint;
   journal: shortint;
   w: integer;
   wtimeout: integer;
   wtag: PChar;
   frozen: boolean;
   compiled: TBson;
   compiled_gle: TBson;
  end;
  PTWriteConcern= ^TWriteConcern;


  TMongoCollection = class
    handle: Pointer;
    constructor Create(connection: TMongoClient; database: string; collection: string);
    destructor Destroy; override;

    function drop(var error: TBsonError): boolean;
    function create_index(const keys: TBson; opt: TIndexOpt; var error: TBsonError): boolean;
    function drop_index(const name: string; var error: TBsonError): boolean;

    function get_name: string;

    function find(flag: TQUERY_FLAGS; skip: longint; limit: longint; batch_size: longint; query: TBson; fields: TBson; read_prefs: PTReadPrefs): TMongoCursor;
    function count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64;

    function update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
    function insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
    function delete(flag: TDELETE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
  end;

implementation
const
  //origin is the current c mongodb driver in version 1.3.4
  MongoDll = 'libmongoc-1.0.dll';


function utf8_encode(str: string): string;
begin
  Result := str;
end;

function utf8_decode(str: string): string;
begin
  Result := str;
end;


procedure mongo_init; cdecl; external MongoDll name 'mongoc_init';
procedure mongo_cleanup; cdecl; external MongoDll name 'mongoc_cleanup';

function mongoc_client_new(url: PAnsiString): Pointer; cdecl; external MongoDll name 'mongoc_client_new';
procedure mongo_client_destroy(handle: Pointer); cdecl; external MongoDll name 'mongoc_client_destroy';


function mongo_client_get_collection(client: Pointer; database: PAnsiString; collection: PAnsiString): Pointer;  cdecl; external MongoDll name 'mongoc_client_get_collection';

procedure mongo_collection_destroy(collection: Pointer); cdecl; external MongoDll name 'mongoc_collection_destroy';
function mongo_collection_find(collection: Pointer; flag: integer; skip: longint; limit: longint; batch_size: longint; query: Pointer; fields: Pointer; read_prefs: PTReadPrefs): TMongoCursor;  cdecl; external MongoDll name 'mongoc_collection_find';
function mongo_collection_drop(collection: Pointer; error: Pointer): boolean;   cdecl; external MongoDll name 'mongoc_collection_drop';
function mongo_collection_drop_index(collection: Pointer; const name: PAnsiString; error: Pointer): boolean; cdecl; external MongoDll name 'mongoc_collection_drop_index';
function mongo_collection_create_index(collection: Pointer; const keys: Pointer; opt: Pointer; error: Pointer): boolean; cdecl; external MongoDll name 'mongoc_collection_create_index';
function mongo_collection_get_name(collection: Pointer): AnsiString; cdecl; external MongoDll name 'mongoc_collection_get_name';
function mongo_collection_count(collection: Pointer; const queryflag: integer; query: Pointer; skip: int64; limit: int64; const read_prefs: Pointer): int64; cdecl; external MongoDll name 'mongoc_collection_count';


function mongo_collection_update(collection: Pointer; updateflags: integer; const selector: Pointer; const update: Pointer; const write_concern: Pointer; error: Pointer): boolean; cdecl; external MongoDll name 'mongoc_collection_update';
function mongo_collection_insert(collection: Pointer; insertflags: integer; const document: Pointer; const write_concern: Pointer; error: Pointer): boolean; cdecl; external MongoDll name 'mongoc_collection_insert';
function mongo_collection_delete(collection: Pointer; deleteflags: integer; const selector: Pointer; const write_concern: Pointer; error: Pointer): Boolean;  cdecl; external MongoDll name 'mongoc_collection_delete';


procedure mongo_cursor_destroy(const cursor: Pointer);cdecl; external MongoDll name 'mongoc_cursor_destroy';
function mongo_cursor_next(const cursor: Pointer; document: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_next';
function mongo_cursor_has_more(const cursor: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_more';
function mongo_cursor_clone(const cursor: Pointer): Pointer; external MongoDll name 'mongoc_cursor_clone';
function mongo_cursor_current(const cursor: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_cursor_current';

constructor TMongoClient.Create(url: string='mongodb://localhost:27017/');
begin
  handle := mongoc_client_new(PAnsiString(utf8_encode(url)));
end;

destructor TMongoClient.Destroy;
begin
  mongo_client_destroy(handle);
  handle := nil;
end;


constructor TMongoCollection.Create(connection: TMongoClient; database: string; collection: string);
var
  utf8_database,
  utf8_collection: string;
begin
  utf8_database := utf8_encode(database);
  utf8_collection := utf8_encode(collection);
  handle := mongo_client_get_collection(connection.handle, PAnsiString(utf8_database), PAnsiString(utf8_collection));
end;

destructor TMongoCollection.Destroy;
begin
  mongo_collection_destroy(handle);
  handle := nil;
end;

function TMongoCollection.update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_update(handle, ord(flag), selector.handle, update.handle, write_concern, @error);
end;

function TMongoCollection.insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_insert(handle, ord(flag), document.handle, write_concern, @error);
end;


function TMongoCollection.delete(flag: TDELETE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_delete(handle, ord(flag), selector.handle, write_concern, @error);
end;

function TMongoCollection.find(flag: TQUERY_FLAGS; skip: longint; limit: longint; batch_size: longint; query: TBson; fields: TBson; read_prefs: PTReadPrefs): TMongoCursor;
var
   query_handle,
   fields_handle: pointer;
begin
  Result := TMongoCursor.Create;
  query_handle := nil;
  fields_handle := nil;
  if query <> nil then
     query_handle := query.handle;
  if fields <> nil then
     fields_handle := fields.handle;

  Result.handle := mongo_collection_find(handle, ord(flag), skip, limit, batch_size, query_handle, fields_handle, read_prefs);
end;

function TMongoCollection.drop(var error: TBsonError): boolean;
begin
  Result := mongo_collection_drop(handle, @error);
end;

function TMongoCollection.get_name: string;
begin
  Result := string(utf8_decode(mongo_collection_get_name(handle)));
end;

function TMongoCollection.drop_index(const name: string; var error: TBsonError): boolean;
var
  utf8_name: string;
begin
  utf8_name := utf8_encode(name);
  Result := mongo_collection_drop_index(handle, PAnsiString(utf8_name), @error);
end;

function TMongoCollection.create_index(const keys: TBson; opt: TIndexOpt; var error: TBsonError): boolean;
begin
  Result := mongo_collection_create_index(handle, @keys, @opt, @error);
end;

function TMongoCollection.count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64;
begin
  Result := mongo_collection_count(handle, ord(flag), query.handle, skip, limit, read_prefs); 
end;

function TMongoCursor.has_next: boolean;
begin
  Result := mongo_cursor_has_more(handle);
end;

function TMongoCursor.current: TBson;
begin
  Result := mongo_cursor_current(handle);
end;

function TMongoCursor.next(var document: TBson): boolean;
begin
  Result := mongo_cursor_next(handle, @document);
end;

destructor TMongoCursor.Destroy;
begin
  mongo_cursor_destroy(handle);
  handle := nil;
end;


initialization
  mongo_init;
finalization
  mongo_cleanup;

end.
