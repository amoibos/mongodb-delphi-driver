unit mongodb;

interface

uses
  Bson;

type
  TMongoClient = class(TObject)
  private
    FHandle: Pointer;
  public
    constructor Create(url: string='mongodb://localhost:27017/');
    destructor Destroy; override;
  end;

  TReadMode = (READ_PRIMARY, READ_SECONDARY, READ_PRIMARY_PREFERRED, READ_SECONDARY_PREFFERED, READ_NEAREST);
  TReadPrefs = packed record
    mode: TReadMode;
    tags: TBson;
  end;
  PTReadPrefs= ^TReadPrefs;

  TQUERY_FLAG  = (QUERY_NONE, QUERY_TAILABLE_CURSOR, QUERY_SLAVE_OK, QUERY_OPLOG_REPLAY, QUERY_NO_CURSOR_TIMEOUT, QUERY_AWAIT_DATA, QUERY_EXHAUST, QUERY_PARTIAL);
  TQUERY_FLAGS = set of TQUERY_FLAG;


  TREPLY_FLAGS  = (REPLY_NONE, REPLAY_CURSOR_NOT_FOUND, REPLY_QUERY_FAILURE, REPLY_SHARD_CONFIG_STALE, REPLY_AWAIT_CAPABLE);
  TUPDATE_FLAGS = (UPDATE_NONE, UPDATE_INSERT, UPDATE_MULTI_UPDATE);
  TINSERT_FLAGS = (INSERT_NONE, INSERT_CONTINUE_ON_ERROR);
  TREMOVE_FLAGS = (REMOVE_NONE, REMOVE_SINGLE_REMOVE);

  TMongoCursor = class(TObject)
  private
    FHandle: Pointer;
    FBson: TBson;
  public
    constructor Create; 
    destructor Destroy; override;
    
    function next: boolean;
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
    name: PChar;
    drop_dups: boolean;
    sparse: boolean;
    expire_after_seconds: integer;
    v: integer;
    weight: PTBson;
    default_language: PChar;
    language_override: PChar;
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

  {TReadConcern = packed record

  end;
  PTReadConcern = ^TReadConcern; }

  TMongoCollection = class
  private
    FHandle: Pointer;
    FConnection: TMongoClient;
    FDatabase: string;
    FCollection: string;
  public
    constructor Create(AConnection: TMongoClient; ADatabase: string; ACollection: string);
    destructor Destroy; override;

    function drop(var error: TBsonError): boolean;
    function create_index(const keys: TBson; opt: TIndexOpt; var error: TBsonError): boolean;
    function drop_index(const name: string; var error: TBsonError): boolean;

    function get_name: string;
    function get_last_error: TBson;

    function find(flag: TQUERY_FLAGS; skip: longint; limit: longint; batch_size: longint; const query: TBson; const fields: TBson; read_prefs: PTReadPrefs): TMongoCursor;
    function find_indexes(var error: TBsonError): TMongoCursor;
    function count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64;

    function update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
    function insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
    function remove(flag: TREMOVE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;

    function rename(const new_db: string; const new_name: string; drop_target_before_rename: boolean; var error: TBsonError): boolean;
    function save(const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;

    function copy: TMongoCollection;
    function command_simple(const command: TBson; const read_prefs: PTReadPrefs; var reply: TBson; var error: TBsonError): boolean;

    function validate(const options: TBSon; var reply: TBson; var error: TBsonError): boolean;
    function stats(const options: TBson; var reply: TBson; var error: TBsonError): boolean;
  end;

const
  //origin is the current c mongodb driver in version 1.3.4
  MongoDll = 'libmongoc-1.0.dll';



procedure mongo_init; cdecl; external MongoDll name 'mongoc_init';
procedure mongo_cleanup; cdecl; external MongoDll name 'mongoc_cleanup';

function mongo_client_new(url: PChar): Pointer; cdecl; external MongoDll name 'mongoc_client_new';
procedure mongo_client_destroy(FHandle: Pointer); cdecl; external MongoDll name 'mongoc_client_destroy';


function mongo_client_get_collection(client: Pointer; FDatabase: PChar; FCollection: PChar): Pointer;  cdecl; external MongoDll name 'mongoc_client_get_collection';

procedure mongo_collection_destroy(FCollection: Pointer); cdecl; external MongoDll name 'mongoc_collection_destroy';
function mongo_collection_find(FCollection: Pointer; flag: integer; skip: longint; limit: longint; batch_size: longint; query: Pointer; fields: Pointer; read_prefs: PTReadPrefs): TMongoCursor;  cdecl; external MongoDll name 'mongoc_collection_find';
function mongo_collection_drop(FCollection: Pointer; error: PTBsonError): boolean;   cdecl; external MongoDll name 'mongoc_collection_drop';
function mongo_collection_drop_index(FCollection: Pointer; const name: PChar; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_drop_index';
function mongo_collection_create_index(FCollection: Pointer; const keys: Pointer; opt: Pointer; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_create_index';
function mongo_collection_get_name(FCollection: Pointer): AnsiString; cdecl; external MongoDll name 'mongoc_collection_get_name';
function mongo_collection_count(FCollection: Pointer; const queryflag: integer; query: Pointer; skip: int64; limit: int64; const read_prefs: Pointer): int64; cdecl; external MongoDll name 'mongoc_collection_count';

function mongo_collection_update(FCollection: Pointer; updateflags: integer; const selector: Pointer; const update: Pointer; const write_concern: PTWriteConcern; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_update';
function mongo_collection_insert(FCollection: Pointer; insertflags: integer; const document: Pointer; const write_concern: PTWriteConcern; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_insert';
function mongo_collection_remove(FCollection: Pointer; removeflags: integer; const selector: Pointer; const write_concern: PTWriteConcern; error: PTBsonError): Boolean;  cdecl; external MongoDll name 'mongoc_collection_remove';

function mongo_collection_copy(FCollection: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_collection_copy';
function mongo_collection_stats(FCollection: Pointer; const options: Pointer; reply: Pointer; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_stats';
function mongo_collection_rename(FCollection: Pointer; const new_db: PChar; const new_name: PChar; drop_target_before_rename: boolean; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_rename';
function mongo_collection_save(FCollection: Pointer; document: Pointer; const write_concern: PTWriteConcern; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_save';
function mongo_collection_get_last_error(FCollection: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_collection_get_last_error';
function mongo_collection_validate(FCollection: Pointer; options: Pointer; reply: Pointer; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_validate';
function mongo_collection_command_simple(FCollection: Pointer; const command: Pointer; const read_prefs: Pointer; reply: Pointer; error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_command_simple';
function mongo_collection_find_indexes(FCollection: Pointer; error: PTBsonError): Pointer;  cdecl; external MongoDll name 'mongoc_collection_find_indexes';

procedure mongo_cursor_destroy(const cursor: Pointer);cdecl; external MongoDll name 'mongoc_cursor_destroy';
function mongo_cursor_next(const cursor: Pointer; document: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_next';
function mongo_cursor_has_more(const cursor: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_more';
function mongo_cursor_clone(const cursor: Pointer): Pointer; external MongoDll name 'mongoc_cursor_clone';
function mongo_cursor_current(const cursor: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_cursor_current';

implementation

function utf8_encode(const str: string): string;
begin
  Result := str;
end;

function utf8_decode(const str: string): string;
begin
  Result := str;
end;

constructor TMongoClient.Create(url: string='mongodb://localhost:27017/');
begin
  //inherited;
  FHandle := mongo_client_new(PChar(utf8_encode(url)));
end;

destructor TMongoClient.Destroy;
begin
  mongo_client_destroy(FHandle);
  inherited
end;

constructor TMongoCollection.Create(AConnection: TMongoClient; ADatabase: string; ACollection: string);
var
  utf8_database,
  utf8_collection: string;
begin
  utf8_database := utf8_encode(ADatabase);
  utf8_collection := utf8_encode(ACollection);
  FConnection := AConnection;
  FDatabase := utf8_database;
  FCollection := utf8_collection;
  FHandle := mongo_client_get_collection(FConnection.FHandle, PChar(utf8_database), PChar(utf8_collection));
end;

destructor TMongoCollection.Destroy;
begin
  mongo_collection_destroy(FHandle);
end;

function TMongoCollection.update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_update(FHandle, ord(flag), selector.Handle, update.Handle, write_concern, @error);
end;

function TMongoCollection.insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_insert(FHandle, ord(flag), document.Handle, write_concern, @error);
end;


function TMongoCollection.remove(flag: TREMOVE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_remove(FHandle, ord(flag), selector.Handle, write_concern, @error);
end;

function TMongoCollection.rename(const new_db: string; const new_name: string; drop_target_before_rename: boolean; var error: TBsonError): boolean;
begin
  Result := mongo_collection_rename(FHandle, PChar(new_db), PChar(new_name), drop_target_before_rename, @error);
end;

function TMongoCollection.save(const document: TBson; const write_concern: PTWriteConcern; var error: TBsonError): boolean;
begin
  Result := mongo_collection_save(FHandle, document.Handle, write_concern, @error);
end;

function TMongoCollection.find_indexes(var error: TBsonError): TMongoCursor;
begin
  Result := TMongoCursor.Create;
  Result.FHandle := mongo_collection_find_indexes(FHandle, @error);
end;

function TMongoCollection.find(flag: TQUERY_FLAGS; skip: longint; limit: longint; batch_size: longint; const query: TBson; const fields: TBson; read_prefs: PTReadPrefs): TMongoCursor;
var
  query_handle,
  fields_handle: pointer;
begin
  Result := TMongoCursor.Create;
  query_handle := nil;
  fields_handle := nil;
  if query <> nil then
    query_handle := query.Handle;
  if fields <> nil then
    fields_handle := fields.Handle;

  Result.FHandle := mongo_collection_find(FHandle, 0, skip, limit, batch_size, query_handle, fields_handle, read_prefs);
end;

function TMongoCollection.drop(var error: TBsonError): boolean;
begin
  Result := mongo_collection_drop(FHandle, @error);
end;

function TMongoCollection.get_name: string;
begin
  Result := string(utf8_decode(mongo_collection_get_name(FHandle)));
end;

function TMongoCollection.drop_index(const name: string; var error: TBsonError): boolean;
var
  utf8_name: string;
begin
  utf8_name := utf8_encode(name);
  Result := mongo_collection_drop_index(FHandle, PChar(utf8_name), @error);
end;

function TMongoCollection.create_index(const keys: TBson; opt: TIndexOpt; var error: TBsonError): boolean;
begin
  Result := mongo_collection_create_index(FHandle, keys.Handle, @opt, @error);
end;

function TMongoCollection.count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64;
begin
  Result := mongo_collection_count(FHandle, 0, query.Handle, skip, limit, read_prefs);
end;

function TMongoCollection.get_last_error: TBson;
begin
  Result := TBson.Create(mongo_collection_get_last_error(FHandle));
end;

function TMongoCollection.copy: TMongoCollection;
begin
  Result := TMongoCollection.Create(self.FConnection, self.FDatabase, self.FCollection);
  Result.FHandle := mongo_collection_copy(FHandle);
end;

function TMongoCollection.command_simple(const command: TBson; const read_prefs: PTReadPrefs; var reply: TBson; var error: TBsonError): boolean;
begin
  Result := mongo_collection_command_simple(FHandle, command.Handle, read_prefs, reply.Handle, @error);
end;

function TMongoCollection.validate(const options: TBson; var reply: TBson; var error: TBsonError): boolean;
begin
  Result := mongo_collection_validate(FHandle, options.Handle, reply.Handle, @error);
end;

function TMongoCollection.stats(const options: TBson; var reply: TBson; var error: TBsonError): boolean;
begin
  Result := mongo_collection_stats(FHandle, options.Handle, reply.Handle, @error);
end;

function TMongoCursor.has_next: boolean;
begin
  Result := mongo_cursor_has_more(FHandle);
end;

function TMongoCursor.current: TBson;
begin
  Result := FBson;
end;

function TMongoCursor.next: boolean;
begin
  Result := mongo_cursor_next(FHandle, FBson.PHandle);
end;

destructor TMongoCursor.Destroy;
begin
  FBson.Free;
  mongo_cursor_destroy(FHandle);
  inherited;
end;

constructor TMongoCursor.Create;
begin
  FBson := TBson.Create;
end;

initialization
  mongo_init;

finalization
  mongo_cleanup;

end.
