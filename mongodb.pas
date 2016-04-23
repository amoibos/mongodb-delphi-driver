unit MongoDb;

interface

uses
  Bson;

type
  TMongoClient = class(TObject)
  private
    FHandle: Pointer;
    procedure SetHandle(const Value: Pointer);
  public
    constructor Create(const url: string='mongodb://localhost:27017/');
    destructor Destroy; override;

    property Handle : Pointer read FHandle write SetHandle;
  end;

  TReadMode = (READ_PRIMARY, READ_SECONDARY, READ_PRIMARY_PREFERRED,
                             READ_SECONDARY_PREFFERED, READ_NEAREST);
  TReadPrefs = packed record
    mode: TReadMode;
    tags: TBson;
  end;
  PTReadPrefs= ^TReadPrefs;

  TQUERY_FLAGS  = (QUERY_NONE, QUERY_TAILABLE_CURSOR, QUERY_SLAVE_OK,
                              QUERY_OPLOG_REPLAY, QUERY_NO_CURSOR_TIMEOUT,
                              QUERY_AWAIT_DATA, QUERY_EXHAUST, QUERY_PARTIAL);
 
  TUPDATE_FLAGS = (UPDATE_NONE, UPDATE_INSERT, UPDATE_MULTI_UPDATE);
  TINSERT_FLAGS = (INSERT_NONE, INSERT_CONTINUE_ON_ERROR);
  TREMOVE_FLAGS = (REMOVE_NONE, REMOVE_SINGLE_REMOVE);

  TMongoCursor = class(TObject)
  private
    FHandle: Pointer;
    FBson: TBson;
    procedure SetHandle(const Value: Pointer);
  public
    constructor Create;
    destructor Destroy; override;

    function next: boolean;
    function has_next: boolean;
    function current: TBson;
    property Handle : Pointer read FHandle write SetHandle;
  end;

  TIndexOptGeo = packed record
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
    weight: PTBsonType;
    default_language: PChar;
    language_override: PChar;
    geo_options: TIndexOptGeo;
    storage_options: ^Integer;
    partial_filter_expression: PTBsonType;
    //padding void *[5]
  end;

  TWriteConcern = packed record
   fsync: shortint;
   journal: shortint;
   w: integer;
   wtimeout: integer;
   wtag: PChar;
   frozen: boolean;
   compiled: PTBsonType;
   compiled_gle: PTBsonType;
  end;
  PTWriteConcern= ^TWriteConcern;

  TMongoCollection = class(TObject)
  private
    FHandle: Pointer;
    FConnection: TMongoClient;
    FDatabase: string;
    FCollection: string;
    FQueryCount: Integer;
    procedure SetHandle(const Value: Pointer);
  public
    constructor Create(AConnection: TMongoClient; const ADatabase: string; const ACollection: string);
    destructor Destroy; override;

    function drop(var Error: TBsonError): boolean;
    function create_index(const keys: TBson; var Error: TBsonError): boolean; overload;
    function create_index(const keys: TBson; opt: TIndexOpt; var Error: TBsonError): boolean; overload;
    function drop_index(const name: string; var Error: TBsonError): boolean;

    function get_name: string;
    function get_last_error: TBson;

    function find_and_modify(query: TBson; sort: TBson; update: TBson; fields: TBson; remove: boolean; upsert: boolean; new: boolean; Reply: TBson; var Error: TBsonError): boolean;
    function find(const query: TBson; limit: longint=0): TMongoCursor; overload;
    function find(flag: TQUERY_FLAGS; skip: longint; limit: longint; batch_size: longint; const query: TBson; const fields: TBson; read_prefs: PTReadPrefs): TMongoCursor; overload;
    procedure find(ACursor: TMongoCursor; const query: TBson; limit: longint=0); overload;
    procedure find(ACursor: TMongoCursor; flag: TQUERY_FLAGS; skip, limit, batch_size: Integer; const query, fields: TBson; read_prefs: PTReadPrefs); overload;


    function find_indexes(var Error: TBsonError): TMongoCursor;
    function count(const query: TBson; limit: int64=0): int64; overload;
    function count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64; overload;

    function update(const selector: TBson; const update: TBson; var Error: TBsonError): boolean; overload;
    function update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var Error: TBsonError): boolean; overload;
    function insert(const document: TBson; var Error: TBsonError): boolean; overload;
    function insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern; var Error: TBsonError): boolean; overload;
    function remove(const selector: TBson; var Error: TBsonError): boolean; overload;
    function remove(flag: TREMOVE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern; var Error: TBsonError): boolean; overload;

    function rename(const new_db: string; const new_name: string; drop_target_before_rename: boolean; var Error: TBsonError): boolean;
    function save(const document: TBson; const write_concern: PTWriteConcern; var Error: TBsonError): boolean;

    function copy: TMongoCollection;
    function command_simple(const command: TBson; const read_prefs: PTReadPrefs; var Reply: TBson; var Error: TBsonError): boolean;

    function validate(const options: TBSon; var Reply: TBson; var Error: TBsonError): boolean;
    function stats(const options: TBson; var Reply: TBson; var Error: TBsonError): boolean;

    property QueryCount: Integer read FQueryCount;
    property Handle : Pointer read FHandle write SetHandle;

    property Database: string read FDatabase;
    property Collection: string read FCollection;
  end;

const
  //origin is the current c mongodb driver in version 1.3.4
  MongoDll = 'libmongoc-1.0.dll';



procedure mongo_init; cdecl; external MongoDll name 'mongoc_init';
procedure mongo_cleanup; cdecl; external MongoDll name 'mongoc_cleanup';

function mongo_client_new(url: PChar): Pointer; cdecl; external MongoDll name 'mongoc_client_new';
procedure mongo_client_destroy(Handle: Pointer); cdecl; external MongoDll name 'mongoc_client_destroy';


function mongo_client_get_collection(client: Pointer; FDatabase: PChar; FCollection: PChar): Pointer;  cdecl; external MongoDll name 'mongoc_client_get_collection';

procedure mongo_collection_destroy(FCollection: Pointer); cdecl; external MongoDll name 'mongoc_collection_destroy';

function mongo_collection_find(FCollection: Pointer; flag: integer; skip: longint; limit: longint; batch_size: longint; query: Pointer; fields: Pointer; read_prefs: PTReadPrefs): TMongoCursor;  cdecl; external MongoDll name 'mongoc_collection_find';
function mongo_collection_drop(FCollection: Pointer; Error: PTBsonError): boolean;   cdecl; external MongoDll name 'mongoc_collection_drop';
function mongo_collection_drop_index(FCollection: Pointer; const name: PChar; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_drop_index';
function mongo_collection_create_index(FCollection: Pointer; const keys: Pointer; opt: Pointer; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_create_index';
function mongo_collection_get_name(FCollection: Pointer): AnsiString; cdecl; external MongoDll name 'mongoc_collection_get_name';
function mongo_collection_count(FCollection: Pointer; const queryflag: integer; query: Pointer; skip: int64; limit: int64; const read_prefs: Pointer): int64; cdecl; external MongoDll name 'mongoc_collection_count';

function mongo_collection_update(FCollection: Pointer; updateflags: integer; const selector: Pointer; const update: Pointer; const write_concern: PTWriteConcern; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_update';
function mongo_collection_insert(FCollection: Pointer; insertflags: integer; const document: Pointer; const write_concern: PTWriteConcern; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_insert';
function mongo_collection_remove(FCollection: Pointer; removeflags: integer; const selector: Pointer; const write_concern: PTWriteConcern; Error: PTBsonError): Boolean;  cdecl; external MongoDll name 'mongoc_collection_remove';

function mongo_collection_copy(FCollection: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_collection_copy';
function mongo_collection_stats(FCollection: Pointer; const options: Pointer; Reply: Pointer; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_stats';
function mongo_collection_rename(FCollection: Pointer; const new_db: PChar; const new_name: PChar; drop_target_before_rename: boolean; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_rename';
function mongo_collection_save(FCollection: Pointer; document: Pointer; const write_concern: PTWriteConcern; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_save';
function mongo_collection_get_last_error(FCollection: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_collection_get_last_error';
function mongo_collection_validate(FCollection: Pointer; options: Pointer; Reply: Pointer; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_validate';
function mongo_collection_command_simple(FCollection: Pointer; const command: Pointer; const read_prefs: Pointer; Reply: Pointer; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_command_simple';
function mongo_collection_find_indexes(FCollection: Pointer; Error: PTBsonError): Pointer;  cdecl; external MongoDll name 'mongoc_collection_find_indexes';
function mongo_collection_find_and_modify(FCollection: Pointer; Query: PTBsonType; Sort: PTBsonType; Update: PTBsonType; Fields: PTBsonType; Remove: boolean; Upsert: boolean; new: boolean; Reply: PTBsonType; Error: PTBsonError): boolean; cdecl; external MongoDll name 'mongoc_collection_find_and_modify';

procedure mongo_cursor_destroy(const cursor: Pointer);cdecl; external MongoDll name 'mongoc_cursor_destroy';
function mongo_cursor_next(const cursor: Pointer; document: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_next';
function mongo_cursor_has_more(const cursor: Pointer): Boolean; cdecl; external MongoDll name 'mongoc_cursor_more';
function mongo_cursor_clone(const cursor: Pointer): Pointer; external MongoDll name 'mongoc_cursor_clone';
function mongo_cursor_current(const cursor: Pointer): Pointer; cdecl; external MongoDll name 'mongoc_cursor_current';

implementation

function utf8_encode(const str: string): string;
begin
  Result:=str;
end;

function utf8_decode(const str: string): string;
begin
  Result:=str;
end;

constructor TMongoClient.Create(const url: string='mongodb://localhost:27017/');
begin
  inherited Create;
  Handle:=mongo_client_new(PChar(utf8_encode(url)));
end;

destructor TMongoClient.Destroy;
begin
  mongo_client_destroy(Handle);
  inherited;
end;

constructor TMongoCollection.Create(AConnection: TMongoClient; const ADatabase: string; const ACollection: string);
var
  utf8_database,
  utf8_collection: string;
begin
  inherited Create;
  utf8_database:=utf8_encode(ADatabase);
  utf8_collection:=utf8_encode(ACollection);
  FConnection:=AConnection;
  FDatabase:=utf8_database;
  FCollection:=utf8_collection;
  Handle:=mongo_client_get_collection(FConnection.Handle, PChar(utf8_database), PChar(utf8_collection));
end;

destructor TMongoCollection.Destroy;
begin
  mongo_collection_destroy(Handle);
  inherited;
end;

function TMongoCollection.update(const selector: TBson; const update: TBson; var Error: TBsonError): boolean;
begin
  Result:=self.update(UPDATE_NONE, selector, update, nil, Error);
end;

function TMongoCollection.update(flag: TUPDATE_FLAGS; const selector: TBson; const update: TBson; const write_concern: PTWriteConcern; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_update(Handle, ord(flag), selector.Handle, update.Handle, write_concern, @Error);
end;

function TMongoCollection.insert(const document: TBson; var Error: TBsonError): Boolean;
begin
  Result:=insert(INSERT_NONE, document, nil, Error);
end;


function TMongoCollection.insert(flag: TINSERT_FLAGS; const document: TBson; const write_concern: PTWriteConcern;
                                       var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_insert(Handle, ord(flag), document.Handle, write_concern, @Error);
end;

function TMongoCollection.remove(const selector: TBson; var Error: TBsonError): boolean;
begin
  Result:=remove(REMOVE_NONE, selector, nil, Error);
end;


function TMongoCollection.remove(flag: TREMOVE_FLAGS; const selector: TBson; const write_concern: PTWriteConcern;
                                       var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_remove(Handle, ord(flag), selector.Handle, write_concern, @Error);
end;

function TMongoCollection.rename(const new_db: string; const new_name: string;
         drop_target_before_rename: boolean; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_rename(Handle, PChar(new_db), PChar(new_name), drop_target_before_rename, @Error);
end;

function TMongoCollection.save(const document: TBson; const write_concern: PTWriteConcern;
                                     var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_save(Handle, document.Handle, write_concern, @Error);
end;

function TMongoCollection.find_indexes(var Error: TBsonError): TMongoCursor;
begin
  Result:=TMongoCursor.Create;
  Result.Handle:=mongo_collection_find_indexes(Handle, @Error);
end;

function TMongoCollection.find(const query: TBson; limit: longint=0): TMongoCursor;
begin
  Result:=find(QUERY_NONE, 0, limit, 0, query, nil, nil);
end;


//helper to avoid redundancy
function AccessAttributeIfPossible(bson: TBson): pointer;
begin
  if bson <> nil then
    Result:=bson.Handle
  else
    Result:=bson;
end;


procedure TMongoCollection.find(ACursor: TMongoCursor; flag: TQUERY_FLAGS; skip: longint; limit: longint;
                               batch_size: longint; const query: TBson;
                               const fields: TBson; read_prefs: PTReadPrefs);
var
  query_handle,
  fields_handle: pointer;
  number: integer;
begin
  Inc(FQueryCount);
  query_handle := AccessAttributeIfPossible(query);
  fields_handle:= AccessAttributeIfPossible(fields);

  //strange but so defined in mongoc_flags.h
  number:=ord(flag);
  if number > 0 then
     number:=1 shl ord(flag);
 
  ACursor.Handle:=mongo_collection_find(Handle, number, skip, limit, batch_size, query_handle, fields_handle, read_prefs);
end;

function TMongoCollection.drop(var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_drop(Handle, @Error);
end;

function TMongoCollection.get_name: string;
begin
  Result:=utf8_decode(string(mongo_collection_get_name(Handle)));
end;

function TMongoCollection.drop_index(const name: string; var Error: TBsonError): boolean;
var
  utf8_name: string;
begin
  utf8_name:=utf8_encode(name);
  Result:=mongo_collection_drop_index(Handle, PChar(utf8_name), @Error);
end;

function TMongoCollection.create_index(const keys: TBson; var Error: TBsonError): boolean; 
begin
  Result:=mongo_collection_create_index(Handle, keys.Handle, nil, @Error);
end;

function TMongoCollection.create_index(const keys: TBson; opt: TIndexOpt; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_create_index(Handle, keys.Handle, @opt, @Error);
end;

function TMongoCollection.count(const query: TBson; limit: int64=0): int64;
begin
  Result:=self.count(QUERY_NONE, query, 0, limit, nil);
end;

function TMongoCollection.count(flag: TQUERY_FLAGS; const query: TBson; skip: int64; limit: int64; read_prefs: PTReadPrefs): int64;
begin
  Result:=mongo_collection_count(Handle, 0, query.Handle, skip, limit, read_prefs);
end;

function TMongoCollection.get_last_error: TBson;
begin
  Result:=TBson.Create(mongo_collection_get_last_error(Handle));
end;

function TMongoCollection.copy: TMongoCollection;
begin
  Result:=TMongoCollection.Create(self.FConnection, self.FDatabase, self.FCollection);
  Result.Handle:=mongo_collection_copy(Handle);
end;

function TMongoCollection.command_simple(const command: TBson; const read_prefs: PTReadPrefs; var Reply: TBson; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_command_simple(Handle, command.Handle, read_prefs, Reply.Handle, @Error);
end;

function TMongoCollection.validate(const options: TBson; var Reply: TBson; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_validate(Handle, options.Handle, Reply.Handle, @Error);
end;

function TMongoCollection.stats(const options: TBson; var Reply: TBson; var Error: TBsonError): boolean;
begin
  Result:=mongo_collection_stats(Handle, options.Handle, Reply.Handle, @Error);
end;

function TMongoCollection.find_and_modify(Query: TBson; Sort: TBson; Update: TBson; Fields: TBson; Remove: boolean; Upsert: boolean; New: boolean; Reply: TBson; var Error: TBsonError): boolean;
var
  query_handle,
  sort_handle,
  update_handle,
  fields_handle,
  reply_handle : PTBsonType;
begin
  query_handle:=AccessAttributeIfPossible(Query);
  sort_handle:=AccessAttributeIfPossible(Sort);
  update_handle:=AccessAttributeIfPossible(Update);
  fields_handle:=AccessAttributeIfPossible(Fields);
  reply_handle:=AccessAttributeIfPossible(Reply);
  Result:=mongo_collection_find_and_modify(Handle, query_handle, sort_handle, update_handle, fields_handle, Remove, Upsert, New, reply_handle, @Error);
end;


function TMongoCursor.has_next: boolean;
begin
  Result:=mongo_cursor_has_more(Handle);
end;

function TMongoCursor.current: TBson;
begin
  Result:=FBson;
end;

function TMongoCursor.next: boolean;
begin
  Result:=mongo_cursor_next(Handle, FBson.PHandle);
 { if not Result then
    mongo_cursor_destroy(FHandle); }
end;

destructor TMongoCursor.Destroy;
begin
  FBson.Free;
  inherited;
end;

constructor TMongoCursor.Create;
begin
  if Assigned(FBson) then
    FBson.Free;
  FBson:=TBson.Create;
  FHandle:=nil;
end;

function TMongoCollection.find(flag: TQUERY_FLAGS; skip, limit,
  batch_size: Integer; const query, fields: TBson;
  read_prefs: PTReadPrefs): TMongoCursor;
begin
  //returned instance cursor should be freed
  Result:=TMongoCursor.Create;
  find(Result, flag, skip, limit, batch_size, query, fields, read_prefs);
end;

procedure TMongoCollection.find(ACursor: TMongoCursor; const query: TBson;
  limit: Integer);
begin
  find(ACursor, QUERY_NONE, 0, limit, 0, query, nil, nil);
end;

procedure TMongoCursor.SetHandle(const Value: Pointer);
begin
  if Assigned(FHandle) then
      mongo_cursor_destroy(FHandle);
  FHandle:=Value;
end;

procedure TMongoClient.SetHandle(const Value: Pointer);
begin
  if Assigned(FHandle) then
      mongo_client_destroy(FHandle);
  FHandle:=Value;
end;


procedure TMongoCollection.SetHandle(const Value: Pointer);
begin
  if Assigned(FHandle) then
      mongo_collection_destroy(FHandle);
  FHandle:=Value;
end;


initialization
  mongo_init;

finalization
  mongo_cleanup;

end.

