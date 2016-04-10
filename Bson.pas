unit Bson;

interface

const
  BsonDll = 'libbson-1.0.dll';

type
   TBsonFlags = (BSON_FLAG_NONE, BSON_FLAG_INLINE, BSON_FLAG_STATIC, BSON_FLAG_RDONLY,
              BSON_FLAG_CHILD, BSON_FLAG_IN_CHILD, BSON_FLAG_NO_FREE);

   TBsonOid = array[0..11] of byte;
   TSubType = (TYPE_EOD, TYPE_DOUBLE, TYPE_UTF8, TYPE_DOCUMENT, TYPE_ARRAY, TYPE_BINARY, TYPE_UNDEFINED,
               TYPE_OID, TYPE_BOOL, TYPE_DATE_TIME, TYPE_NULL, TYPE_REGEX, TYPE_DBPOINTER, TYPE_CODE,
               TYPE_SYMBOL, TYPE_CODEWSCOPE, TYPE_INT32, TYPE_TIMESTAMP, TYPE_INT64, TYPE_MAXKEY, TYPE_MINKEY);
               
   TBson = class
   private
     FHandle: Pointer;
     procedure SetHandle(const Value: Pointer);
    function GetPHandle: Pointer;
   public
     constructor Create; overload;
     constructor Create(AHandle: Pointer); overload;
     destructor Destroy; override;

     function compare(other: TBson): integer;
     function concat(var destination: TBson): boolean;
     function copy: TBson;
     function count_keys: integer;
     function equal(other: TBson): boolean;
     function has_field(key: string): boolean;
     function as_json: string;

     procedure update_boolean(value: boolean);
     procedure update_int(value: integer);
     procedure update_int64(value: int64);
     procedure update_double(value: double);

     function append_regex(key: string; regex: string; options: string): boolean;
     function append_symbol(key: string; value: string): boolean;
     function append_time(key: string; value: int64): boolean;
     function append_timestamp(key: string; timestamp: integer; increment: integer): boolean;
     function append_datetime(key: string; value: int64): boolean;
     function append_array(key: string; value: TBson): boolean;
     function append_binary(key: string; subtype: TSubType; binary: string): boolean;
     function append_bool(key: string; value: boolean): boolean;
     function append_code(key: string; javascript: string): boolean;
     function append_code_with_scope(key: string; javascript: string; scope: TBson): boolean;
    // function append_dbpointer(key: string; collection: TMongoCollection): boolean;
    // function append_iter(key: string; iter: TBsonIter): boolean;
     function append_double(key: string; value: double): boolean;
     function append_document(key: string; value: TBson): boolean;
     function append_document_begin(key: string; child: TBson): boolean;
     function append_document_end(child: TBson): boolean;
     function append_int64(key: string; value: int64): boolean;
     function append_minkey(key: string): boolean;
     function append_maxkey(key: string): boolean;
     function append_null(key: string): boolean;
     function append_int(key: string; value: integer): boolean;
     function append_oid(key: string; oid: TBsonOid): boolean;
     function append_text(key: string; value: string): boolean;

     function append_array_begin(key: string; child: TBson): boolean;
     function append_array_end(child: TBson): boolean;

     property Handle: Pointer read FHandle write SetHandle;
     property PHandle: Pointer read GetPHandle;
   end;

   TBsonError = packed record
     domain: longint;
     code: longint;
     message: array[0..503] of char;
   end;

   PTBsonError = ^TBsonError;

   TBsonType = packed record
     flags: longint;
     len: longint;
     //padding byte 120
   end;
   PTBsonType = ^TBsonType;

   function bson_new_oid: TBsonOid;

function bson_compare(document: Pointer; other: Pointer): integer;   cdecl; external BsonDll name 'bson_compare';
function bson_concat(document: Pointer; var destination: Pointer): boolean; cdecl; external BsonDll name 'bson_concat';
function bson_copy(document: Pointer): Pointer;  cdecl; external BsonDll name 'bson_copy';
function bson_count_keys(document: Pointer): integer; cdecl; external BsonDll name 'bson_count_keys';
function bson_equal(document: Pointer; other: Pointer): boolean; cdecl; external BsonDll name 'bson_equal';
function bson_has_field(document: Pointer; key: PChar): boolean; cdecl; external BsonDll name 'bson_has_field';
function bson_as_json(document: Pointer; len: Pointer): PChar; cdecl; external BsonDll name 'bson_as_json';

function bson_new: Pointer  cdecl; external BsonDll name 'bson_new';
procedure bson_destroy(document: Pointer) cdecl; external BsonDll name 'bson_destroy';

procedure bson_oid_init(oid: Pointer; bson_context: Pointer); cdecl; external BsonDll name 'bson_oid_init';


function bson_append_int(document: Pointer; key: PChar; len_key: integer; value: integer): boolean; cdecl; external BsonDll name 'bson_append_int32';
function bson_append_oid(document: Pointer; key: PChar; len_key: integer; oid: Pointer): boolean; cdecl; external BsonDll name 'bson_append_oid';
function bson_append_text(document: Pointer; key: PChar; len_key: integer; value: PChar; len_value: integer): Boolean; cdecl; external BsonDll name 'bson_append_utf8';
function bson_append_array_begin(document: Pointer; key: PChar; len_key: integer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array_begin';
function bson_append_array_end(document: Pointer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array_end';


function bson_append_regex(document: Pointer; key: PChar; key_len: integer; regex: PChar; options: PChar): boolean; cdecl; external BsonDll name 'bson_append_regex';
function bson_append_symbol(document: Pointer; key: PChar; key_len: integer; value: PChar; len_val: integer): boolean; cdecl; external BsonDll name 'bson_append_symbol';
function bson_append_time_t(document: Pointer; key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_time_t';
function bson_append_timestamp(document: Pointer; key: PChar; key_len: integer; timestamp: integer; increment: integer): boolean; cdecl; external BsonDll name 'bson_append_timestamp';
function bson_append_datetime(document: Pointer; key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_date_time';
function bson_append_array(document: Pointer; key: PChar; key_len: integer; value: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array';
function bson_append_binary(document: Pointer; key: PChar; key_len: integer; subtype: integer; binary: PChar; bin_len: integer): boolean; cdecl; external BsonDll name 'bson_append_binary';
function bson_append_bool(document: Pointer; key: PChar; len_key: integer; value: boolean): boolean; cdecl; external BsonDll name 'bson_append_bool';
function bson_append_code(document: Pointer; key: PChar; key_len: integer; javascript: PChar): boolean; cdecl; external BsonDll name 'bson_append_code';
function bson_append_code_with_scope(document: Pointer; key: PChar; key_len: integer; javascript: PChar; scope: Pointer): boolean; cdecl; external BsonDll name 'bson_append_code_with_scope';
function bson_append_dbpointer(document: Pointer; key: PChar; key_len: integer; collection: PChar; oid: Pointer): boolean;  cdecl; external BsonDll name 'bson_append_dbpointer';
//function bson_append_iter(document: Pointer; key: PAnsiString; key_len: integer; iter: Pointer): boolean;   cdecl; external BsonDll name 'bson_append_iter';
function bson_append_double(document: Pointer; key: PChar; key_len: integer;value: double): boolean; cdecl; external BsonDll name 'bson_append_double';
function bson_append_document(document: Pointer; key: PChar; key_len: integer; value: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document';
function bson_append_document_begin(document: Pointer; key: PChar; key_len: integer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document_begin';
function bson_append_document_end(document: Pointer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document_end';
function bson_append_int64(document: Pointer; key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_int64';
function bson_append_minkey(document: Pointer; key: PChar; key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_min_key';
function bson_append_maxkey(document: Pointer; key: PChar;  key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_max_key';
function bson_append_null(document: Pointer; key: PChar; key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_null';

procedure bson_iter_overwrite_bool(document: Pointer; value: boolean); cdecl; external BsonDll name 'bson_iter_overwrite_bool';
procedure bson_iter_overwrite_int32(document: Pointer; value: integer); cdecl; external BsonDll name 'bson_iter_overwrite_int32';
procedure bson_iter_overwrite_int64(document: Pointer; value: int64); cdecl; external BsonDll name 'bson_iter_overwrite_int64';
procedure bson_iter_overwrite_double(document: Pointer; value: double); cdecl; external BsonDll name 'bson_iter_overwrite_double';

implementation

uses
  SysUtils;

function utf8_encode(str: string): string;
begin
  Result := str;
end;

function utf8_decode(str: string): string;
begin
  Result := str;
end;


function bson_new_oid: TBsonOid;
begin
  bson_oid_init(@Result, nil);
end;

constructor TBson.Create;
begin
  FHandle := bson_new;
end;

constructor TBson.Create(AHandle: Pointer);
begin
  FHandle := AHandle;
end;

destructor TBson.Destroy;
begin
  if Assigned(FHandle) then
    bson_destroy(FHandle);
end;

function TBson.append_array_begin(key: string; child: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_array_begin(FHandle, PChar(utf8_key), length(utf8_key), child.FHandle);
end;

function TBson.append_array_end(child: TBson): boolean;
begin
  Result := bson_append_array_end(FHandle, @child);
end;


function TBson.append_int(key: string; value: integer): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_int(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_oid(key: string; oid: TBsonOid): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_oid(FHandle, PChar(utf8_key), length(utf8_key), @oid);
end;

function TBson.append_text(key: string; value: string): boolean;
var
  utf8_key,
  utf8_value: string;
begin
  utf8_key := utf8_encode(key);
  utf8_value := utf8_encode(value);
  Result := bson_append_text(FHandle, PChar(utf8_key), length(utf8_key), PChar(value), length(utf8_value));
end;

function TBson.compare(other: TBson): integer;
begin
  Result := bson_compare(FHandle, other.FHandle);
end;

function TBson.concat(var destination: TBson): boolean;
begin
  Result := bson_concat(destination.FHandle, FHandle);
end;

function TBson.copy: TBson;
begin
  Result := TBson.Create;
  Result.FHandle := bson_copy(FHandle);
end;

function TBson.count_keys: integer;
begin
  Result := bson_count_keys(FHandle);
end;

function TBson.equal(other: TBson): boolean;
begin
  Result := bson_equal(FHandle, other.FHandle);
end;

function TBson.has_field(key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_has_field(FHandle, PChar(utf8_key));
end;

function TBson.append_regex(key: string; regex: string; options: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_regex(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(utf8_encode(regex)), PChar(utf8_encode(options)));
end;

function TBson.append_symbol(key: string; value: string): boolean;
var
  utf8_key,
  utf8_value: string;
begin
  utf8_key := utf8_encode(key);
  utf8_value := utf8_encode(value);
  Result := bson_append_symbol(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(utf8_value), length(utf8_value));
end;


function TBson.append_time(key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_time_t(FHandle, PChar(utf8_key), length(utf8_key), value);
end;


function TBson.append_timestamp(key: string; timestamp: integer; increment: integer): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_timestamp(FHandle, PChar(utf8_key), length(utf8_key),
         timestamp, increment);
end;


function TBson.append_datetime(key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_datetime(FHandle, PChar(utf8_key), length(utf8_key), value);
end;


function TBson.append_array(key: string; value: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_array(FHandle, PChar(utf8_key), length(utf8_key), value.FHandle);
end;


function TBson.append_binary(key: string; subtype: TSubType; binary: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_binary(FHandle, PChar(utf8_key), length(utf8_key),
         ord(subtype), @binary, length(binary));
end;


function TBson.append_code(key: string; javascript: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_code(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(javascript));
end;


function TBson.append_code_with_scope(key: string; javascript: string; scope: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_code_with_scope(FHandle, PChar(utf8_key), length(utf8_key),
    PChar(javascript), scope.FHandle);
end;

function TBson.append_bool(key: string; value: boolean): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_bool(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_double(key: string; value: double): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_double(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_document(key: string; value: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_document(FHandle, PChar(utf8_key), length(utf8_key), value.FHandle);
end;

function TBson.append_document_begin(key: string; child: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_document_begin(FHandle, PChar(utf8_key), length(utf8_key), child.FHandle);
end;

function TBson.append_document_end(child: TBson): boolean;
begin
  Result := bson_append_document_end(FHandle, child.FHandle);
end;

function TBson.append_int64(key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_int64(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_minkey(key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_minkey(FHandle, PChar(utf8_key), length(utf8_key));
end;

function TBson.append_maxkey(key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_maxkey(FHandle, PChar(utf8_key), length(utf8_key));
end;

function TBson.append_null(key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(key);
  Result := bson_append_null(FHandle, PChar(utf8_key), length(utf8_key));
end;

function TBson.as_json: string;
var
  LJson: PChar;
begin
  LJson := bson_as_json(FHandle, nil);
  Result := utf8_decode(LJson);
  bson_destroy(LJson);
end;

procedure TBson.SetHandle(const Value: Pointer);
begin
  if (FHandle <> nil) then
    bson_destroy(FHandle);

  FHandle := Value;
end;

function TBson.GetPHandle: Pointer;
begin
  Result := @FHandle;
end;

procedure TBson.update_boolean(value: boolean);
begin
  bson_iter_overwrite_bool(Handle, value);
end;

procedure TBson.update_int(value: integer);
begin
  bson_iter_overwrite_int32(Handle, value);
end;

procedure TBson.update_int64(value: int64);
begin
  bson_iter_overwrite_int64(Handle, value);
end;

procedure TBson.update_double(value: double);
begin
  bson_iter_overwrite_double(Handle, value);
end;


end.
