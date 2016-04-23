unit Bson;

interface

const
  BsonDll = 'libbson-1.0.dll';

type
   TBsonFlags = (BSON_FLAG_NONE, BSON_FLAG_INLINE, BSON_FLAG_STATIC, BSON_FLAG_RDONLY,
              BSON_FLAG_CHILD, BSON_FLAG_IN_CHILD, BSON_FLAG_NO_FREE);


   TBsonOidType = array[0..11] of byte;
   PTBsonOidType = ^TBsonOidType;
   TSubType = (TYPE_EOD, TYPE_DOUBLE, TYPE_TEXT, TYPE_DOCUMENT, TYPE_ARRAY, TYPE_BINARY, TYPE_UNDEFINED,
               TYPE_OID, TYPE_BOOL, TYPE_DATE_TIME, TYPE_NULL, TYPE_REGEX, TYPE_DBPOINTER, TYPE_CODE,
               TYPE_SYMBOL, TYPE_CODEWSCOPE, TYPE_INT32, TYPE_TIMESTAMP, TYPE_INT64{required special handling, TYPE_MAXKEY=7F, TYPE_MINKEY=0xFF});

   TBsonValue = packed record
     value_type: longint;//TSubType;
     padding: integer; //4 bytes
     union_data: array[1..12] of char;
   end;

    TBsonIter = packed record
     raw: PChar;        // The raw buffer being iterated.
     len: longint;      // The length of raw.
     off: longint;      // The offset within the buffer. 
     type_: longint;    // The offset of the type byte.
     Key: longint;      // The offset of the key byte.
     d1: longint;       // The offset of the first data byte.
     d2: longint;       // The offset of the second data byte.
     d3: longint;       // The offset of the third data byte.
     d4: longint;       // The offset of the fourth data byte.
     next_off: longint; // The offset of the next field.
     err_off: longint;  // The offset of the error.
     value: TBsonValue; // Internal value for various state.
   end;
   //PTBsonIter = ^TBsonIter;

   TBsonType = packed record
     flags: longint;
     len: longint;
    // padding: array[1..120] of char;
   end;
   PTBsonType = ^TBsonType;

  TBsonOid = class(TObject)
  public
    BsonOid: TBsonOidType;
    constructor Create; overload;
    constructor Create(const Hexstr: string); overload;
    constructor Create(const BsonOid: TBsonOidType); overload;
    destructor Destroy; override;
    procedure Assign(const Hexstr: string); overload;
    procedure Assign(const BsonOid: TBsonOidType); overload;
    function ToHexstr(const BsonOid: TBsonOid): string; overload;
    function ToHexstr: string; overload;
  end;

   TBson = class(TObject)
   private
     FHandle: Pointer;
     procedure SetHandle(const Value: Pointer);
     function GetPHandle: Pointer;
   public
     constructor Create; overload;
     constructor Create(AHandle: Pointer); overload;
     destructor Destroy; override;

     procedure reinit;
     function compare(other: TBson): integer;
     function concat(var destination: TBson): boolean;
     function copy: TBson;
     function count_keys: integer;
     function equal(other: TBson): boolean;
     function has_field(const Key: string): boolean;
     function as_json: string;

     function append_regex(const Key: string; const regex: string; const options: string): boolean;
     function append_symbol(const Key: string; const value: string): boolean;
     function append_time(const Key: string; value: int64): boolean;
     function append_timestamp(const Key: string; timestamp: integer; increment: integer): boolean;
     function append_datetime(const Key: string; value: int64): boolean;
     function append_array(const Key: string; value: TBson): boolean;
     function append_binary(const Key: string; subtype: TSubType; const binary: string): boolean;
     function append_boolean(const Key: string; value: boolean): boolean;
     function append_code(const Key: string; const javascript: string): boolean;
     function append_code_with_scope(const Key: string; const javascript: string; scope: TBson): boolean;
    // function append_dbpointer(key: string; collection: TMongoCollection): boolean;
    // function append_iter(key: string; iter: TBsonIter): boolean;
     function append_double(const Key: string; value: double): boolean;
     function append_document(const Key: string; value: TBson): boolean;
     function append_document_begin(const Key: string; child: TBson): boolean;
     function append_document_end(child: TBson): boolean;
     function append_int64(const Key: string; value: int64): boolean;
     function append_minkey(const Key: string): boolean;
     function append_maxkey(const Key: string): boolean;
     function append_null(const Key: string): boolean;
     function append_int(const Key: string; value: integer): boolean;
     function append_oid(const Key: string; oid: TBsonOid): boolean;
     function append_text(const Key: string; const value: string): boolean;

     function append_array_begin(const Key: string; const child: TBson): boolean;
     function append_array_end(const child: TBson): boolean;

     property Handle: Pointer read FHandle write SetHandle;
     property PHandle: Pointer read GetPHandle;
   end;

   TBsonError = packed record
     domain: longint;
     code: longint;
     message: array[0..503] of char;
   end;
   PTBsonError = ^TBsonError;

  TBsonIterator = class(TObject)
  private
    FIterStruct: TBsonIter;
    FBsonOid: TBsonOid;
  public
    constructor Create; overload;
    constructor Create(Document: TBson); overload;
    constructor Create(Document: TBson; field: string); overload;
    destructor Destroy; override;

    function Init(Document: TBson): boolean; overload;
    function Init(Document: TBson; Key: string): boolean; overload;

    function find(const Key: string): Boolean;
    function Key: string;
    function next: boolean;
    function typ: integer;//TSubType
    function text: string;
    function int: integer;
    function int64: int64;
    function double: double;
    function boolean: boolean;
    function oid: TBsonOid;

    procedure update_boolean(value: boolean);
    procedure update_int(value: integer);
    procedure update_int64(value: int64);
    procedure update_double(value: double);

  end;

function bson_compare(Document: Pointer; other: Pointer): integer;   cdecl; external BsonDll name 'bson_compare';
function bson_concat(Document: Pointer; var destination: Pointer): boolean; cdecl; external BsonDll name 'bson_concat';
function bson_copy(Document: Pointer): Pointer;  cdecl; external BsonDll name 'bson_copy';
function bson_count_keys(Document: Pointer): integer; cdecl; external BsonDll name 'bson_count_keys';
function bson_equal(Document: Pointer; other: Pointer): boolean; cdecl; external BsonDll name 'bson_equal';
function bson_has_field(Document: Pointer; Key: PChar): boolean; cdecl; external BsonDll name 'bson_has_field';
function bson_as_json(Document: Pointer; len: Pointer): PChar; cdecl; external BsonDll name 'bson_as_json';

function bson_new: Pointer  cdecl; external BsonDll name 'bson_new';
procedure bson_destroy(Document: Pointer) cdecl; external BsonDll name 'bson_destroy';
procedure bson_reinit(Document: Pointer); cdecl; external BsonDll name 'bson_reinit';

procedure bson_oid_init(oid: Pointer; bson_context: Pointer); cdecl; external BsonDll name 'bson_oid_init';

//append functions
function bson_append_int(Document: Pointer; Key: PChar; len_key: integer; value: integer): boolean; cdecl; external BsonDll name 'bson_append_int32';
function bson_append_oid(Document: Pointer; Key: PChar; len_key: integer; oid: Pointer): boolean; cdecl; external BsonDll name 'bson_append_oid';
function bson_append_text(Document: Pointer; Key: PChar; len_key: integer; value: PChar; len_value: integer): Boolean; cdecl; external BsonDll name 'bson_append_utf8';
function bson_append_array_begin(Document: Pointer; Key: PChar; len_key: integer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array_begin';
function bson_append_array_end(Document: Pointer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array_end';


function bson_append_regex(Document: Pointer; Key: PChar; key_len: integer; regex: PChar; options: PChar): boolean; cdecl; external BsonDll name 'bson_append_regex';
function bson_append_symbol(Document: Pointer; Key: PChar; key_len: integer; value: PChar; len_val: integer): boolean; cdecl; external BsonDll name 'bson_append_symbol';
function bson_append_time_t(Document: Pointer; Key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_time_t';
function bson_append_timestamp(Document: Pointer; Key: PChar; key_len: integer; timestamp: integer; increment: integer): boolean; cdecl; external BsonDll name 'bson_append_timestamp';
function bson_append_datetime(Document: Pointer; Key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_date_time';
function bson_append_array(Document: Pointer; Key: PChar; key_len: integer; value: Pointer): boolean; cdecl; external BsonDll name 'bson_append_array';
function bson_append_binary(Document: Pointer; Key: PChar; key_len: integer; subtype: integer; binary: PChar; bin_len: integer): boolean; cdecl; external BsonDll name 'bson_append_binary';
function bson_append_bool(Document: Pointer; Key: PChar; len_key: integer; value: boolean): boolean; cdecl; external BsonDll name 'bson_append_bool';
function bson_append_code(Document: Pointer; Key: PChar; key_len: integer; javascript: PChar): boolean; cdecl; external BsonDll name 'bson_append_code';
function bson_append_code_with_scope(Document: Pointer; Key: PChar; key_len: integer; javascript: PChar; scope: Pointer): boolean; cdecl; external BsonDll name 'bson_append_code_with_scope';
function bson_append_dbpointer(Document: Pointer; Key: PChar; key_len: integer; collection: PChar; oid: Pointer): boolean;  cdecl; external BsonDll name 'bson_append_dbpointer';
//function bson_append_iter(document: Pointer; key: PAnsiString; key_len: integer; iter: Pointer): boolean;   cdecl; external BsonDll name 'bson_append_iter';
function bson_append_double(Document: Pointer; Key: PChar; key_len: integer;value: double): boolean; cdecl; external BsonDll name 'bson_append_double';
function bson_append_document(Document: Pointer; Key: PChar; key_len: integer; value: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document';
function bson_append_document_begin(Document: Pointer; Key: PChar; key_len: integer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document_begin';
function bson_append_document_end(Document: Pointer; child: Pointer): boolean; cdecl; external BsonDll name 'bson_append_document_end';
function bson_append_int64(Document: Pointer; Key: PChar; key_len: integer; value: int64): boolean; cdecl; external BsonDll name 'bson_append_int64';
function bson_append_minkey(Document: Pointer; Key: PChar; key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_min_key';
function bson_append_maxkey(Document: Pointer; Key: PChar;  key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_max_key';
function bson_append_null(Document: Pointer; Key: PChar; key_len: integer): boolean; cdecl; external BsonDll name 'bson_append_null';

//iterator functions
function bson_iter_init(AIter: Pointer; Document: Pointer): Boolean; cdecl; external BsonDll name 'bson_iter_init';
function bson_iter_find(AIter: Pointer; Key: PChar): Boolean; cdecl; external BsonDll name 'bson_iter_find';
function bson_iter_next(AIter: Pointer): Boolean; cdecl; external BsonDll name 'bson_iter_next';
function bson_iter_key(AIter: Pointer): PChar; cdecl; external BsonDll name 'bson_iter_key';
function bson_iter_type(AIter: Pointer): integer; cdecl; external BsonDll name 'bson_iter_type';
function bson_iter_text(AIter: Pointer; var len: integer): PChar; cdecl; external BsonDll name 'bson_iter_utf8';

function bson_iter_int64(AIter: Pointer): int64; cdecl; external BsonDll name 'bson_iter_int64';
function bson_iter_int(AIter: Pointer): integer; cdecl; external BsonDll name 'bson_iter_int32';
function bson_iter_document(AIter: Pointer; len: integer; Document: Pointer): double; cdecl; external BsonDll name 'bson_iter_document';
function bson_iter_code(AIter: Pointer; len: integer): PChar; cdecl; external BsonDll name 'bson_iter_code';
function bson_iter_boolean(AIter: Pointer): boolean; cdecl; external BsonDll name 'bson_iter_bool';
function bson_iter_double(AIter: Pointer): double; cdecl; external BsonDll name 'bson_iter_double';
function bson_iter_oid(AIter: Pointer): PTBsonOIDType; cdecl; external BsonDll name 'bson_iter_oid';
function bson_iter_init_find(AIter: Pointer; Document: Pointer; Key: PChar): boolean; cdecl; external BsonDll name 'bson_iter_init_find';

procedure bson_iter_overwrite_bool(Document: Pointer; value: boolean); cdecl; external BsonDll name 'bson_iter_overwrite_bool';
procedure bson_iter_overwrite_int(Document: Pointer; value: integer); cdecl; external BsonDll name 'bson_iter_overwrite_int32';
procedure bson_iter_overwrite_int64(Document: Pointer; value: int64); cdecl; external BsonDll name 'bson_iter_overwrite_int64';
procedure bson_iter_overwrite_double(Document: Pointer; value: double); cdecl; external BsonDll name 'bson_iter_overwrite_double';


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
  inherited;
end;

procedure TBson.reinit;
begin
 if Assigned(FHandle) then
  begin
    bson_reinit(FHandle);
 end;
end;


function TBson.append_array_begin(const Key: string; const child: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_array_begin(FHandle, PChar(utf8_key), length(utf8_key), child.FHandle);
end;

function TBson.append_array_end(const child: TBson): boolean;
begin
  Result := bson_append_array_end(FHandle, child.FHandle);
end;


function TBson.append_int(const Key: string; value: integer): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_int(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_oid(const Key: string; oid: TBsonOid): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_oid(FHandle, PChar(utf8_key), length(utf8_key), @oid.BsonOid);
end;

function TBson.append_text(const Key: string; const value: string): boolean;
var
  utf8_key,
  utf8_value: string;
begin
  utf8_key := utf8_encode(Key);
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
  Result.Handle := bson_copy(FHandle);
end;

function TBson.count_keys: integer;
begin
  Result := bson_count_keys(FHandle);
end;

function TBson.equal(other: TBson): boolean;
begin
  Result := bson_equal(FHandle, other.FHandle);
end;

function TBson.has_field(const Key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_has_field(FHandle, PChar(utf8_key));
end;

function TBson.append_regex(const Key: string; const regex: string; const options: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_regex(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(utf8_encode(regex)), PChar(utf8_encode(options)));
end;

function TBson.append_symbol(const Key: string; const value: string): boolean;
var
  utf8_key,
  utf8_value: string;
begin
  utf8_key := utf8_encode(Key);
  utf8_value := utf8_encode(value);
  Result := bson_append_symbol(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(utf8_value), length(utf8_value));
end;


function TBson.append_time(const Key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_time_t(FHandle, PChar(utf8_key), length(utf8_key), value);
end;


function TBson.append_timestamp(const Key: string; timestamp: integer; increment: integer): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_timestamp(FHandle, PChar(utf8_key), length(utf8_key),
         timestamp, increment);
end;


function TBson.append_datetime(const Key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_datetime(FHandle, PChar(utf8_key), length(utf8_key), value);
end;


function TBson.append_array(const Key: string; value: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_array(FHandle, PChar(utf8_key), length(utf8_key), value.FHandle);
end;


function TBson.append_binary(const Key: string; subtype: TSubType; const binary: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_binary(FHandle, PChar(utf8_key), length(utf8_key),
         ord(subtype), @binary, length(binary));
end;


function TBson.append_code(const Key: string; const javascript: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_code(FHandle, PChar(utf8_key), length(utf8_key),
         PChar(javascript));
end;


function TBson.append_code_with_scope(const Key: string; const javascript: string; scope: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_code_with_scope(FHandle, PChar(utf8_key), length(utf8_key),
    PChar(javascript), scope.FHandle);
end;

function TBson.append_boolean(const Key: string; value: boolean): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_bool(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_double(const Key: string; value: double): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_double(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_document(const Key: string; value: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_document(FHandle, PChar(utf8_key), length(utf8_key), value.FHandle);
end;

function TBson.append_document_begin(const Key: string; child: TBson): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_document_begin(FHandle, PChar(utf8_key), length(utf8_key), child.FHandle);
end;

function TBson.append_document_end(child: TBson): boolean;
begin
  Result := bson_append_document_end(FHandle, child.FHandle);
end;

function TBson.append_int64(const Key: string; value: int64): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_int64(FHandle, PChar(utf8_key), length(utf8_key), value);
end;

function TBson.append_minkey(const Key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_minkey(FHandle, PChar(utf8_key), length(utf8_key));
end;

function TBson.append_maxkey(const Key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
  Result := bson_append_maxkey(FHandle, PChar(utf8_key), length(utf8_key));
end;

function TBson.append_null(const Key: string): boolean;
var
  utf8_key: string;
begin
  utf8_key := utf8_encode(Key);
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
  if FHandle <> nil then
    bson_destroy(FHandle);

  FHandle := Value;
end;

function TBson.GetPHandle: Pointer;
begin
  Result := @FHandle;
end;

function TBsonIterator.find(const Key: string): Boolean;
begin
  Result := bson_iter_find(@FIterStruct, PChar(utf8_encode(Key)));
end;

constructor TBsonIterator.Create;
begin
  inherited;
  FBsonOid := TBsonOid.Create;
end;

destructor TBsonIterator.Destroy;
begin
  FBsonOid.Free;
  inherited;
end;

function  TBsonIterator.Init(Document: TBson): boolean;begin
  Result := bson_iter_init(@FIterStruct, Document.Handle);
end;

function  TBsonIterator.Init(Document: TBson; Key: string): boolean;
begin
  Result := bson_iter_init_find(@FIterStruct, Document.Handle, PChar(utf8_encode(Key)));
end;


procedure TBsonIterator.update_boolean(value: boolean);
begin
  bson_iter_overwrite_bool(@FIterStruct, value);
end;

procedure TBsonIterator.update_int(value: integer);
begin
  bson_iter_overwrite_int(@FIterStruct, value);
end;

procedure TBsonIterator.update_int64(value: int64);
begin
  bson_iter_overwrite_int64(@FIterStruct, value);
end;

procedure TBsonIterator.update_double(value: double);
begin
  bson_iter_overwrite_double(@FIterStruct, value);
end;

function TBsonIterator.Key: string;
var
  str: PChar;
begin
  str := bson_iter_key(@FIterStruct);

  if str = nil then
    exit;
  Result := String(str);
  Result := utf8_decode(Result);
end;

function TBsonIterator.typ: integer;
begin
  Result := bson_iter_type(@FIterStruct);
end;


constructor TBsonIterator.Create(Document: TBson);
begin
  inherited Create;
  Init(Document);
end;

constructor TBsonIterator.Create(Document: TBson; field: string);
begin
  inherited Create;
  Init(Document, field);
end;

function TBsonIterator.next: boolean;
begin
  Result := bson_iter_next(@FIterStruct);
end;

function TBsonIterator.text: string;
var
  str: PChar;
  len: integer;
begin
  str := bson_iter_text(@FIterStruct, len);

  if (str = nil) or (len = 0) then
    exit;
  SetLength(Result, len);
  Move(str^, Result[1], len);
  Result := utf8_decode(Result);
end;

function TBsonIterator.int: integer;
begin
  Result := bson_iter_int(@FIterStruct);
end;

function TBsonIterator.oid: TBsonOid;
var
  L: PTBsonOidType;
begin
  L := bson_iter_oid(@FIterStruct);

  if not Assigned(l) then Exit;
  
  FBsonOid.BsonOid := L^;
  Result := FBsonOid;
end;

function TBsonIterator.int64: int64;
begin
  Result := bson_iter_int64(@FIterStruct);
end;

function TBsonIterator.double: double;
begin
  Result := bson_iter_double(@FIterStruct);
end;

function TBsonIterator.boolean: boolean;
begin
  Result := bson_iter_boolean(@FIterStruct);
end;

{ TBsonOid }

procedure TBsonOid.Assign(const BsonOid: TBsonOidType);
begin
  self.BsonOid := BsonOid;
end;

procedure TBsonOid.Assign(const Hexstr: string);
var
  i: integer;
  hexstr_cpy: string;
begin
  hexstr_cpy := UpperCase(Hexstr);
  if Length(hexstr_cpy) MOD 2 = 1 then
    hexstr_cpy := '0' + hexstr_cpy;
  if length(hexstr_cpy) DIV 2 <> Length(self.BsonOid) then
    Exit;
  for i := Low(TBsonOidType) to High(TBsonOidType) do
  begin
     self.BsonOid[i] := StrToInt('$' + copy(hexstr_cpy, i * 2 + 1, 2));
  end;
end;

constructor TBsonOid.Create;
begin
  inherited;
  bson_oid_init(@self.BsonOid, nil);
end;

constructor TBsonOid.Create(const BsonOid: TBsonOidType);
begin
  inherited Create;
  self.BsonOid := BsonOid;
end;

constructor TBsonOid.Create(const hexstr: string);
begin
  inherited Create;
  Assign(hexstr);
end;

function TBsonOid.ToHexstr: string;
var
  i: integer;
begin
  Result := '';
  for i := Low(TBsonOidType) to High(TBsonOidType) do
    Result := Result + IntToHex(self.BsonOid[i], 2);
end;

function TBsonOid.ToHexstr(const BsonOid: TBsonOid): string;
var
  i: integer;
begin
  Result := '';
  for i := Low(TBsonOidType) to High(TBsonOidType) do
    Result := Result + IntToHex(BsonOid.BsonOid[i], 2);
end;

destructor TBsonOid.Destroy;
begin
  inherited;
end;

end.
