unit mongodb_tester;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  StdCtrls,
  mongodb,
  Bson;

type
  TForm1 = class(TForm)
    mmo1: TMemo;
    btn1: TButton;
    procedure btn1Click(Sender: TObject);
  private
    { Private-Deklarationen }
  public
    { Public-Deklarationen }
  end;

var
  Form1: TForm1;


implementation

{$R *.DFM}

procedure TForm1.btn1Click(Sender: TObject);
var
  client: TMongoClient;
  collection: TMongoCollection;
  doc: TBson;
  oid: TBsonOid;
  bool: boolean;
  error:TBsonError;
  query: TBson;
  cursor: TMongoCursor;
begin

    client := TMongoClient.Create;
    collection := TMongoCollection.Create(client, 'test', 'test');

    doc := TBson.Create;

    oid := bson_new_oid;


    doc.append_oid('_oid', oid);
    doc.append_text('hello', 'world');
    bool := collection.insert(INSERT_NONE, doc, nil, error);

    doc.free;

    doc := TBson.Create;
    doc.append_oid('oid', oid);


    bool := collection.delete(DELETE_SINGLE_REMOVE, doc, nil, error);

    query := TBson.Create;
    cursor := collection.find(QUERY_NONE, 0, 0, 0, query, nil, nil);
    while cursor.next(doc) do ;

    query.free;
    cursor.free;
    doc.free;
    collection.free;
    client.free;

end;

end.
