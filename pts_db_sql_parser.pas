unit pts_db_sql_parser;
 {$ifdef fpc}
   {$mode delphi}
{$endif}
{$h+}
interface
uses classes,sysutils,ux_ansi_classes;

type

  TUX_Field_item = class
    field_name:ansiString;
    raw_str:ansiString;
  end;


  TUX_SQL_Parser = class
    private
        raw_sql:ansiString;
        Ret:record
              error_code,
              cmd:integer; // insert, update, select
              table_name,
              fields:ansiString;
              where:ansiString;

              where_list,
              field_list:TAnsiStringList;
            end;
      function do_parse():integer;

      function do_parse_fields(const str:ansiString):integer;
      function do_parse_where(const str:ansiString):integer;

      procedure Settable_name(const Value: ansistring);

    public
      property    table_name:ansistring read ret.table_name write Settable_name;
      property    Fields:TansiStringList read ret.field_list;
      constructor create;
      destructor  destroy; override;

   class function parse(const sql:ansiString):TUX_SQL_Parser;
  end;




implementation
uses ex_funcs;

const max_int = $7fffff;

{ TUX_SQL_Parser }

constructor TUX_SQL_Parser.create;
begin
  inherited create;
  self.raw_sql := '';
  fillchar(ret,sizeof(ret),0);
end;

destructor TUX_SQL_Parser.destroy;
begin

  inherited;
end;

function TUX_SQL_Parser.do_parse: integer;

   function cmd_text_to_code(const cmd_text:ansiString):integer;
   begin
     Result := 0;

     if sameText(cmd_text,'select') then Result := 1
     else  if sameText(cmd_text,'update') then Result := 2
     else  if sameText(cmd_text,'delete') then Result := 3
     else  if sameText(cmd_text,'insert') then Result := 4
     else  if sameText(cmd_text,'call') then Result := 5
     else  if sameText(cmd_text,'alter') then Result := 11

   end;







var s1,s2:ansistring;
    state:integer;
    p:TParser;
begin
  s1 := trim(raw_sql);
  state := 1;

  while s1<>'' do
   begin
     case state of
       1: begin
             s2 := fsc(s1,' ');
             s1 := trim(s1);
             self.ret.cmd := cmd_text_to_code(s2);
             state := 2;
          end;
       2: begin
            case self.ret.cmd of
              1: begin

                 end;
            end;


          end;
     end;



   end;



end;

function TUX_SQL_Parser.do_parse_fields(const str: ansiString): integer;

   function add_f_item(const str:ansiString; p1,p2:integer):ansiString;
   var    item:TUX_Field_item;
        s2:ansiString;
   begin

      s2 := copy(str,p1,p2-p1+1);
      s2 := trim(s2);
      if s2='' then exit;
      
      item := TUX_Field_item.Create;
      item.field_name := s2;
      item.raw_str := s2;

      self.Ret.field_list.AddObject(item.field_name,item);

      Result := s2;
   end;


var s1,s2:ansiString;
    lastpos, pos,k1,k2,len:integer;
    item:TUX_Field_item;
begin

   s1 := trim(str);
   pos := 1;
   len := length(s1);
   lastpos := 1;
   self.ret.field_list.Clear(true);

   while pos<=len  do
    begin
      case s1[pos] of
        ',': begin
               add_f_item(s1,lastpos,pos);
               pos := pos + 1;
               lastpos := pos;
             end;
        else pos := pos +1;
      end;
    end;

    if lastpos<pos then
    begin
      add_f_item(s1,lastpos,pos);
    end;

  Result := self.ret.field_list.count;
end;

function TUX_SQL_Parser.do_parse_where(const str: ansiString): integer;
var s1,s2,last_word:ansiString;
    lastpos, pos,k1,k2,len:integer;
    item:TUX_Field_item;

begin

   s1 := trim(str);
   pos := 1;
   len := length(s1);
   lastpos := 1;
   self.ret.where_list.Clear();

   while s1<>''  do
    begin
      k1 := Strpos('or',s1);
      k2 := Strpos('and',s1);
      if k1=0 then k1:= MAX_INT;
      if k2=0 then k2:= MAX_INT;

      if k1<k2 then s2 := fsc(s1,'or');
      if k2<k1 then s2 := fsc(s1,'and');


    end;

    if lastpos<pos then
    begin

    end;

  Result := self.ret.field_list.count;
end;

class function TUX_SQL_Parser.parse(const sql: ansiString): TUX_SQL_Parser;
begin
  Result := TUX_SQL_Parser.create;
  Result.raw_sql := sql;
  Result.do_parse;
end;


procedure TUX_SQL_Parser.Settable_name(const Value: ansistring);
begin
  ret.table_name := Value;
end;

end.
