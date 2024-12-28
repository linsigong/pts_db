unit pts_db_main;
{$ifdef fpc}
   {$mode delphi}
{$endif}
{$h+}

(*


  基于 PTS 的数据库存储引擎。 下一步整合到 dao 中。

  性能极大的受存储的影响，最好使用 SSD 或者内存虚拟盘。


  core.init_database('d:\lins\apps\db\x\'); // data_path =path

  core.largeMode := true; // false is default.

  core.select('time,data','tbl_x_ff2','time>2020-23-22 22:22:22 and data.xt>20');  // select(fields, from , where);

  core.create_table('tbl_x_filex');  // table = files

  core.update('tbl_tab_1','data','time=xxxxx or time=xxxx');  // table, field, where

  core.insert('tbl_tab',time,data);

  core.delete_from('tbl_tab','where time>xxxxx and data.x=xxx'):

  core.delete_table(table_name);

  core.get('time_key',data); // ret=boolean;
  core.put('time_key',data); // ret=boolean
  core.remove('time_key');

  core.close_database('d:\xxx\xxx'); // path.

  core.copyTableFrom(src_database);


  // select 支持一下选择器

  time,data
  time,data.xxx,data.*
  data
  data.xxx as xxxx, data.bbb as xxxxxx,


  // 性能指标

   1. select 批量读取， 12.5W/s  , over SSD.
   2. insert 写入  7.7W/s   , over SSD


  升级记录：

   1. [2021-8-10]  select 里面的 pts_start , 增加支持时间段写法 between(xxxx-xx-xx xx:xx:xx, xxxx-xx-xx xx:xx:xx);

   2. 索引字段增加到  8+8 16 字节。


*)

interface
uses classes,sysutils, ux_file_io, pts_db_store_engine, pts_db_common, ux_hugedata_index,
     ux_ansi_classes ;


type

  TPTS_DB_Main_Core = class(TInterfacedObject)
    private
        db_files:TPTS_DB_Store_File_list;

        kv_db: TUX_Huge_HashMap_File;
        save_db_path:ansiString;

        FActive: Boolean;
        rt:record
             last_error_code:integer;
             last_Error_str:ansiString;
           end;
        FlargeMode: Boolean;
        f_table_List:TAnsiStringList;

      procedure SetActive(const Value: Boolean);
      procedure update_error(code:integer=0; const str:ansiString='');
      function  get_full_fname(const tab_name:ansiString):ansiString;
      procedure SetlargeMode(const Value: Boolean);
    public
      function table_create(const tab_name:ansiString):boolean;
      function table_drop(const tab_name:ansiString):boolean;
      function table_exists(const tab_name:ansiString):Boolean;
      function  table_attr(const tab_name:AnsiString):AnsiString; overload;
      procedure table_attr(const tab_name, attr:AnsiString); overload;
      function  table_summary(const tab_name:AnsiString):AnsiString;


      // 关闭之前，可以做 sort 操作。
      function close_table(const tab_name:ansiString):Boolean;

      //写入的最后阶段，重新拍一下索引，便于更快的查询。
      function Sort_table(const table_name:ansiString):TPTS_DB_Main_Core;

      // return json arrays.
      function select(const fields, tab_name, pts_start:ansiString; where_func:TPTS_DB_Where_func=nil; limit:integer=0):AnsiString; overload;
      function select(const fields, tab_name:ansiString; pts_start,pts_end:int64; where_func:TPTS_DB_Where_func=nil; limit:integer=0):AnsiString; overload;

   function select(const SQL:ansiString):AnsiString; overload;  // not yet

      function select(const tab_name:ansiString; func:TPTS_DB_Where_func; limit:integer=0):AnsiString; overload; // not yet

      function update(const tab_name, pts_start,  data:ansiString; where_func:TPTS_DB_Where_func=nil; limit:integer=0 ):integer; overload;

      function insert(const tab_name, time, data:ansiString; key1, key2:cardinal):Boolean; overload;
      function insert(const tab_name:ansiString; pts:int64; key1, key2:cardinal; data:pbyte; datalen:integer):Boolean; overload;

      function delete_from(const tab_name, pts_start:ansiString;  where_func:TPTS_DB_Where_func=nil; limit:integer=0 ):integer;

      function each(const tab_name:ansiString; pts_start:int64; userData:pointer; func_each:TUX_pts_db_each_func):integer;

      // kv-function
      function get(const table_name, time_key:ansiString; var data:ansiString):Boolean;
      function put(const table_name, time_key, data:ansiString):boolean;
      function remove(const table_name, time_key:ansiString):Boolean;

      // if tab_name does not exists, return nil;
      function getStreamReader(const tab_name:ansiString; pts_start,pts_end:int64):IStreamReader;

      property  Active:Boolean read FActive write SetActive;
      property  ErrorString:ansiString read  rt.last_Error_str;
      property  ErrorCode:integer read  rt.last_error_code;
      property  largeMode:Boolean read FlargeMode write SetlargeMode;

      constructor  create(const db_path:ansiString);
      destructor   destroy; override;

      function close_database():boolean;
      function copyTableFrom(const tab_name:ansiString; src_db:TPTS_DB_Main_Core):boolean;
      function TableList:TAnsiStringList;

   class Function init_database(const db_path:ansiString):TPTS_DB_Main_Core;
   class function get_Version_Text():ansiString;
   class function get_version:cardinal;
  end;


procedure  unit_test();

implementation
uses math, ex_funcs, ext_mem, dateutils, ux_encoder_base64, ux_encoder_utf8;

const

   cnt_version = $01001001;
   cnt_version_text = 'v1.0 (Build 1001)';


function _UnixTimeMsec2(T:TDateTime=-1):int64;
var k:integer;
begin
  k := RTL.timezone_offset;

  if t<0 then
    Result := MilliSecondsBetween(Now(),EncodeDateTime(1970,1,1,K,0,0,0))
  else Result := MilliSecondsBetween(t,EncodeDateTime(1970,1,1,K,0,0,0));

end;


procedure  unit_test2();
var i:integer;
    T1,T2:int64;
    Tx:TDatetime;
begin
    T1 := UnixTimeMsec2();
    Writeln(t1);

    T1 := UnixTimeMsec();
    Writeln(t1);

    Writeln(DateTimeToStr(UnixTimeMsecToDatetime(T1)));
    Writeln(DateTimeToStr(now));

    for i := 0 to 1000000 do
      begin
         //T2 := UnixTimeMsec();
         Tx := UnixTimeMsecToDatetime(T1);
         //T2 := getTickCount;
         //writeln(T2);
      end;

    T1 := UnixTimeMsec()-T1;
    Writeln('time span=',T1,'ms');
    readln;


end;

procedure  unit_test();
var db:TPTS_DB_Main_Core;
    s1,path:ansiString;
    T1,T2:int64;
    K:integer;
begin
  path := 'e:\temp\pts_db\';
  db := TPTS_DB_Main_Core.init_database(path);
  db.table_create('tbl_test');
  db.insert('tbl_test','now()','{"a":1,"b":2}',0,0);
  T1 := UnixTimeMsec();


  (*
  for K := 0 to 100000 do
  begin
    db.insert('tbl_test','now()',format('{"a":1,"b":2,"k":%d}',[k]),0,k);
  end;
  *)

  T1 := UnixTimeMsec()-T1;
  Writeln('time span=',T1,'ms');

 // db.delete_from('tbl_test','',nil,100);


  T1 := UnixTimeMsec();
  s1 := db.select('time, data.a as X1, data.b, to_base64(data) as X2','tbl_test','2021-06-04 16:22:15', nil,1000);
                                                                             //  'between(xx-xx-xxx,xx-xx-xx)';

  T1 := UnixTimeMsec()-T1;
  Writeln('time span=',T1,'ms, len=',length(s1));

//  db.drop_table('tbl_test');

  writeln(db.TableList.Text);

  writeln(s1);
  db.free;

  readln;
end;


type

 TUX_Stream_reader = class(TInterfacedObject, IStreamReader)
   private
      RT:record
           uuid:ansiString;
           RecNo:cardinal;
           pts_start,
           pts_end : int64;
           last_get_pts: int64;
         end;
      fh:TPTS_DB_Store_File;
      F_UserData:pointer;
    function  getData(PX:PPTS_DB_Item):integer;
    function  getUUID:AnsiString;
    function  getUserData():pointer;
    procedure setUserData(data:pointer);
    function getPTS:int64;
    procedure setPTS(Time:int64);
   public
    property UserData:pointer read getUserData write setUserData;
    property CurrentPTS:int64 read getPTS write setPTS;
    constructor  Create;
    destructor   destroy; override;
 end;



{ TPTS_DB_Main_Core }

function TPTS_DB_Main_Core.close_database: boolean;
begin
  self.Active := false;
end;

function TPTS_DB_Main_Core.close_table(const tab_name: ansiString): Boolean;
begin
  Result := db_files.table_close(tab_name);
end;

function TPTS_DB_Main_Core.copyTableFrom(const tab_name: ansiString; src_db: TPTS_DB_Main_Core): boolean;
var dest_fname,src_fname:ansiString;
begin
   dest_fname := self.get_full_fname(tab_name);
   src_fname := src_db.get_full_fname(tab_name);

   if src_db.table_exists(tab_name) =false then
    begin
      Result := false;
      exit;
    end;

   TShareFile.CopyShareFile(src_fname,dest_fname);
   TShareFile.CopyShareFile(src_fname+'.blob',dest_fname+'.blob');
   TShareFile.CopyShareFile(TShareFile.change_file_ext(src_fname,'.FDX'),TShareFile.change_file_ext(dest_fname,'.FDX'));

   Result := FileExists(dest_fname);
end;

constructor TPTS_DB_Main_Core.create(const db_path: ansiString);
begin
  inherited create;
  self.save_db_path := db_path;
  self.FActive := false;
  self.FlargeMode := false;
  self.f_table_List :=nil;
end;

procedure TPTS_DB_Main_Core.table_attr(const tab_name, attr: AnsiString);
var fh:TPTS_DB_Store_File;
begin
  fh  := self.db_files.find_table_RW(tab_name,false);
  if fh<>nil then
   begin
     fh.attr(attr);
   end;
end;

function TPTS_DB_Main_Core.table_attr(const tab_name: AnsiString): AnsiString;
var fh:TPTS_DB_Store_File;
begin
  fh  := self.db_files.find_table_RW(tab_name,false);
  if fh<>nil then
   begin
     Result := fh.attr;
   end;

end;

function TPTS_DB_Main_Core.table_create(const tab_name: ansiString): boolean;
var fh:TPTS_DB_Store_File;
begin
   fh := db_files.find_table_RW(tab_name,true);

   Result := fh<>nil;

   db_files.recycle_rw(fh);

end;

function TPTS_DB_Main_Core.delete_from(const tab_name, pts_start: ansiString; where_func:TPTS_DB_Where_func; limit:integer): integer;
var fh:TPTS_DB_Store_File;
    X1:int64;
    ret:integer;
    pkt:TDB_Data_packet;
    count,rec_no:integer;
    found, abort:boolean;
    item:TPTS_DB_Item;
    buf:array[0..65536*4-1] of byte;
begin
   Result := 0;
   fh := db_files.find_table_RW(tab_name,false);
   if fh=nil then exit;

   if pts_start='' then X1 := 0
      else X1 := unixTimeMsec2(pts_start);

   fillchar(pkt,sizeof(pkt),0);

   if assigned(where_func) then pkt.Data := @buf[0] else pkt.Data := nil;

   fh.lockEnter;
   try
     if (x1=0) and (not assigned(@where_func)) and (limit=0) then
      begin
        fh.DeleteAllItem;
        exit;
      end;

     fh.Seek_PTS_Min(x1);
     count := 0;
     rec_no := 0;


     while true do
      begin


         ret := fh.Get(@pkt);
         if ret=0 then continue
         else if ret<0 then break
         else begin

           rec_no := rec_no +1;
           found := true;
           abort := false;
           if assigned(where_func) then
            begin
              item.RecNo :=  rec_no;
              item.time := pkt.PTS_IDX;
              item.key1 := pkt.key1;
              item.key2 := pkt.key2;
              item.data := pkt.data;
              item.data_len := pkt.Data_len;
              found := where_func(tab_name,@item,abort);
            end;

           if found then
            begin

               fh.DeletePkt(pkt.abs_packet_file_pos);

              inc(count);
            end;

            if abort then break;

            if (limit>0) and (count>=limit)  then break;

         end;

      end;

   finally
     db_files.recycle_rw(fh);
     fh.lockExit;
   end;

end;

destructor TPTS_DB_Main_Core.destroy;
begin
  self.Active := false;
  inherited;
end;

function TPTS_DB_Main_Core.table_drop(const tab_name: ansiString): boolean;
begin
  self.db_files.delete(tab_name);
  Result := FileExists(self.get_full_fname(tab_name))=false;
end;

function TPTS_DB_Main_Core.each(const tab_name: ansiString; pts_start:int64; userData:pointer; func_each: TUX_pts_db_each_func): integer;
var ret,got_count,rec_no:integer;
    pkt:TDB_Data_packet;
    fh:TPTS_DB_Store_File;
    fname:ansiString;
    buf2:array[0..65536*4-1] of byte;
    found,abort:boolean;
    item:TPTS_DB_Item;
begin
  if assigned(func_each)=false then exit;

  fname := self.get_full_fname(tab_name);
  if not FileExists(fname) then
   begin
     Result := -1;
     self.update_error(-101,'table: '+tab_name+' does not exists.');
     exit;
   end;

  update_error();

  fh  := self.db_files.find_table_RO(tab_name,true);
  got_count := 0;
  rec_no := 0;

  pkt.Data_len := 0;
  pkt.data := @buf2[128];

  fh.lockEnter;
  try

   fh.Seek_PTS_Min(pts_start);

   while true  do
   begin
     ret := fh.Get(@pkt,-1);
     if ret>0 then
      begin
         rec_no := rec_no +1;
         found := true;
         abort := false;
         if assigned(func_each) then
          begin
            item.RecNo :=  rec_no;
            item.time := pkt.PTS_IDX;
            item.key1 := pkt.key1;
            item.key2 := pkt.key2;
            item.data := pkt.data;
            item.data_len := pkt.Data_len;
            found := func_each(tab_name,userData, @item, abort);
            if found then got_count := got_count +1;
          end;

          if abort then break;

      end else
      if ret=0 then continue
      else break;

   end;

  finally
    self.db_files.recycle_ro(fh);
    fh.lockExit;
  end;


  Result := got_count;
end;

function TPTS_DB_Main_Core.table_exists(const tab_name: ansiString): Boolean;
begin
  Result := db_files.table_exists(tab_name);
end;

function TPTS_DB_Main_Core.table_summary(const tab_name: AnsiString): AnsiString;
var fh:TPTS_DB_Store_File;
begin

  fh := self.db_files.find_table_RO(tab_name,true);
  if fh<>nil then
   begin
     Result := fh.summary();
     json_set(Result,'table','tab_name');
     //Result := json_object(['table',tab_name,'pts_start',fh.first_pts_int,'pts_end',fh.last_pts,
     //                       'size',fh.Size,'state',1,'count',fh.record_count]);
     db_files.recycle_ro(fh);
   end else begin
     Result := json_object(['table',tab_name,'state',-1]);
   end;

end;

function TPTS_DB_Main_Core.get(const table_name, time_key: ansiString;
  var data: ansiString): Boolean;
var key:ansiString;
begin
  key := format('kv.%s.%s',[table_name,time_key]);
  Result := self.kv_db.Get(key,data);
end;

function TPTS_DB_Main_Core.getStreamReader(const tab_name:ansiString; pts_start,pts_end:int64): IStreamReader;
var R:TUX_Stream_reader;
    fhx: TPTS_DB_Store_File;
    ret:boolean;
begin
   fhx := self.db_files.find_table_RO(tab_name,true);
   if fhx=nil then
    begin
      Result := nil;
      exit;
    end;

   R := TUX_Stream_reader.Create;
   R.fh := fhx;
   R.RT.pts_start := pts_start;
   R.RT.pts_end := pts_end;

   if pts_start=cnt_pts_live_stream then
   begin
      ret := true;
      R.fh.Seek_PTS_end();
   end else ret := R.fh.Seek_PTS_Min(pts_start);

  if ret then Result := R
   else begin
     Result := nil;
     R.free;
   end;

end;

function TPTS_DB_Main_Core.get_full_fname(const tab_name: ansiString): ansiString;
var s1,s2:ansiString;
begin
  s1 := lowercase(trim(tab_name));
  s1 := replace(s1,'/','_');
  s1 := replace(s1,'\','_');

  if FlargeMode then
  begin
    s2 := md5Encode(s1);
    Result := format('%s\%s\%s\',[self.save_db_path,copy(s2,1,2),copy(s2,3,2)]);
  end else begin
    Result :=  self.save_db_path+'\';
  end;


  if PathDelim='/' then Result := replace(Result,'\','/');

  if DirectoryExists(Result)=false then MakeDir(Result);

  Result := Result + s1 +'.db';

  if PathDelim='/' then
    Result := Replace(Result,'//','/')
  else
   Result := Replace(Result,'\\','\');  


end;

class function TPTS_DB_Main_Core.get_version: cardinal;
begin
  Result := cnt_version;
end;

class function TPTS_DB_Main_Core.get_Version_Text: ansiString;
begin
  Result := cnt_version_text;
end;

class function TPTS_DB_Main_Core.init_database( const db_path: ansiString): TPTS_DB_Main_Core;
begin
  Result := TPTS_DB_Main_Core.create(db_path);
  Result.Active := true;
end;


function TPTS_DB_Main_Core.insert(const tab_name: ansiString; pts:int64;
                                        key1, key2: cardinal; data: pbyte; datalen: integer): Boolean;
var fh:TPTS_DB_Store_File;
    P:TDB_Data_packet;
    k1:integer;
    t1_now:Tdatetime;
    t1:int64;
    s1,s2:ansiString;
begin
  Result := false;

  if pts<=0 then T1 := unixTimeMsec() else T1 := pts;

  fh := db_files.find_table_RW(tab_name,true);
  if fh<>nil then
   begin
     update_error();
     fillchar(p,sizeof(p),0);

     p.PTS_IDX := T1;
     p.key1 := key1;
     p.key2 := key2;

     p.Data := data;
     p.Data_len :=ifthen(data=nil,0, datalen);

     fh.lockEnter;
     try
      Result := fh.Put(@p,-1)>0;
     finally fh.lockExit; end;

   end else begin
     update_error(-103,'open table:'+tab_name+' fail.');
   end;

end;


// 7.7W/s 插入 ， 位于 SSD 上。

// data 必须为 json 格式的数据。

function TPTS_DB_Main_Core.insert(const tab_name, time, data: ansiString; key1, key2:cardinal): Boolean;
var fh:TPTS_DB_Store_File;
    P:TDB_Data_packet;
    k1:integer;
    t1_now:Tdatetime;
    t1:int64;
    s1,s2:ansiString;
begin
  Result := false;

  s1 := trim(time);
  s1 := replace(s1,' ','');

  if sameText(s1,'now','now()','') or sameText(s1,'timestamp','now[]')  then
   begin
     t1_now := now();
     T1 := UnixTimeMsec(t1_now);
   end else begin
     if not TrystrToDatetime(time,t1_now) then
     begin
       self.update_error(-102,'time was invalid.');
       exit;
     end;

     T1 := UnixTimeMsec(t1_now);
   end;


  fh := db_files.find_table_RW(tab_name,true);
  if fh<>nil then
   begin
     update_error();
     fillchar(p,sizeof(p),0);

     p.PTS_IDX := T1;
     p.key1 := key1;
     p.key2 := key2;

     if data='' then
     begin
       s2 := '""';
       p.Data := @s2[1];
       p.Data_len := 2;
     end else begin
       p.Data := @data[1];
       p.Data_len := length(data);
     end;

     fh.lockEnter;
     try
       Result := fh.Put(@p)>0;
     finally fh.lockExit; end;

   end else begin
     update_error(-103,'open table:'+tab_name+' fail.');
   end;

end;

function TPTS_DB_Main_Core.put(const table_name, time_key,
  data: ansiString): boolean;
var key:ansiString;
begin
  key := format('kv.%s.%s',[table_name,time_key]);
  Result := true;
  self.kv_db.Put(key,data);
end;


function TPTS_DB_Main_Core.remove(const table_name, time_key: ansiString): Boolean;
var key:ansiString;
begin
  key := format('kv.%s.%s',[table_name,time_key]);
  kv_db.Remove(key);
end;

// pts_start = between('xx-xx-xx xx:xx:xx','xx-xx-xx xx:xx:xx')
// pts-start = 'xx-xx-xx xx:xx:xx';
// pts-start = ''
function TPTS_DB_Main_Core.select(const fields, tab_name, pts_start: ansiString;
  where_func: TPTS_DB_Where_func; limit: integer): AnsiString;
var X1,X2:int64;
    s1,s2:AnsiString;
    k1,k2:integer;
begin
  s1 := Trim(pts_start);
  X1 := 0;
  X2 := 0;

  if s1='' then X1 := 0
   else begin
     if  sametextLen('between',s1,7) then
     begin
       s2 := fsc(s1,'(');
       s2 := fsc(s1,',');
       X1 := unixTimeMsec2(s2);
       s2 := fsc(s1,')');
       X2 := unixTimeMsec2(s2);
       if (X2<=0) then x2 := unixTimeMsec();

       if (X1>X2) then
        begin
          SwapInt64(X1,X2);
        end;
        
     end else
     begin
       X1 := unixTimeMsec2(s1);
     end;
   end;

  Result := self.select(fields,tab_name,X1,X2, where_func,limit);
end;

function TPTS_DB_Main_Core.select(const tab_name: ansiString; func: TPTS_DB_Where_func; limit:integer): AnsiString;
begin
   Result := self.select('*',tab_name,0,0,func,limit);
end;

// select time,data.xx,data.xx2 from tbl_tab1 where time>'2021-12-2 20:22:9' and data.x = 222;

function TPTS_DB_Main_Core.select(const SQL: ansiString): AnsiString;
begin

  Result := 'not implement yet.';


end;

procedure fmt_fields(buf:TFifo_mem_stream; list:TAnsiStringList; pts:int64; const json:ansiString);
var i:integer;
    s1,s2:ansiString;
    k1,k2:integer;
begin

   buf.WriteString('{ "timeId":');
   buf.WriteString(intToStr(pts));
   buf.WriteString(', "time":"');
   buf.WriteString(DateTimeToStr(UnixTimeMsecToDatetime(pts)));
   buf.WriteString('"');

   for i := 0 to list.count -1 do
     begin
       s1 := trim(list[i]);

       if sametext(s1, 'data','data.*') then
        begin

          buf.WriteString(', "data":');

          if json='' then buf.WriteString('""')
             else buf.WriteString(json);

        end else
       if pos('data.',s1)=1 then
        begin
          // s2 = key
          // s1 = field name

          delete(s1,1,5);
          if strPos('as',s1)>0 then
          begin
            s2 := trim(fsc(s1,'as'));
            s1 := trim(s1);
          end else begin
            s2 := s1;
          end;


          buf.WriteString(', "').WriteString(s1).WriteString('":"');
          buf.WriteString(json_get(json,s2)).WriteString('"');

        end else
       if pos('(',s1)>1 then
        begin

          if strPos(' as ',s1)>0 then
          begin
            s2 := trim(fsc(s1,' as '));
            s1 := trim(s1);

            if (s2='') or (s1='') then continue;
            
          end else begin
            s2 := s1;
            s1 := replace(s1,'(','_');
            s1 := replace(s1,')','_');
          end;

          if sameText(s2,'to_base64(data)') then  // to_base64(data) as text
           begin
              buf.WriteString(', "').WriteString(s1).WriteString('":"');
              buf.WriteString(base64_Encode(json)).WriteString('"');
           end else
          if sameText(s2,'from_base64(data)') then  // from_base64(data) as btext
           begin
              buf.WriteString(', "').WriteString(s1).WriteString('":"');
              buf.WriteString(base64_Decode(json)).WriteString('"');
           end else
          if sameText(s2,'length(data)') then
           begin
              buf.WriteString(', "').WriteString(s1).WriteString('":');
              buf.WriteString(intToStr(length(json)));
           end else
          if sameText(s2,'md5(data)') then
           begin
              buf.WriteString(', "').WriteString(s1).WriteString('":"');
              buf.WriteString(md5Encode(json)).WriteString('"');
           end else
          if sameText(s2,'sha1(data)') then
           begin
              buf.WriteString(', "').WriteString(s1).WriteString('":"');
              buf.WriteString(sha1Encode(json)).WriteString('"');
           end;

        end;

     end;

   buf.WriteString(' }');

end;


// 批量读取， 12.5W/s  , over SSD.

function TPTS_DB_Main_Core.select(const fields, tab_name:ansiString; pts_start, pts_end:int64; where_func:TPTS_DB_Where_func; limit:integer): AnsiString;
var ret,got_count,rec_no:integer;
    pkt:TDB_Data_packet;
    doNext:Boolean;
    fh:TPTS_DB_Store_File;
    fname:ansiString;
    buf:TFifo_mem_stream;
    buf2:array[0..65536*4-1] of byte;
    found,abort:boolean;
    item:TPTS_DB_Item;
    list:TAnsiStringlist;
begin

  fname := self.get_full_fname(tab_name);
  if not FileExists(fname) then
   begin
     Result := '';
     self.update_error(-101,'table: '+tab_name+' does not exists.');
     exit;
   end;

  update_error();

  fh  := self.db_files.find_table_RO(tab_name,true);
  buf:=TFifo_mem_stream.Create();
  buf.WriteString('[');
  got_count := 0;
  rec_no := 0;

  pkt.Data_len := 0;
  pkt.data := @buf2[128];

  list := TAnsiStringList.Split(fields,',');

  fh.lockEnter;
  try

   fh.Seek_PTS_Min(pts_start);

   while true  do
   begin
     ret := fh.Get(@pkt,-1);
     if ret>0 then
      begin
         rec_no := rec_no +1;
         found := true;
         abort := false;

         if (pts_end>0) and (pts_end<pkt.PTS_IDX) then Break; // end of pts_end

         if assigned(where_func) then
          begin
            item.RecNo :=  rec_no;
            item.time := pkt.PTS_IDX;
            item.key1 := pkt.key1;
            item.key2 := pkt.key2;
            item.data := pkt.data;
            item.data_len := pkt.Data_len;
            found := where_func(tab_name,@item,abort);
          end;

          if found then
          begin
              if got_count>0 then buf.WriteString(', ');

              if sameText(trim(fields),'*','','data') or (list.count=0) then
              begin
                buf.WriteString('{ "timeId":');
                buf.WriteString(intToStr(pkt.PTS_IDX));
                buf.WriteString(', "time":"');
                buf.WriteString(DateTimeToStr(UnixTimeMsecToDatetime(pkt.PTS_IDX)));
                buf.WriteString('", "data":');

                if pkt.Data_len<=0 then buf.WriteString('""')
                  else buf.Write(pkt.Data^,pkt.Data_len);

                buf.WriteString(' }');
              end else begin
                fmt_fields(buf,list,pkt.PTS_IDX,dataToString(pkt.Data,pkt.Data_len));
              end;

              got_count := got_count+1;
          end;

          if abort then break;
          if (limit>0) and (got_count>=limit) then break;


      end else
      if ret=0 then continue
      else break;
   end;

  finally
    self.db_files.recycle_ro(fh);
    fh.lockExit;
  end;

  buf.WriteString(']');

  Result := buf.asString();

  freeAndnil(buf);
  freeAndNil(list);

end;

procedure TPTS_DB_Main_Core.SetActive(const Value: Boolean);
begin
  if self.FActive = value then exit;
  FActive := Value;
  if value then
   begin
    self.kv_db := TUX_Huge_HashMap_File.Create(self.save_db_path+pathDelim+'kv.db',true,true);

    self.db_files := TPTS_DB_Store_File_list.Create(self.save_db_path);
    self.db_files.on_tabName_to_fileName := self.get_full_fname;

   end else begin

    freeAndnil(self.db_files);
    freeAndNil(self.kv_db);
    freeAndnil(self.f_table_List);

   end;

end;

procedure TPTS_DB_Main_Core.SetlargeMode(const Value: Boolean);
begin
  FlargeMode := Value;
end;

function TPTS_DB_Main_Core.Sort_table(const table_name: ansiString): TPTS_DB_Main_Core;
var fh:TPTS_DB_Store_File;
begin
   Result := self;
   fh := db_files.find_table_RW(table_name,false);
   if fh=nil then exit;
   fh.ReBuildFileIndex();
end;

function TPTS_DB_Main_Core.TableList: TAnsiStringList;
var i:integer;
    ls:TStringList;
    s1,s2:ansiString;
begin
  if self.f_table_List=nil then  self.f_table_List := TAnsiStringList.Create;

  ls := Dir_path_to_list(self.save_db_path+'*.db');
  f_table_List.Clear();

  for i := 0 to ls.Count -1 do
  begin
     s1 := trim(ls[i]);
     s2 := Left(s1,length(s1)-3);
     if s2<>'' then f_table_List.Add(s2);
  end;

  Result := f_table_List;
end;

function TPTS_DB_Main_Core.update(const tab_name, pts_start,  data:ansiString; where_func:TPTS_DB_Where_func; limit:integer): integer;
var fh:TPTS_DB_Store_File;
    X1:int64;
    ret:integer;
    pkt:TDB_Data_packet;
    count,rec_no:integer;
    found, abort:boolean;
    item:TPTS_DB_Item;
    buf:array[0..65536*4-1] of byte;
begin
   Result := 0;
   fh := db_files.find_table_RW(tab_name,false);
   if fh=nil then exit;

   if pts_start='' then X1 := 0
      else X1 := unixTimeMsec2(pts_start);

   fillchar(pkt,sizeof(pkt),0);

   fh.lockEnter;
   try

     fh.Seek_PTS_Min(x1);
     count := 0;
     rec_no := 0;


     while true do
      begin
         if assigned(where_func) then pkt.Data := @buf[0] else pkt.Data := nil;

         ret := fh.Get(@pkt);
         if ret=0 then continue
         else if ret<0 then break
         else begin

           rec_no := rec_no +1;
           found := true;
           abort := false;
           if assigned(where_func) then
            begin
              item.RecNo :=  rec_no;
              item.time := pkt.PTS_IDX;
              item.key1 := pkt.key1;
              item.key2 := pkt.key2;
              item.data := pkt.data;
              item.data_len := pkt.Data_len;
              found := where_func(tab_name,@item,abort);

            end;

           if found then
            begin
              inc(count);

              pkt.Data := @data[1];
              pkt.Data_len := length(data);

              fh.Put(@pkt,pkt.abs_packet_file_pos);

            end;

            if abort then break;

            if (limit>0) and (count>=limit)  then break;

         end;

      end;

   finally
     db_files.recycle_rw(fh);
     fh.lockExit;
   end;

end;


procedure TPTS_DB_Main_Core.update_error(code: integer; const str: ansiString);
begin
  rt.last_error_code := code;
  rt.last_Error_str := str;
end;

{ TUX_Stream_reader }

constructor TUX_Stream_reader.Create;
begin
  inherited create;
  fh := nil;
  fillchar(rt,sizeof(rt),0);
  rt.uuid := 'SR'+getRcode(20);
end;

destructor TUX_Stream_reader.destroy;
begin
  freeandnil(fh);
  inherited;
end;

function TUX_Stream_reader.getData(PX: PPTS_DB_Item): integer;
var i:integer;
    pkt:TDB_Data_Packet;
    ret:integer;
begin
  Result := -1;
  if fh<>nil then
    begin
       if (rt.pts_end>0) and (rt.last_get_pts>rt.pts_end) then    // end of file.
        begin
          Result := -2;
          exit;
        end;

       fillchar(pkt,sizeof(pkt),0);
       pkt.Data := px.data;

       while True do
       begin
        ret := fh.Get(@pkt);
        if ret>0 then
        begin
          rt.RecNo := rt.RecNo +1;

          px.RecNo := rt.RecNo;
          px.time := pkt.PTS_IDX;
          px.key1 := pkt.key1;
          px.key2 := pkt.key2;
          px.data_len := pkt.Data_len;

          Result := pkt.Data_len;

          rt.last_get_pts := pkt.PTS_IDX;

          break;
        end else
        if ret=0 then continue
        else break;
       end;


       if (Result<0) and ( (fh=nil) or (fh.isEndOfRead) ) then
       begin
         Result := -2;
         exit;
       end;

    end;

end;

function TUX_Stream_reader.getPTS: int64;
begin
  if fh<>nil then Result := fh.last_pts else Result := 0;
end;

function TUX_Stream_reader.getUserData: pointer;
begin
  Result := F_UserData;
end;

function TUX_Stream_reader.getUUID: AnsiString;
begin
 Result := RT.uuid;
end;

procedure TUX_Stream_reader.setPTS(Time: int64);
begin
  if fh<>nil then fh.Seek_PTS_Min(time);
end;

procedure TUX_Stream_reader.setUserData(data: pointer);
begin
  F_UserData := data;
end;

end.
