unit pts_db_store_engine;
{$ifdef fpc}
   {$mode delphi}
{$endif}
{$h+}


(*

  从 PTS_DB_File 集成过来，做了一定程度的改动， 支持基于 PTS 的数据库存储引擎。

  1. 每个块最少存储 32 个字节数据， 支持修改。 修改内容超出原来空间，改为
     存储到同名数据库文件的单独 blob 文件中。

  2. 索引文件不变。

  3. 支持重建文件的时候，去掉 blob。 blob 如果丢失，可能损失部分记录。原来的记录还在。

  4. 捆绑了 KV-db ， 用于存储快速检索的数据。 缓存等。

  5. 支持[千万级别]张表。  一个对象一个表。 比如一个设备， 一个 session , 通过表名来区分。
     存储的时候，采用分级存储。 分成 256x256 级别。

  6. 每块数据， 最大 256K。

*)


interface
uses classes,sysutils, pts_db_common,ux_file_io,ux_ansi_classes;

type

   (*
      文件格式，  8192 个字节头部 +  BLOCK_DATA

      [FILE_HEADER_FLAG]

      [FLAG:32bit] [Last_Block_Pos:64bit] [last_block_pts:64bit]

      // FLAG: 32it, pos = 0
      // Last_block_pos:64bit , pos = 4
      // last_block_pts:64bit , pos = 12
      // first_block_pts:64bit, pos = 20



      [Meta_data: pos=256, n-bytes] 最大 8192-256 bytes.
      // position = 256


      数据载体 BLOCK_DATA:

      [FLAG: 32bit] [PTS_IDX:64bit]  [32+32]     [DATALEN: 32bit ]    [DATA ... N bytes]    [Last_Block_Step_Len: 32bit ]
      标志位          时间戳索引     key1,key2    本块数据长度        数据体，多个字节       上一个 Block_data 相对距离

   *)

   PDB_Data_packet = ^TDB_Data_packet;
   TDB_Data_packet = packed record     // Map to BLOCK_DATA
     Block_Flag: Cardinal;   // normal or deleted.
     PTS_IDX   : Int64;
     key1,key2 : cardinal;
     Data_len  : Cardinal;
     data_space: cardinal;
     Record_NO : uInt64; // 包顺序号 +=1
     _blank    : array[0..7] of byte; //  空白预留。
    /////////////////////////////////////////////////
     Data      : PByte;      // sizeof(pointer)
     abs_packet_file_pos:int64; // 4  ， 最后一个 block_node 相对位置
   end;

   TMX_Pos_mode = (pm_none=0, pm_start, pm_last_pos,  pm_abs_pos);


   PUXI_KV_Item =^TUXI_KV_Item;
   TUXI_KV_Item = packed record  //12-byte  // 4G /1000 = 4M s =  46 days = 1111 hour
                    Key_low:Cardinal;  // now_ptr - self.first_pts_int;
                    key_hi:word;
                    V_low:cardinal;       // pos of file. max file size=120T=65536*2G
                    V_hi:word;
                 end;
   TUXI_KV_Items = array[0..32767] of TUXI_KV_Item;
   PUXI_KV_Items = ^TUXI_KV_Items;


   TUX_Indexed_File = class
     private
         item_len:integer;
         bufs : array of TUXI_KV_Item;
         FH   : TShareFile;
         r_last_size:int64;
         f_Mode:AnsiString;
        procedure  Reload_From_File;
        procedure  internal_quick_sort();
        procedure  QuickSort(L, R: Integer);
        function   func_compare(Index1, Index2: Integer): int64;
     public
         FileName:AnsiString;
         params:record
                  P1,P2:NativeInt;
                  p3,p4:Int64;
                end;
       function     Append(key, V:Int64):TUX_Indexed_File;
       function     FindKey(Key:int64; const findmethod:AnsiString='='):integer;
       function     Items(idx:Integer):PUXI_KV_Item;
       constructor  Create(const fname,Mode:AnsiString);
       destructor   Destroy; override;
       procedure    reBuild();
   end;

   TPTS_DB_Store_blob_File = Class
     private
        fh:TShareFile;
     public
      procedure    lock;
      procedure    unlock;
      constructor  OpenRead(const fname: ansistring);  virtual;
      constructor  OpenWrite(const fname: ansistring); Virtual;
      destructor   Destroy; override;

      function  GetData(pts_idx, pos:int64; data:pbyte; datalen:integer):integer;
      function  SaveData(pts_idx, block_pos, abs_file_pos:int64; data:pbyte; datalen:integer):int64;
   end;


   // 带时间序列标志的文件
   TPTS_DB_Store_File = Class(TShareFile)   // 增加索引文件
    private
       Read_Mode:record
                    active:boolean;
                    Pos_mode:TMX_Pos_mode;
                    last_block_pos,
                    last_pts : int64;
                    last_Record_NO:UInt64;
                 end;
       Write_Mode: record
                    active:Boolean;
                    last_block_pos,
                    last_pts : int64;
                    last_Record_NO:UInt64;
                   end;
       idx_fh:TUX_Indexed_File;
       meta_data:AnsiString;
       f_attr:AnsiString;
       attr_rec:record
                  expire_days:integer; // 多少天后, 数据自动清理掉.  >=0 不清理.

                end;

       function read_blob_data_from(pts, pos:int64; data:pbyte; datalen:integer):integer;
       function Write_blob_data_to(pts, link_block_pos, abs_file_pos:int64; data:pbyte; datalen:integer):int64;
       function packet_header_len:integer;
       procedure _init_attr(const str:AnsiString);
       function read_last_pts(var _last_pts, _last_blk_pos, _last_rec_no:UInt64):Boolean;
    public
       userData:record
                  str:ansiString;
                end;
      // last_pts_int,
       first_pts_int:int64; // 第一个 PTS值
       on_rec_item_changed: procedure (const tab_name:ansiString; action:integer;  pkt:PDB_Data_packet) of object;
      function     Get(pkt:PDB_Data_packet; from_pos:int64=-1):integer; overload;
      function     Put(pkt:PDB_Data_packet; to_pos:int64=-1):integer; overload;
      function     DeletePkt(abs_file_pos:int64):integer;
      function     DeleteAllItem():Boolean;

      function     RebuildFile(const newFname:AnsiString):integer;
      function     ReBuildFileIndex():integer;

      function     Seek_PTS_Min(const start_pts:Int64):Boolean;
      function     Seek_PTS_Max(const start_pts:Int64):Boolean;
      function     Seek_PTS_end:TPTS_DB_Store_File;

    //  function     PtsInRange(pts_start, pts_end:int64):Boolean; overload;
    //  function     PtsInRange(pts_start:int64):Boolean; overload;

      function     put_meta_data(const key,V:AnsiString):AnsiString;  overload;
      function     get_meta_data_json():AnsiString;
      function     put_meta_data(const ALL_KV:AnsiString):AnsiString; overload;


      constructor  OpenRead(const fname: ansistring; pos_Mode:TMX_Pos_mode);  virtual;
      constructor  OpenWrite(const fname: ansistring); Virtual;
      destructor   Destroy; override;

      procedure    Clear_Recorded_Data; // 丢弃全部的数据。
      procedure    finish_File;

      function     isEndOfRead:Boolean;
      function     record_count:uint64;

      function     last_pts:int64;
      procedure    Seek_Percent(pcent:Integer);

      function     attr():AnsiString; overload;
      procedure    attr(const attr:AnsiString); overload;

      function     Expire_data():Boolean; // 实验功能, 删除过期数据, 紧缩重建.
                                         //  不能频繁调用. 24小时闲时调用.

      function     summary():ansiString;

   class function  Header_Size:integer;

   end;


   TPTS_DB_Store_File_list = class
     private
       ro_list,
       rw_list:TAnsiStringList;
       db_path:ansiString;
     public
        on_tabName_to_fileName: function (const tab_name:ansiString):ansiString of object;
        on_rec_item_changed: procedure (const tab_name:ansiString; action:integer;  pkt:PDB_Data_packet) of object;

      function  find_table_RW(const tab_name:ansiString; createNew:Boolean=false):TPTS_DB_Store_File;
      function  find_table_RO(const tab_name:ansiString; createNew:Boolean=false):TPTS_DB_Store_File;

      procedure recycle_rw(fh:TPTS_DB_Store_File);
      procedure recycle_ro(fh:TPTS_DB_Store_File);

      procedure delete(const tab_name:ansiString);
      function  table_exists(const tab_name:ansiString):Boolean;
      function  table_close(const tab_name:ansiString):Boolean;


      constructor  Create(const path: ansistring);
      destructor   Destroy; override;
      procedure    Execute;
   end;

implementation
uses math, ex_funcs;

const
    cnt_Block_Data_Flag = $0F998822;
    cnt_Block_Data_Flag_Deleted = $0F998820;
    cnt_Block_Data_Flag_link_blob = $0F998821;    // 连接到外部的 blob data.

    cnt_pts_file_start_offset = 8192; // 起始数据块的偏移


function is_valid_data_flag(flag:cardinal):Boolean;
begin
  Result := SameInt(flag,cnt_Block_Data_Flag,cnt_Block_Data_Flag_Deleted,cnt_Block_Data_Flag_link_blob);
end;

function UXI_KV_Item_key(P:PUXI_KV_Item):Int64;
begin
   Int64Rec(Result).Hi := p^.key_hi;
   Int64Rec(Result).Lo := p^.key_low;
end;

function UXI_KV_Item_val(P:PUXI_KV_Item):Int64;
begin
   Int64Rec(Result).Hi := p^.V_hi;
   Int64Rec(Result).Lo := p^.V_low;
end;

{ TUX_Indexed_File }

function TUX_Indexed_File.Append(key , V: Int64): TUX_Indexed_File;
var b:TUXI_KV_Item;
begin
   b.Key_low := Int64Rec(key).Lo;
   b.key_hi := Word(Int64Rec(key).Hi);
   b.V_low := Int64Rec(V).Lo;
   b.V_hi :=  Word(Int64Rec(V).Hi);

   if self.FH<>nil then
   begin
     fh.Position := fh.Size;
     FH.WriteBuf(@b,SizeOf(b));

     fh.FlushBuffer;
   end;
end;

constructor TUX_Indexed_File.Create(const fname,Mode:AnsiString);
var buf:array[0..255] of byte;
begin
  Self.FileName := fname;
  Self.FH := nil;
  self.item_len := 0;
  self.f_Mode := mode;

  if mode='r' then
   begin
     self.FH := TShareFile.openRead(fname);
     r_last_size := 0;
   end;

  if (mode='w') or (mode='rw') then
  begin
    Self.FH := TShareFile.openWrite(fname);
    if self.FH.Size<256 then
    begin
      FillChar(buf[0],256,0);
      self.FH.Write(buf[0],256);
    end;
    self.FH.Position := self.FH.Size;
  end;


end;

destructor TUX_Indexed_File.Destroy;
begin
  FreeAndNil(Self.fh);
  inherited;
end;

function TUX_Indexed_File.FindKey(Key: int64; const findmethod: AnsiString): integer;
var C,L,R:integer;
    ret:Int64;
    p:PUXI_KV_Item;
begin
  Result := -1;
  if (item_len<=0) or (Length(bufs)<=0) then exit;

  if findmethod ='=' then
  begin

      L := 0;
      R := self.item_len;
      repeat
         C := (L+R) shr 1;

         ret :=  UXI_KV_Item_key(@bufs[C]) - Key;
         if ret=0 then
         begin
           Result := C;
           Exit;
         end else
         if ret>0 then
         begin
            R := C-1;
         end else
         if ret<0 then
         begin
            L := C +1;
         end;

      until (L>R);

  end else
  if findmethod='left' then
  begin

     if UXI_KV_Item_key(@bufs[0]) >= Key then
     begin
       Result := 0;
       exit;
     end else
     if UXI_KV_Item_key(@bufs[item_len-1])<Key then
     begin
       Result := -1;
       exit;
     end;


      L := 0;
      R := self.item_len;
      repeat
         C := (L+R) shr 1;

         ret :=  UXI_KV_Item_key(@bufs[C]) - Key;
         if ret=0 then
         begin
           Result := C;
           Exit;
         end else
         if ret>0 then
         begin
            R := C-1;

            if UXI_KV_Item_key(@bufs[R]) <=key then
            begin
              Result := R;
              exit;
            end;

         end else
         if ret<0 then
         begin
            L := C +1;

            if UXI_KV_Item_key(@bufs[L]) >=key then
            begin
              Result := C;
              exit;
            end;

         end;

      until (L>R);


  end else
  if findmethod='right' then
  begin

      if UXI_KV_Item_key(@bufs[0]) > Key then
      begin
        Result := -1;
        exit;
      end else
      if UXI_KV_Item_key(@bufs[item_len-1]) <Key then
      begin
        Result := item_len-1;
        exit;
      end;


      L := 0;
      R := self.item_len;
      repeat
         C := (L+R) shr 1;

         ret :=  UXI_KV_Item_key(@bufs[C]) - Key;
         if ret=0 then
         begin
           Result := C;
           Exit;
         end else
         if ret>0 then
         begin
            R := C-1;

            if UXI_KV_Item_key(@bufs[R]) <=key then
            begin
              Result := C;
              exit;
            end;

         end else
         if ret<0 then
         begin
            L := C +1;

            if UXI_KV_Item_key(@bufs[L]) >=key then
            begin
              Result := L;
              exit;
            end;

         end;

      until (L>R);


  end;

end;


function TUX_Indexed_File.func_compare(Index1, Index2: Integer): int64;
begin
   // asc mode.
   Result := UXI_KV_Item_key(@self.bufs[Index1]) - UXI_KV_Item_key(@self.bufs[Index2]);
end;

procedure TUX_Indexed_File.QuickSort(L, R: Integer);

  procedure ExchangeItems(idx1, idx2:integer);
  var U:TUXI_KV_Item;
  begin
    U := self.bufs[idx1];

    self.bufs[idx1] := self.bufs[idx2];
    Self.bufs[idx1] := self.bufs[idx2];

    self.bufs[idx2] := U;
  end;

var
  I, J, P: Integer;
begin
  while L < R do
  begin
    if (R - L) = 1 then
    begin
      if func_compare(L, R) > 0 then
       begin
          ExchangeItems(L, R);
       end;

      break;
    end;
    I := L;
    J := R;
    P := (L + R) shr 1;
    repeat
      while (I <> P) and (func_compare(I, P) < 0) do Inc(I);
      while (J <> P) and (func_compare(J, P) > 0) do Dec(J);
      if I <= J then
      begin
        if I <> J then ExchangeItems(I, J);

        if P = I then
          P := J
        else if P = J then
          P := I;
        Inc(I);
        Dec(J);
      end;
    until I > J;
    if (J - L) > (R - I) then
    begin
      if I < R then
        QuickSort(I, R);
      R := J;
    end
    else
    begin
      if L < J then
        QuickSort(L, J);
      L := I;
    end;
  end;
end;


procedure TUX_Indexed_File.internal_quick_sort;
begin
  //asc mode.

  QuickSort(0, self.item_len-1);

end;

function TUX_Indexed_File.Items(idx: Integer): PUXI_KV_Item;
begin

  if (idx<item_len) and (idx>=0) then
  begin
    Result := @bufs[idx];
  end else Result := nil;

end;


procedure TUX_Indexed_File.reBuild;
var len:integer;
begin
 if SameText(self.f_Mode,'w','rw') then
  begin
     self.Reload_From_File();
     if self.item_len<=0 then exit;

     self.internal_quick_sort();

     len := ex_funcs.min(length(Self.bufs),item_len);
     len := len * sizeof(TUXI_KV_Item);

     self.FH.Size := 256 + len;
     self.FH.Position := 256;
     self.FH.Write(bufs[0],len);
     FreeAndNil(fh);

     fh := TShareFile.OpenWrite(self.FileName);
     fh.Position := fh.size;
  end;

end;

procedure TUX_Indexed_File.Reload_From_File;
var len:integer;
begin
  if self.FH.Size  =  self.r_last_size then exit;
  self.FH.Position := 256;
  self.r_last_size := self.FH.Size;

  len := (self.r_last_size - 256) div sizeof(TUXI_KV_Item);
  item_len := len;

  if length(bufs)< item_len then  SetLength(bufs, len + 8);

  if len>0 then self.FH.Read(bufs[0], len * sizeof(TUXI_KV_Item));

end;



{ TPTS_DB_Store_File }

procedure TPTS_DB_Store_File.attr(const attr: AnsiString);
begin

  self.lockEnter;
  try
     self.Position := 256;
     self.WriteInteger(length(attr));
     if Length(attr)>0 then self.Write(attr[1],Length(attr));
  finally self.lockExit; end;


end;

function TPTS_DB_Store_File.attr(): AnsiString;
begin
  Result := self.f_attr;
end;

procedure TPTS_DB_Store_File.Clear_Recorded_Data;
begin
   Self.Position := TPTS_DB_Store_File.Header_Size;
   self.Size := TPTS_DB_Store_File.Header_Size;
   Self.Write_Mode.last_block_pos := Self.Size;
   self.Write_Mode.last_pts := 0;
end;

function TPTS_DB_Store_File.DeleteAllItem: Boolean;
begin
  Result := true;
  Clear_Recorded_Data();
end;

function TPTS_DB_Store_File.DeletePkt(abs_file_pos: int64): integer;
var p:TDB_Data_packet;
    ret:integer;
begin
  Result := 0;
  if abs_file_pos<=0 then exit;

  self.Position := abs_file_pos;
  ret := self.Read(p, self.packet_header_len);
  if (ret>0) and is_valid_data_flag(p.Block_Flag) then
   begin
     self.Position := abs_file_pos;
     self.WriteInteger(cnt_Block_Data_Flag_Deleted);
     Result := 1;


     if assigned(on_rec_item_changed) then
        begin
          p.Block_Flag := cnt_Block_Data_Flag_Deleted;
          on_rec_item_changed(userData.str, 3, @p);
        end;

   end;


end;

destructor TPTS_DB_Store_File.Destroy;
begin
  inherited;
  FreeAndNil(idx_fh);
end;

//  每 24小时, 执行 1-2次. 按时间 0 点对齐.
function TPTS_DB_Store_File.Expire_data: Boolean;
begin
  Result := false;
  if attr_rec.expire_days<=0 then exit;


end;

procedure TPTS_DB_Store_File.finish_File;
var str:AnsiString;
    T:integer;
begin
   str := self.get_meta_data_json;

   if Self.Size<8192 then self.Size := 8192;

   Self.Position := 256;
   self.WriteString_Ex(str);

end;



function TPTS_DB_Store_File.Get(pkt: PDB_Data_packet; from_pos: int64): integer;
var p:TDB_Data_packet;
    ret, hLen:integer;
    k1,k2:cardinal;
    pos,link_pos:int64;
begin
   // ret >0  OK!
   // ret =0  is deleted block, get again.
   // ret <0  read fail. abort loop get.

   Result := -1;
   Hlen := packet_header_len;

   if (pkt=nil) or ( (Self.Read_Mode.last_block_pos +hlen) > self.Size) then exit;

  // if (pkt.Data=nil) then  =nil 时，只读出头部，不读取数据，加快处理。
  // begin
  //   Result := -2; exit;
  // end;

   if from_pos<0 then // last block.
   begin

     self.Position := self.Read_Mode.last_block_pos;
     pos := self.Position;

   end else
   if from_pos = 0 then   // 第一个数据包
   begin

     self.Position := cnt_pts_file_start_offset;
     pos := self.Position;

   end else
   begin // 从某个地方开始读取

     self.Position := from_pos;
     pos := self.Position;

   end;


   ret := self.Read(p,hlen);
   if ret < hlen then
   begin

     Exit;
   end;

   if not is_valid_data_flag(p.Block_Flag) then
   begin
     self.Read_Mode.last_block_pos := self.Read_Mode.last_block_pos+1; //尝试恢复看看
     Result := -3; exit;
   end;

   if p.Block_Flag = cnt_Block_Data_Flag  then
   begin
       move(p,pkt^,hlen);

       if pkt^.data<>nil then
       begin

         ret := self.Read(pkt^.data^, p.Data_len);
         if ret< p.Data_len then exit;
         if p.Data_len<p.data_space then   // 预留 p.data_space 个字节出来。
         begin
           self.Position := self.Position + (p.data_space - p.Data_len);
         end;

       end else begin
         self.Position := self.Position + p.data_space;
       end;

       k1 := self.ReadInt(4);
       pkt.abs_packet_file_pos := pos;

       Read_Mode.last_block_pos := self.Position;
       Read_Mode.last_pts := p.PTS_IDX;

       Result := hLen + p.data_space + 4;

   end else
   if p.Block_Flag = cnt_Block_Data_Flag_link_blob then
   begin

       move(p,pkt^,hlen);

       if pkt^.data<>nil then
       begin
         ret := self.Read(k1,4); // data - size
         ret := self.Read(link_pos,8); // link-blob-file-pos

         ret := read_blob_data_from(p.PTS_IDX, link_pos, pkt^.data, k1);

         self.Position := self.Position + p.Data_space - 12;
       end else begin
         self.Position := self.Position + p.data_space;
       end;

       k1 := self.ReadInt(4);
       pkt.abs_packet_file_pos := pos;

       pkt^.data_len := k1; // size from blob

       Read_Mode.last_block_pos := self.Position;
       Read_Mode.last_pts := p.PTS_IDX;

       Result := hLen + p.Data_space + 4;
       if k1<=0 then Result := 0;

   end else
   if p.Block_Flag = cnt_Block_Data_Flag_deleted then
   begin

       pkt^.Data_len := p.Data_len;

       self.Position := self.Position + p.Data_space;

       k1 := self.ReadInt(4);
       pkt.abs_packet_file_pos := pos;

       Read_Mode.last_block_pos := self.Position;
       Read_Mode.last_pts := p.PTS_IDX;

       Result := 0;
   end;



end;

function TPTS_DB_Store_File.get_meta_data_json: AnsiString;
begin
  if self.meta_data ='' then
  begin
    Result :=  FileToStr(ChangeFileExt(Self.FileName,'.FDI'));
  end;

  Result := self.meta_data;
end;


class function TPTS_DB_Store_File.Header_Size: integer;
begin
  Result := 8192;
end;

{
//have error.

function TPTS_DB_Store_File.PtsInRange(pts_start: int64): Boolean;
begin
   Result := (pts_start>=self.first_pts_int) and (pts_start<=self.last_pts);
end;

function TPTS_DB_Store_File.PtsInRange(pts_start, pts_end: int64): Boolean;
begin
   Result := self.PtsInRange(pts_start) or self.PtsInRange(pts_end);
end;
}
function TPTS_DB_Store_File.isEndOfRead: Boolean;
begin
  Result := (Self.Read_Mode.last_block_pos + packet_header_len) >= self.Size;
end;

function TPTS_DB_Store_File.last_pts: int64;
begin
  if Read_Mode.active  then Result := Read_Mode.last_pts;
  if write_mode.active then Result := Write_Mode.last_pts;
end;

constructor TPTS_DB_Store_File.OpenRead(const fname: ansistring; pos_Mode:TMX_Pos_mode);
var fsize, fpos:int64;
    s:Cardinal;
    T1,T2,T3:UInt64;
begin
   Inherited OpenRead(fname,32768);
   idx_fh := nil;
   fillchar(self.Read_Mode, sizeof(self.Read_Mode),0);

   fsize := self.Size;
   Self.Read_Mode.Pos_mode := pos_Mode;
   self.Read_Mode.active := True;
   self.Write_Mode.active := false;

   if self.Size>8192 then
     begin
       self.Position := 20;
       self.first_pts_int := self.ReadInt(8);
       self.Position := 256;

       s := self.ReadInt(4);
       if s>(8192-300) then s := 8192 -300;
       setlength(self.f_attr, s);
       if s>0 then self.Read(self.f_attr[1],s);
       _init_attr(f_attr);

       if self.read_last_pts(T1,T2,T3) then
        begin
          self.Read_Mode.last_pts := T1;
          self.Read_Mode.last_block_pos := T2;
          self.Read_Mode.last_Record_NO := T3;
        end;

     end else begin
       self.first_pts_int := 0;


     end;


   if pos_Mode = pm_start then
   begin

       Self.Position := 8192;
       Read_Mode.last_block_pos := 8192;

      // self.last_pts_int := read_mode.last_pts;

   end else
   if (pos_Mode = pm_last_pos) or (pos_Mode = pm_abs_pos) then
   begin
     // self.last_pts_int := read_mode.last_pts;
   end;

end;

constructor TPTS_DB_Store_File.OpenWrite(const fname: ansistring);
var buf:array[0..8192] of byte;
    FLAG,s:cardinal;
    T1,T2,T3:uint64;
begin
   Inherited;
   idx_fh := TUX_Indexed_File.Create(TShareFile.change_file_ext(fname,'.FDX'),'rw');

   fillchar(self.Write_Mode,sizeof(self.Write_Mode),0);
   self.first_pts_int :=0;
   self.Write_Mode.last_pts := 0;
   self.Read_Mode.active := false;
   self.Write_Mode.active := true;

   if self.Size<8192 then
   begin
     self.Position := 0;
     FillChar(buf,8192,0);
     // wirte other params.
     self.Write(buf[0],8192);

     self.Position := 0;
     self.WriteBytes([Byte('F'),Byte('P'),Byte('T'),Byte('S')]);

     self.Position := 20;
     self.first_pts_int := UnixTimeMsec;
     self.WriteInt64(self.first_pts_int);

     idx_fh.params.p3 := self.first_pts_int;

   end else begin

     self.Position := 20;
     self.first_pts_int := self.ReadInt(8);

     idx_fh.params.p3 := self.first_pts_int;

       s := self.ReadInt(4);
       if s>(8192-300) then s := 8192 -300;
       setlength(self.f_attr, s);
       if s>0 then self.Read(self.f_attr[1],s);

       _init_attr(f_attr);


       if self.read_last_pts(T1,T2,T3) then
        begin
          self.write_Mode.last_pts := T1;
          self.write_Mode.last_block_pos := T2;
          self.write_Mode.last_Record_NO := T3;
        end;
   end;

   write_mode.last_block_pos := self.Size;
   write_mode.last_pts := 0;

end;


function TPTS_DB_Store_File.packet_header_len: integer;
begin
  Result := SizeOf(TDB_Data_Packet)- SizeOf(Pointer)- 8;
end;

function TPTS_DB_Store_File.Put(pkt: PDB_Data_packet; to_pos: int64): integer;
var k1,k2, hlen, ret:integer;
    pos:Int64;
    buf:array[0..63] of byte;
    overrideMode:Boolean;
    p2: TDB_Data_packet;
    x1:int64;
begin
  Result := 0;
  if (pkt=nil) or (pkt.PTS_IDX<=0) then exit;

  hlen := packet_header_len; // p.abs_file_pos 不需要写入
  pkt^.Block_Flag := cnt_Block_Data_Flag;
  

  overrideMode := false;
  if to_pos = -1 then // 按顺序写入  , defalt.
  begin
    Self.Position := write_mode.last_block_pos;
    pos := self.Position;

  end else
  if to_pos =-2 then // 绝对最后一个 , APPEND
  begin

    Self.Position := self.Size;
    pos := self.Position;

  end else
  if to_pos = 0 then  // 绝对第一个
  begin

    self.Position := 8192;
    pos := 8192;

    overrideMode := self.size > 8192;

  end else
  if to_pos > 0 then  // 某个标定的 POS
  begin

    if to_pos>=self.size then
    begin
      Self.Position := self.Size;
      pos := self.Position;
    end else begin
      self.Position := to_pos;
      pos := to_pos;
      overrideMode := true;
    end;

  end;

  if overrideMode then // 修改，覆盖等。
  begin

    ret := self.Read(p2,hlen);
    overrideMode := p2.Block_Flag = cnt_Block_Data_Flag_link_blob;

    if (ret=hlen) and (p2.Data_space < pkt.Data_len) then // 外部的
    begin
      k1 := self.ReadInt(4);
      X1 := self.ReadInt(8);

      pkt^.data_space := p2.Data_space;
      pkt^.block_flag := cnt_Block_Data_Flag_link_blob;
      self.Position := pos;
      self.Write(pkt^,hlen);

      if overrideMode then
       begin
         x1 := self.Write_blob_data_to(pkt^.pts_idx,pos,x1,pkt^.data,pkt^.data_len);
       end else begin
         x1 := self.Write_blob_data_to(pkt^.pts_idx,pos,0,pkt^.data,pkt^.data_len);
       end;

      k1 := pkt^.data_len;
      self.WriteInteger(k1);   // 4
      self.WriteInt64(X1);     // 8

      self.Position := self.Position + pkt^.data_space - 12+4;

    end else begin


      self.Position := pos;
      pkt^.data_space := ex_funcs.max(32,pkt^.Data_len);
      pkt^.Block_Flag := cnt_Block_Data_Flag;


      self.Write(pkt^,hlen);
      self.Write(pkt.data^,pkt.Data_len);
      if pkt.Data_len<pkt^.data_space then
      begin
         fillchar(buf[0],length(buf),0);
         self.Write(buf[0],pkt^.data_space-pkt.Data_len);
      end;

      self.Position := self.Position +4;
    end;

  end else begin   //append

    inc(self.Write_Mode.last_Record_NO);

    pkt^.data_space := ex_funcs.max(32,pkt^.Data_len);
    pkt^.Block_Flag := cnt_Block_Data_Flag;
    pkt^.Record_NO :=  self.Write_Mode.last_Record_NO;

    self.Write(pkt^,hlen);
    self.Write(pkt.data^,pkt.Data_len);
    if pkt.Data_len<pkt^.data_space then
    begin
       fillchar(buf[0],length(buf),0);
       self.Write(buf[0],pkt^.data_space-pkt.Data_len);
    end;

    self.WriteInteger(hlen+pkt^.data_space);

  end;

  write_mode.last_block_pos := self.Position;
  write_mode.last_pts := pkt^.PTS_IDX;

  Result := Pos;

  if first_pts_int =0 then
  begin
     first_pts_int := Write_Mode.last_pts;
     self.Position := 20;
     self.WriteInt64(self.first_pts_int);
  end;

  idx_fh.params.p3 := self.first_pts_int;

 // self.last_pts_int := Write_Mode.last_pts;

  // 索引文件处理
  if (to_pos = -1) or (to_pos = -2) then // pts:file_pos
  begin
    idx_fh.Append( Write_Mode.last_pts, pos);
  end;

  self.FlushBuffer;

  if assigned(on_rec_item_changed) then
    begin
      on_rec_item_changed(userData.str,ifthen(overrideMode,2,1), pkt);
    end;


end;

function TPTS_DB_Store_File.put_meta_data(const key,V: AnsiString): AnsiString;
var str, fname:AnsiString;
begin
   if key='' then exit;

   str := self.get_meta_data_json;

   Json_Set(str,key,v);

   StrTOFile(str,ChangeFileExt(FileName,'.FDI'));
end;

function TPTS_DB_Store_File.put_meta_data(const ALL_KV: AnsiString): AnsiString;
begin
  Result := ALL_KV;
  StrTOFile(ALL_KV,ChangeFileExt(FileName,'.FDI'));
end;

function TPTS_DB_Store_File.read_blob_data_from(pts, pos: int64; data: pbyte; datalen: integer): integer;
var fh:TPTS_DB_Store_blob_File;
begin

  fh := TPTS_DB_Store_blob_File.OpenRead(self.FileName+'.blob');
  fh.lock;
  try
    Result := fh.GetData(pts,pos,data,datalen);
  finally
    fh.unlock;
    fh.free;
  end;

end;

function TPTS_DB_Store_File.read_last_pts(var _last_pts, _last_blk_pos, _last_rec_no: UInt64): Boolean;
var R:TDB_Data_packet;
    k1,ret:Integer;
begin
  result := false;
  self.Position := self.Size -4;
  ret := self.Read(k1,4);
  if ret=4 then
   begin
     self.Position := self.Size - 4 - k1;
     ret := self.Read(R,self.packet_header_len);
     if ret>0 then
     begin
       Result := true;
       _last_pts := R.PTS_IDX;
       _last_rec_no := R.Record_NO;
       _last_blk_pos := self.Size - 4 - k1;
     end;
   end;
end;

function TPTS_DB_Store_File.RebuildFile(const newFname: AnsiString): integer;
begin

end;

function TPTS_DB_Store_File.ReBuildFileIndex(): integer;
begin
  if (self.Write_Mode.active) and (self.idx_fh<>nil) then
     self.idx_fh.reBuild();
end;

function TPTS_DB_Store_File.record_count: uint64;
begin
  if self.Read_Mode.active then Result := self.Read_Mode.last_Record_NO;
  if self.Write_Mode.active then Result := self.Write_Mode.last_Record_NO;
end;

// 按百分比快进
procedure TPTS_DB_Store_File.Seek_Percent(pcent: Integer);
begin




end;

function TPTS_DB_Store_File.Seek_PTS_end: TPTS_DB_Store_File;
var s:cardinal;
    fsize,fpos:int64;
begin
   fsize := self.Size;
   self.Position := fsize-4;
   self.Read(s,4);

   fpos := fsize - s;
   Read_mode.last_block_pos := fpos;

   self.Position := fpos + 4;
   self.Read( Read_mode.last_pts, 8);

   self.Position := fpos;
 //  self.last_pts_int := Read_mode.last_pts;

end;

function TPTS_DB_Store_File.Seek_PTS_Max(const start_pts: Int64):boolean;
var delta,k:integer;
    T:int64;

begin
  Result := false;
 if idx_fh=nil then
  begin
   idx_fh := TUX_Indexed_File.Create(TShareFile.change_file_ext(self.FileName,'.FDX'),'r');
  end;
  idx_fh.Reload_From_File;

  delta := start_pts ;
  if delta<=0 then
  begin
    self.Position := self.Header_Size;
    Self.Read_Mode.last_block_pos := self.Header_Size;
    self.Read_Mode.last_pts := self.first_pts_int;
    Result := true;

  end else begin

    K := idx_fh.FindKey(delta,'right');
    T := 0;

    if K<0 then
    begin Result := false; exit; end
    else
       T := UXI_KV_Item_val(idx_fh.items(k));

    if T>0 then
    begin
      self.Read_Mode.last_block_pos := T;
      self.Read_Mode.last_pts := 0;

      Self.Position := T;
      Result := true;
    end;

  end;

end;

function TPTS_DB_Store_File.Seek_PTS_Min(const start_pts: Int64):Boolean;
var delta,k:int64;
    T:int64;

begin
  Result := false;
  if idx_fh=nil then
  begin
    idx_fh := TUX_Indexed_File.Create(TShareFile.change_file_ext(filename,'.FDX'),'r');
  end;
  idx_fh.Reload_From_File;

  delta := start_pts;
  if delta<=0 then
  begin
    self.Position := self.Header_Size;
    Self.Read_Mode.last_block_pos := self.Header_Size;
    self.Read_Mode.last_pts := self.first_pts_int;
    Result := true;

  end else begin

    K := idx_fh.FindKey(delta,'left');
    T := 0;

    if k<0 then
    begin
      Result := false;
      exit;
    end else T := UXI_KV_Item_val(idx_fh.items(k));

    Result := true;

    if T<=0 then T := self.Header_Size;

    if T>0 then
    begin
      self.Read_Mode.last_block_pos := T;
      self.Read_Mode.last_pts := 0;

      Self.Position := T;
      Result := true;
    end;

  end;

end;


function TPTS_DB_Store_File.summary: ansiString;
var T1,T2,T3:UInt64;
begin
  if self.Read_Mode.active then
   begin
     self.read_last_pts(T1,T2,T3);
     self.Read_Mode.last_pts := T1;
   end else
  if self.Write_Mode.active then
   begin
     T3 := self.record_count;
   end;


   Result := json_object(['pts_start',self.first_pts_int,'pts_end',self.last_pts,
                          'size',self.Size,'state',1,'count',T3]);
end;

function TPTS_DB_Store_File.Write_blob_data_to(pts, link_block_pos, abs_file_pos: int64; data: pbyte; datalen: integer): int64;
var fh:TPTS_DB_Store_blob_File;
begin

  fh := TPTS_DB_Store_blob_File.OpenWrite(self.FileName+'.blob');
  self.flock.Enter;
  try
    Result := fh.SaveData(pts,link_block_pos, abs_file_pos, data,datalen);
  finally
    self.flock.Leave;
    fh.free;
  end;

end;

procedure TPTS_DB_Store_File._init_attr(const str: AnsiString);
begin
  if str='' then exit;

  attr_rec.expire_days := json_get_int(str,'expire_days');

end;

{ TPTS_DB_Store_File_list }

constructor TPTS_DB_Store_File_list.Create(const path: ansistring);
begin
  inherited create;
  self.db_path :=path;
  self.rw_list := TAnsiStringList.Create;
  self.ro_list := TAnsiStringList.Create;

end;

procedure TPTS_DB_Store_File_list.delete(const tab_name: ansiString);
var fname:ansiString;
    fh,fh2:TPTS_DB_Store_File;

begin
   fh := self.find_table_RW(tab_name,false);
   if fh=nil then exit;

   rw_list.lockEnter;
   try
     rw_list.Remove(tab_name);


     ro_list.lockEnter;
     try
       repeat

        fh2 := self.find_table_RO(tab_name,false);
        ro_list.Remove(tab_name);
        if fh2<>nil then fh2.free;

       until fh2=nil;

       fh.DeleteAllItem();
       fname := fh.FileName;
       try fh.free; except end;

       deleteFile(fname);
       deleteFile(fname+'.blob');
       deleteFile(TShareFile.change_file_ext(fname,'.FDX'));

     finally ro_list.lockExit; end;
   finally
     rw_list.lockExit;
   end;
end;

destructor TPTS_DB_Store_File_list.Destroy;
begin
  self.rw_list.Clear(true);
  freeAndnil(rw_list);

  self.ro_list.Clear(true);
  freeAndnil(ro_list);

  inherited;
end;

procedure TPTS_DB_Store_File_list.Execute;
begin

end;

function TPTS_DB_Store_File_list.find_table_RW(const tab_name: ansiString; createNew:Boolean): TPTS_DB_Store_File;
var i:integer;
    fname:ansiString;
begin
   Result := nil;

   rw_list.lockEnter;
   try

     i := self.rw_list.IndexOf(tab_name);
     if i>=0 then
      begin

        Result := TPTS_DB_Store_File(self.rw_list.Objects[i]);
        Result.params.D1 := 1;

      end else begin

       if createNew then
        begin
          fname := on_tabName_to_fileName(tab_name);


          Result := TPTS_DB_Store_File.OpenWrite(fname);
          Result.userData.str := tab_name;
          Result.params.D1 := 1;
          Result.on_rec_item_changed := self.on_rec_item_changed;

          self.rw_list.AddObject(tab_name,Result);

        end else Result := nil;

      end;

   finally
     rw_list.lockExit;
   end;

end;

procedure TPTS_DB_Store_File_list.recycle_ro(fh: TPTS_DB_Store_File);
begin
  // if assigned(fh) then
  //  begin
  //   try fh.free; except end;
  //  end;


   ro_list.lockEnter;
   try
     ro_list.AddObject(fh.userData.str,fh);
   finally
     ro_list.lockExit;
   end;

end;

procedure TPTS_DB_Store_File_list.recycle_rw(fh: TPTS_DB_Store_File);
begin
   rw_list.lockEnter;
   try
     fh.params.D1 := 0;
   finally
     rw_list.lockExit;
   end;
end;

function TPTS_DB_Store_File_list.table_close(const tab_name: ansiString): Boolean;
var fname:ansiString;
    fh,fh2:TPTS_DB_Store_File;

begin
   fh := self.find_table_RW(tab_name,false);
   if fh=nil then exit;

   rw_list.lockEnter;
   try
     rw_list.Remove(tab_name);

     ro_list.lockEnter;
     try
       repeat

        fh2 := self.find_table_RO(tab_name,false);
        ro_list.Remove(tab_name);
        if fh2<>nil then fh2.free;

       until fh2=nil;

       try fh.free; except end;

     finally ro_list.lockExit; end;
     
   finally
     rw_list.lockExit;
   end;

end;

function TPTS_DB_Store_File_list.table_exists(const tab_name: ansiString): Boolean;
var i:integer;
    fname:ansiString;
begin
   fname := on_tabName_to_fileName(tab_name);
   Result := FileExists(fname);
end;

function TPTS_DB_Store_File_list.find_table_RO(const tab_name: ansiString; createNew:Boolean): TPTS_DB_Store_File;
var i:integer;
    fname:ansiString;
begin


   ro_list.lockEnter;
   try

     i := self.ro_list.IndexOf(tab_name);
     if i>=0 then
      begin
        Result := TPTS_DB_Store_File(self.ro_list.Objects[i]);

        ro_list.Delete(i);

      end else begin

       if createNew then
        begin
          fname := on_tabName_to_fileName(tab_name);

          Result := TPTS_DB_Store_File.OpenRead(fname,pm_start);
          Result.userData.str := tab_name;

        end else Result := nil;

      end;

   finally ro_list.lockExit ; end;

end;

{ TPTS_DB_Store_blob_File }
type

  PPTS_DB_Blob_Item = ^TPTS_DB_Blob_Item;
  TPTS_DB_Blob_Item = packed record
    flag:cardinal;  // index flag.
    pts_idx:int64;
    link_db_block_idx:int64;
    data_len:cardinal;
    data_space:cardinal;
  end;


destructor TPTS_DB_Store_blob_File.Destroy;
begin
  freeAndNil(fh);
  inherited destroy;
end;

function TPTS_DB_Store_blob_File.GetData(pts_idx, pos: int64; data: pbyte; datalen: integer): integer;
var pkt: TPTS_DB_Blob_Item;
begin
   Result := 0;

   fh.Position := pos;

   fh.Read(pkt,sizeof(pkt));

   if pkt.flag = cnt_Block_Data_Flag_link_blob then
   begin
     Result := pkt.data_len;
     fh.Read(data^,pkt.data_len);
   end;

end;

procedure TPTS_DB_Store_blob_File.lock;
begin
 if fh<>nil then fh.lockEnter;
end;

constructor TPTS_DB_Store_blob_File.OpenRead(const fname: ansistring);
begin
   fh := TShareFile.openRead(fname);
end;

constructor TPTS_DB_Store_blob_File.OpenWrite(const fname: ansistring);
var buf:array[0..1023] of byte;
begin
   fh := TShareFile.openWrite(fname);
   if fh.Size<1024 then
    begin
      fillchar(buf[0],1024,0);
      fh.Position := 0;
      fh.Write(buf[0],1024);
    end;

end;

function TPTS_DB_Store_blob_File.SaveData(pts_idx, block_pos,abs_file_pos: int64; data: pbyte; datalen: integer): int64;
var pkt,p2: TPTS_DB_Blob_Item;
    buf:array[0..63] of byte;
begin
   fillchar(pkt,sizeof(pkt),0);
   fillchar(buf[0],length(buf),0);

   pkt.flag := cnt_Block_Data_Flag_link_blob;
   pkt.pts_idx := pts_idx;
   pkt.link_db_block_idx := block_pos;
   pkt.data_len := datalen;

   if abs_file_pos<=0 then
   begin
     fh.Position := fh.Size;
     pkt.data_space := ex_funcs.max(64,pkt.data_len);
   end else begin
     fh.Position := abs_file_pos;
     fh.Read(p2,sizeof(p2));
     if p2.flag=cnt_Block_Data_Flag_link_blob then
     begin
       if p2.data_space >= pkt.data_len then
       begin
         pkt.data_space := p2.data_space;
         fh.Position := abs_file_pos;

       end else begin
         fh.Position := fh.Size;
       end;
     end else fh.Position := fh.Size;
   end;

   Result := fh.Position;

   fh.Write(pkt,sizeof(pkt));
   fh.Write(data^,datalen);
   if pkt.data_len<64 then
    begin
      fh.Write(buf[0],64 - pkt.data_len);
    end;

end;

procedure TPTS_DB_Store_blob_File.unlock;
begin
  if fh<>nil then fh.lockExit;
end;

end.
