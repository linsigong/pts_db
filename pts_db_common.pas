unit pts_db_common;
{$ifdef fpc}
   {$mode delphi}
{$endif}
{$h+}
interface

const

  cnt_pts_live_stream  = -1;
  cnt_pts_stream_begin =  0;


  cnt_error_get_data_ok   =  1;    // >0 有数据来。
  cnt_error_get_data_none = -1;  // 没有符合条件的数据，需要继续 getData
  cnt_error_get_data_end  = -2;  // 数据结束了。


  cnt_action_insert   = 1;
  cnt_action_modify   = 2;
  cnt_action_delete   = 3;

type

  PPTS_DB_Item = ^TPTS_DB_Item;
  TPTS_DB_Item = object
    RecNo:cardinal;
    time:int64;
    key1,
    key2:cardinal;
    data:pbyte;
    data_len:integer;
  end;


  TPTS_DB_Where_func = function (const tab_name:ansiString; item:PPTS_DB_Item; var abort:Boolean):Boolean;
  TUX_pts_db_each_func = function (const tab_name:ansiString; userData:pointer;  item:PPTS_DB_Item; var abort:Boolean):boolean;

  IStreamReader = interface
    ['{FB36B694-3E24-4A4D-A3FD-A8D109D16F6B}']
    function  getData(PX:PPTS_DB_Item):integer;
    function  getUUID:AnsiString;
    function  getUserData():pointer;
    procedure setUserData(data:pointer);

    function  getPTS:int64;
    procedure setPTS(Time:int64);

    property PTS:int64 read getPTS write setPTS;
    property UserData:pointer read getUserData write setUserData;
    property UUID:ansiString read getUUID;
  end;


implementation

end.
