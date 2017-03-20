#pragma once

#ifdef _MSC_VER

#include "targetver.h"

#define WIN32_LEAN_AND_MEAN             // Windows ヘッダーから使用されていない部分を除外します。

#endif

#include <cstdint>

// TODO: プログラムに必要な追加ヘッダーをここで参照してください
#include "../../externals/msredis/deps/hiredis/hiredis.h"


#include "Common.h"