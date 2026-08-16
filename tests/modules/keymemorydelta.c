#include "../../src/valkeymodule.h"

#include <stdint.h>
#include <string.h>

#define MAX_TRACKED_DBS 128

typedef struct KeyMemoryDeltaStats {
  uint64_t bytes[MAX_TRACKED_DBS];
  uint64_t keys[MAX_TRACKED_DBS];
  uint64_t events;
  ValkeyModuleString *last_key;
  uint64_t last_old_bytes;
  uint64_t last_new_bytes;
  int last_dbnum;
  int last_old_exists;
  int last_new_exists;
} KeyMemoryDeltaStats;

static KeyMemoryDeltaStats stats;

static void resetStats(void) {
  if (stats.last_key)
    ValkeyModule_FreeString(NULL, stats.last_key);
  memset(&stats, 0, sizeof(stats));
}

static void applyContribution(uint64_t *counter, uint64_t old_value,
                              uint64_t new_value) {
  if (new_value >= old_value)
    *counter += new_value - old_value;
  else
    *counter -= old_value - new_value;
}

static void keyMemoryDeltaCallback(ValkeyModuleCtx *ctx,
                                   ValkeyModuleEvent event, uint64_t subevent,
                                   void *data) {
  VALKEYMODULE_NOT_USED(ctx);
  VALKEYMODULE_NOT_USED(event);
  VALKEYMODULE_NOT_USED(subevent);
  ValkeyModuleKeyMemoryDelta *delta = data;
  if (delta->dbnum < 0 || delta->dbnum >= MAX_TRACKED_DBS)
    return;

  applyContribution(&stats.bytes[delta->dbnum], delta->old_bytes,
                    delta->new_bytes);
  applyContribution(&stats.keys[delta->dbnum], delta->old_exists != 0,
                    delta->new_exists != 0);
  stats.events++;
  if (stats.last_key)
    ValkeyModule_FreeString(NULL, stats.last_key);
  stats.last_key = ValkeyModule_CreateStringFromString(NULL, delta->key);
  stats.last_old_bytes = delta->old_bytes;
  stats.last_new_bytes = delta->new_bytes;
  stats.last_dbnum = delta->dbnum;
  stats.last_old_exists = delta->old_exists;
  stats.last_new_exists = delta->new_exists;
}

static int resetCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  VALKEYMODULE_NOT_USED(argv);
  if (argc != 1)
    return ValkeyModule_WrongArity(ctx);
  resetStats();
  return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static int usageCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  if (argc != 1 && argc != 2)
    return ValkeyModule_WrongArity(ctx);
  long long dbnum = ValkeyModule_GetSelectedDb(ctx);
  if (argc == 2 &&
      ValkeyModule_StringToLongLong(argv[1], &dbnum) == VALKEYMODULE_ERR)
    return ValkeyModule_ReplyWithError(ctx, "ERR invalid database");
  if (dbnum < 0 || dbnum >= MAX_TRACKED_DBS)
    return ValkeyModule_ReplyWithError(ctx, "ERR invalid database");

  ValkeyModule_ReplyWithArray(ctx, 3);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.bytes[dbnum]);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.keys[dbnum]);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.events);
  return VALKEYMODULE_OK;
}

static int lastCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  VALKEYMODULE_NOT_USED(argv);
  if (argc != 1)
    return ValkeyModule_WrongArity(ctx);
  if (!stats.last_key)
    return ValkeyModule_ReplyWithNull(ctx);

  ValkeyModule_ReplyWithArray(ctx, 7);
  ValkeyModule_ReplyWithString(ctx, stats.last_key);
  ValkeyModule_ReplyWithLongLong(ctx, stats.last_dbnum);
  ValkeyModule_ReplyWithLongLong(ctx, stats.last_old_exists);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.last_old_bytes);
  ValkeyModule_ReplyWithLongLong(ctx, stats.last_new_exists);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.last_new_bytes);
  ValkeyModule_ReplyWithLongLong(ctx, (long long)stats.events);
  return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  VALKEYMODULE_NOT_USED(argv);
  VALKEYMODULE_NOT_USED(argc);
  if (ValkeyModule_Init(ctx, "keymemorydelta", 1, VALKEYMODULE_APIVER_1) ==
      VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  if (ValkeyModule_CreateCommand(ctx, "keymemorydelta.reset", resetCommand,
                                 "write", 0, 0, 0) == VALKEYMODULE_ERR ||
      ValkeyModule_CreateCommand(ctx, "keymemorydelta.usage", usageCommand,
                                 "readonly", 0, 0, 0) == VALKEYMODULE_ERR ||
      ValkeyModule_CreateCommand(ctx, "keymemorydelta.last", lastCommand,
                                 "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
    return VALKEYMODULE_ERR;
  return ValkeyModule_SubscribeToServerEvent(
      ctx, ValkeyModuleEvent_KeyMemoryDelta, keyMemoryDeltaCallback);
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
  VALKEYMODULE_NOT_USED(ctx);
  resetStats();
  return VALKEYMODULE_OK;
}
