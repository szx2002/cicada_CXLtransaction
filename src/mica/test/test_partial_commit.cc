#include <cstdio>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <csignal>
#include <cstdlib>
#include <sys/types.h>
#include <sys/wait.h>
#include "mica/transaction/db.h"
#include "mica/util/lcore.h"
#include "mica/util/config.h"
#include "mica/util/stopwatch.h"
#include "mica/test/test_tx_conf.h"

typedef DBConfig::Alloc Alloc;
typedef DBConfig::Logger Logger;
typedef DBConfig::Timestamp Timestamp;
typedef ::mica::transaction::PagePool<DBConfig> PagePool;
typedef ::mica::transaction::DB<DBConfig> DB;
typedef ::mica::transaction::Table<DBConfig> Table;
typedef ::mica::transaction::RowAccessHandle<DBConfig> RowAccessHandle;
typedef ::mica::transaction::RowAccessHandlePeekOnly<DBConfig> RowAccessHandlePeekOnly;
typedef ::mica::transaction::Transaction<DBConfig> Transaction;
typedef ::mica::transaction::Result Result;

static ::mica::util::Stopwatch sw;
static uint64_t actual_row_ids[4];

void reader_worker(DB* db, Table* tbl) {
  auto ctx = new ::mica::transaction::Context<DBConfig>(db, 100, 1);
  Transaction tx(ctx);
  tx.begin(true);

  printf("[Reader] Reading row states:\n");

  for (int i = 0; i < 4; i++) {
    RowAccessHandlePeekOnly rah(&tx);
    if (!rah.peek_row(tbl, 0, actual_row_ids[i], false, false, false)) {
      printf("  [%d] <not found>\n", i);
      continue;
    }
    const char* data = reinterpret_cast<const char*>(rah.cdata());
    if (!data) {
      printf("  [%d] <null>\n", i);
    } else {
      printf("  [%d] value = %d\n", i, (int)*data);
    }
  }

  Result result;
  tx.commit(&result);
  delete ctx;
}

struct DelayedWriteFunc {
  bool operator()() const {
    for (int i = 0; i < 4; i++) {
      printf("[Writer] Committing row %d\n", i);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (i == 1) {
        printf("[Writer] Simulated interruption after 2 rows. Exiting to simulate partial commit.\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        _exit(1);  // 使用 _exit 替代 std::exit
      }
    }
    return true;
  }
};

void writer_worker(DB* db, Table* tbl, char target_value) {
  auto ctx = db->context(0);
  Transaction tx(ctx);
  tx.begin();

  printf("[Writer] Start writing 4 rows with value = %d\n", (int)target_value);
  for (int i = 0; i < 4; i++) {
    RowAccessHandle rah(&tx);
    if (!rah.peek_row(tbl, 0, actual_row_ids[i], false, true, true) ||
        !rah.read_row() || !rah.write_row(sizeof(char))) {
      printf("[Writer] Failed at row %d\n", i);
      tx.abort();
      return;
    }
    *reinterpret_cast<char*>(rah.data()) = target_value;
  }

  printf("[Writer] Starting delayed commit...\n");
  Result result;
  tx.commit(&result, DelayedWriteFunc());
}

int main() {
  printf("[Main] Initializing DB\n");

  auto config = ::mica::util::Config::load_file("test_tx.json");
  Alloc alloc(config.get("alloc"));
  auto page_pool_size = 8ul * 1024 * 1024 * 1024;
  PagePool* page_pools[2] = {
    new PagePool(&alloc, page_pool_size / 2, 0),
    new PagePool(&alloc, page_pool_size / 2, 1)
  };

  sw.init_start();
  sw.init_end();

  Logger logger;
  DB db(page_pools, &logger, &sw, 2);
  const uint64_t kDataSizes[] = {sizeof(char)};
  db.create_table("test", 1, kDataSizes);
  Table* tbl = db.get_table("test");
  db.activate(0);
  db.activate(1);

  {
    Transaction tx(db.context(0));
    tx.begin();
    for (int i = 0; i < 4; i++) {
      RowAccessHandle rah(&tx);
      rah.new_row(tbl, 0, Transaction::kNewRowID, true, sizeof(char));
      actual_row_ids[i] = rah.row_id();
      *reinterpret_cast<char*>(rah.data()) = 0;
    }
    Result result;
    tx.commit(&result);
    printf("[Main] Initialized all rows to 0\n");
  }

  {
    std::thread writer(writer_worker, &db, tbl, 1);
    writer.join();
    printf("[Main] First commit (1111) done.\n");
  }

  pid_t pid = fork();
  if (pid == 0) {
    writer_worker(&db, tbl, 2);
    return 0;
  }

  int status;
  waitpid(pid, &status, 0);
  printf("[Main] Writer process terminated. Sleeping 1s...\n");
  std::this_thread::sleep_for(std::chrono::seconds(1));

  reader_worker(&db, tbl);

  db.deactivate(0);
  db.deactivate(1);
  delete page_pools[0];
  delete page_pools[1];

  printf("[Main] Test completed.\n");
  return 0;
}