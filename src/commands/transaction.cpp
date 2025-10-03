#include "command_handler.h"
#include "../protocol/resp_parser.h"

// transaction state is defined in main.cpp and extern'd here
extern std::mutex transaction_mutex;
extern std::unordered_map<int, TransactionState> client_transactions;

// this file can contain additional transaction related utilities if needed