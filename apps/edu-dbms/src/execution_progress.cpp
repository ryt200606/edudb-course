#include "execution_progress.hpp"
namespace edudb {
thread_local std::function<void()> page_progress_callback;
}
