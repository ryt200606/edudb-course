#pragma once
#include <functional>
#include <utility>
namespace edudb {
// A statement-owned callback. It must not take the lock-manager mutex: page
// reads can hold the transaction coordinator while commit takes LM -> WAL ->
// coordinator. Atomic cancellation tokens and local budgets are safe here.
extern thread_local std::function<void()> page_progress_callback;
class PageProgressScope {
    std::function<void()> previous_;
public:
    explicit PageProgressScope(std::function<void()> callback):previous_(std::move(page_progress_callback)){page_progress_callback=std::move(callback);}
    ~PageProgressScope(){page_progress_callback=std::move(previous_);}
    PageProgressScope(const PageProgressScope&)=delete;
    PageProgressScope& operator=(const PageProgressScope&)=delete;
};
inline void page_progress(){if(page_progress_callback)page_progress_callback();}
}
