#ifndef REDIS_CPP_SCOPE_GUARD_H
#define REDIS_CPP_SCOPE_GUARD_H
#include <concepts>
#include <utility>

namespace redis_net
{

template <typename F, typename T>
concept CallableNoReturn = requires(F f, T t) {
  { f(t) } -> std::same_as<void>;
};

template <typename T, CallableNoReturn<T> F>
class ScopeGuard
{
public:
  ScopeGuard(T obj, F func) : obj_{obj}, func_{std::move(func)} {}
  ~ScopeGuard() noexcept { func_(obj_); }

  ScopeGuard(ScopeGuard const &) = delete;
  ScopeGuard &operator=(ScopeGuard const &) = delete;
  ScopeGuard(ScopeGuard &&) = delete;
  ScopeGuard &operator=(ScopeGuard &&) = delete;

private:
  T obj_;
  F func_;
};

} // namespace redis_net

#endif // REDIS_CPP_SCOPE_GUARD_H
