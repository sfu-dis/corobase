#pragma once

#include "sm-defs.h"

#include <typeinfo>
#include <type_traits>

namespace ermia {

/* This file provides support for a "stub-impl" idiom, where a "stub"
   class contains all world-facing members of an object, and an "impl"
   class contains all private/internal state. This idiom combines the
   concept of an interface (abstract base class) with the "pimple"
   idiom, but avoids the drawbacks of both: There are no extra
   pointers to chase, and method calls have no overhead vs. a normal
   class (this is true even when an "interface" method calls an "impl"
   method). Internally, this desirable outcome is achieved by
   arranging for the impl object to inherit from the stub, and then
   upcasting, but this detail is transparent to the stub's user.

   The one drawback is that stub-impl objects cannot be allocated
   directly by the user, but must be created and destroyed by factory
   methods. We can prevent the user from instantiating stubs directly,
   and we can also prevent the developer from creating multiple
   implementations (this is *not* a mechanism to support
   polymorphism).

   // in .h //////////////////////////////

   // the "stub" is world-visible
   struct foo {
       // note lack of data members

       // factory methods instead of constructor/destructor
       static
       foo *make();

       void destroy();

       // public method
       void bar();

   protected:
       // prevent users from allocating the stub directly
       foo() { }
       ~foo() { }
   };
   ///////////////////////////////////////

   // in .cpp ////////////////////////////

   // derive the "impl" class from stub
   struct foo_impl : foo {
       // a private method
       void baz();
   };

   // must DEF_IMPL *after* defining the "impl" class
   DEF_IMPL(foo);

   // define factory methods for stub
   foo *foo::make() {
       return new foo_impl;
   }
   void foo::destroy() {
       delete get_impl(this);
   }

   // define "stub" functions
   void foo::bar() {
       get_impl(this)->baz();
   }

   //////////////////////////////////////
*/

/* Invoke this macro after defining the impl class.

   The impl class must derive from the stub class.

   NOTE: N2965 proposes a std::direct_bases trait which would be
   perfect for this, but gcc is the only compiler that implements it
   and the proposal appears to be dead in the water. Avoiding it.
 */
#define DEF_IMPL(tp) DEF_IMPL2(tp, tp##_impl)

#define DEF_IMPL2(tp, impl)                         \
  static_assert(std::is_base_of<tp, impl>::value,   \
                #impl " is not derived from " #tp); \
  template <>                                       \
  struct _impl_of<tp> {                             \
    typedef impl type;                              \
  }

template <typename T>
struct _impl_of {
  typedef T type;
};

/* Given a pointer to T (a stub), return a pointer to T's
   implementation. The default is to return a pointer to T. If the
   user has declared a stub-impl relationship, return a pointer to the
   impl instead. This ensures that only one implementation exists.
 */
template <typename T>
typename _impl_of<T>::type *get_impl(T *ptr) {
  return (typename _impl_of<T>::type *)ptr;
}
template <typename T>
typename _impl_of<T>::type const *get_impl(T const *ptr) {
  return (typename _impl_of<T>::type const *)ptr;
}

}  // namespace ermia
