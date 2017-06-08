#include "cslist.h"

#include <cstddef>
#include <cstdio>

#define LOG(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__)
struct node {
  node *next;
  int payload;
};
typedef cslist<node, &node::next> mylist;
int main() {
  mylist list;

  static size_t const nnodes = 7;
  node nodes[nnodes];
  LOG("Pushing %zd nodes at the tail of the list, for popping in FIFO order\n",
      nnodes);
  for (size_t i = 0; i < nnodes; i++) {
    nodes[i].payload = i + 1;
    list.push_tail(nodes + i);
  }

  LOG("Popping the list until empty.\n\tExpected: 1 2 ... %zd\n\tFound:   ",
      nnodes);
  while (node *n = list.pop_head()) {
    LOG(" %d", n->payload);
  }

  LOG("\n\nPushing %zd nodes at the head of the list, for popping in LIFO "
      "order\n",
      nnodes);
  for (size_t i = 0; i < nnodes; i++) {
    nodes[i].payload = i + 1;
    list.push_head(nodes + i);
  }

  LOG("Popping the list until empty.\n\tExpected: %zd ... 2 1\n\tFound:   ",
      nnodes);
  while (node *n = list.pop_head()) {
    LOG(" %d", n->payload);
  }

  LOG("\n\nAlternating pushes to head and tail\n");
  for (size_t i = 0; i < nnodes; i++) {
    nodes[i].payload = i + 1;
    if (i % 2)
      list.push_head(nodes + i);
    else
      list.push_tail(nodes + i);
  }

  LOG("Iterating over the list.\n\tExpected: %zd ... 2 1 3 ... %zd\n\tFound:  "
      " ",
      nnodes - 1, nnodes);
  for (mylist::iterator it = list.begin(); it != list.end(); ++it) {
    LOG(" %d", it->payload);
  }

  LOG("\nPopping the list until empty.\n\tExpected: %zd ... 2 1 3 ... "
      "%zd\n\tFound:   ",
      nnodes - 1, nnodes);
  while (node *n = list.pop_head()) {
    LOG(" %d", n->payload);
  }

  LOG("\n\nThe list should be empty now: %s\n",
      list.empty() ? "true" : "false");
  LOG("Iterating over the empty list.\n\tExpected: <nothing>\n\tFound:   ");
  for (mylist::iterator it = list.begin(); it != list.end(); ++it) {
    LOG(" %d", it->payload);
  }
}
