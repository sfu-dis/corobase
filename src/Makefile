-include config.mk

### Options ###
#CXX=icc
#CXX=clang
#CXX=/home/ipandis/apps/bin/gcc483/g++
CXX=g++
#CXX=g++491

DEBUG ?= 0
CHECK_INVARIANTS ?= 0

# 0 = libc malloc
# 1 = jemalloc
# 2 = tcmalloc
# 3 = flow
USE_MALLOC_MODE ?= 2

MYSQL ?= 1
MYSQL_SHARE_DIR ?= /x/stephentu/mysql-5.5.29/build/sql/share

# Available modes
#   * perf
#   * backoff
#   * factor-gc
#   * factor-gc-nowriteinplace
#   * factor-fake-compression
#   * sandbox
MODE ?= perf

# run with 'MASSTREE=0' to turn off masstree
MASSTREE ?= 1

###############

DEBUG_S=$(strip $(DEBUG))
CHECK_INVARIANTS_S=$(strip $(CHECK_INVARIANTS))
EVENT_COUNTERS_S=$(strip $(EVENT_COUNTERS))
USE_MALLOC_MODE_S=$(strip $(USE_MALLOC_MODE))
MODE_S=$(strip $(MODE))
MASSTREE_S=$(strip $(MASSTREE))
MASSTREE_CONFIG:=--enable-max-key-len=1024
MASSTREE_LDFLAGS:=

ifeq ($(DEBUG_S),1)
	OSUFFIX_D=.debug
	MASSTREE_CONFIG+=--enable-assertions
else
	MASSTREE_CONFIG+=--disable-assertions
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	OSUFFIX_S=.check
	MASSTREE_CONFIG+=--enable-invariants --enable-preconditions
else
	MASSTREE_CONFIG+=--disable-invariants --disable-preconditions
endif
ifeq ($(EVENT_COUNTERS_S),1)
	OSUFFIX_E=.ectrs
endif
OSUFFIX=$(OSUFFIX_D)$(OSUFFIX_S)$(OSUFFIX_E)

ifeq ($(MODE_S),perf)
	O := out-perf$(OSUFFIX)
	CONFIG_H = config/config-perf.h
else ifeq ($(MODE_S),backoff)
	O := out-backoff$(OSUFFIX)
	CONFIG_H = config/config-backoff.h
else ifeq ($(MODE_S),factor-gc)
	O := out-factor-gc$(OSUFFIX)
	CONFIG_H = config/config-factor-gc.h
else ifeq ($(MODE_S),factor-gc-nowriteinplace)
	O := out-factor-gc-nowriteinplace$(OSUFFIX)
	CONFIG_H = config/config-factor-gc-nowriteinplace.h
else ifeq ($(MODE_S),factor-fake-compression)
	O := out-factor-fake-compression$(OSUFFIX)
	CONFIG_H = config/config-factor-fake-compression.h
else ifeq ($(MODE_S),sandbox)
	O := out-sandbox$(OSUFFIX)
	CONFIG_H = config/config-sandbox.h
else
	$(error invalid mode)
endif

CXXFLAGS := -Wall -std=c++0x -DHACK_SILO -g
CXXFLAGS += -MD -Ithird-party/lz4 -DCONFIG_H=\"$(CONFIG_H)\"
ifeq ($(DEBUG_S),1)
        CXXFLAGS +=  -g -gdwarf-2 -fno-omit-frame-pointer -DDEBUG #-fsanitize=address
else
        CXXFLAGS += -O2 -funroll-loops -fno-omit-frame-pointer
        #CXXFLAGS += -Werror -O2 -funroll-loops -fno-omit-frame-pointer
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	CXXFLAGS += -DCHECK_INVARIANTS
endif
ifeq ($(EVENT_COUNTERS_S),1)
	CXXFLAGS += -DENABLE_EVENT_COUNTERS
endif
ifeq ($(MASSTREE_S),1)
	CXXFLAGS += -DNDB_MASSTREE -include masstree/config.h
	OBJDEP += masstree/config.h
	O := $(O).masstree
else
	O := $(O).silotree
endif

TOP     := $(shell echo $${PWD-`pwd`})
LDFLAGS := -lpthread -lnuma -lrt -static-libstdc++
#ifeq ($(DEBUG_S),1)
#LDFLAGS += -lasan
#endif

LZ4LDFLAGS := -Lthird-party/lz4 -llz4 -Wl,-rpath,$(TOP)/third-party/lz4

GPERFTOOLS :=

ifeq ($(USE_MALLOC_MODE_S),1)
        CXXFLAGS+=-DUSE_JEMALLOC
        LDFLAGS+=-ljemalloc
	MASSTREE_CONFIG+=--with-malloc=jemalloc
else ifeq ($(USE_MALLOC_MODE_S),2)
	GPERFTOOLS+=/home/ipandis/GITPROJECTS/Impala/thirdparty/gperftools-2.0
        CXXFLAGS+=-DUSE_TCMALLOC -I$(GPERFTOOLS)/src/
        LDFLAGS+="-L$(GPERFTOOLS) -ltcmalloc"
	MASSTREE_LDFLAGS="-L$(GPERFTOOLS)/.libs/ "
	MASSTREE_CONFIG+=--with-malloc=tcmalloc
else ifeq ($(USE_MALLOC_MODE_S),3)
        CXXFLAGS+=-DUSE_FLOW
        LDFLAGS+=-lflow
	MASSTREE_CONFIG+=--with-malloc=flow
else
	MASSTREE_CONFIG+=--with-malloc=malloc
endif

ifneq ($(strip $(CUSTOM_LDPATH)), )
        LDFLAGS+=$(CUSTOM_LDPATH)
endif

SRCFILES = allocator.cc \
	core.cc \
	counter.cc \
	memory.cc \
	rcu-wrapper.cc \
	stats_server.cc \
	thread.cc \
	tuple.cc \
	txn_btree.cc \
	txn.cc \
	txn_proto2_impl.cc \
	varint.cc
#ticker.cc \
#txn_table.cc \
#rcu.cc \
#allocator.cc \
#btree.cc \

DBCORE_SRCFILES = dbcore/sm-alloc.cpp \
	dbcore/sm-log.cpp \
	dbcore/sm-tx-log.cpp \
	dbcore/sm-log-alloc.cpp \
	dbcore/sm-log-recover.cpp \
	dbcore/sm-log-offset.cpp \
	dbcore/sm-log-file.cpp \
	dbcore/sm-exceptions.cpp \
	dbcore/sm-common.cpp \
	dbcore/sm-gc.cpp \
	dbcore/window-buffer.cpp \
	dbcore/rcu-slist.cpp \
	dbcore/rcu.cpp \
	dbcore/epoch.cpp \
	dbcore/adler.cpp \
	dbcore/w_rand.cpp \
	dbcore/size-encode.cpp \
	dbcore/xid.cpp

ifeq ($(MASSTREE_S),1)
MASSTREE_SRCFILES = masstree/compiler.cc \
	masstree/str.cc \
	masstree/string.cc \
	masstree/straccum.cc \
	masstree/json.cc
endif

OBJFILES := $(patsubst %.cc, $(O)/%.o, $(SRCFILES))

MASSTREE_OBJFILES := $(patsubst masstree/%.cc, $(O)/%.o, $(MASSTREE_SRCFILES))
DBCORE_OBJFILES := $(patsubst dbcore/%.cpp, $(O)/dbcore/%.o, $(DBCORE_SRCFILES))

BENCH_CXXFLAGS := $(CXXFLAGS)
BENCH_LDFLAGS := $(LDFLAGS) -ldb_cxx -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_SRCFILES = benchmarks/bdb_wrapper.cc \
	benchmarks/bench.cc \
	benchmarks/encstress.cc \
	benchmarks/bid.cc \
	benchmarks/masstree/kvrandom.cc \
	benchmarks/queue.cc \
	benchmarks/tpcc.cc
#	benchmarks/ycsb.cc FIXME: tzwang: don't bother non-tpcc for now

ifeq ($(MYSQL_S),1)
BENCH_CXXFLAGS += -DMYSQL_SHARE_DIR=\"$(MYSQL_SHARE_DIR)\"
BENCH_LDFLAGS := -L/usr/lib/mysql -lmysqld $(BENCH_LDFLAGS)
BENCH_SRCFILES += benchmarks/mysql_wrapper.cc
else
BENCH_CXXFLAGS += -DNO_MYSQL
endif

BENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(BENCH_SRCFILES))

NEWBENCH_SRCFILES = new-benchmarks/bench.cc \
	new-benchmarks/tpcc.cc

NEWBENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(NEWBENCH_SRCFILES))

all: $(O)/test

$(O)/benchmarks/%.o: benchmarks/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/masstree/%.o: benchmarks/masstree/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/new-benchmarks/%.o: new-benchmarks/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(O)/%.o: %.cc $(O)/buildstamp $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(MASSTREE_OBJFILES) : $(O)/%.o: masstree/%.cc masstree/config.h
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -include masstree/config.h -c $< -o $@

third-party/lz4/liblz4.so:
	make -C third-party/lz4 library

$(DBCORE_OBJFILES) : $(O)/dbcore/%.o: dbcore/%.cpp $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: test
test: $(O)/test

$(O)/test: $(O)/test.o $(OBJFILES) $(DBCORE_OBJFILES) $(MASSTREE_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/test $^ $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: persist_test
persist_test: $(O)/persist_test

$(O)/persist_test: $(O)/persist_test.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/persist_test $(O)/persist_test.o  $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: stats_client
stats_client: $(O)/stats_client

$(O)/stats_client: $(O)/stats_client.o
	$(CXX) -o $(O)/stats_client $(O)/stats_client.o $(LDFLAGS)

masstree/config.h: $(O)/buildstamp.masstree masstree/configure masstree/config.h.in
	rm -f $@
	cd masstree; LDFLAGS=$(MASSTREE_LDFLAGS) ./configure $(MASSTREE_CONFIG)
	if test -f $@; then touch $@; fi

masstree/configure masstree/config.h.in: masstree/configure.ac
	cd masstree && autoreconf -i && touch configure config.h.in

.PHONY: dbtest
dbtest: $(O)/benchmarks/dbtest

$(O)/benchmarks/dbtest: $(O)/benchmarks/dbtest.o $(OBJFILES) $(DBCORE_OBJFILES) $(MASSTREE_OBJFILES) $(BENCH_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/dbtest $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS)

.PHONY: kvtest
kvtest: $(O)/benchmarks/masstree/kvtest

$(O)/benchmarks/masstree/kvtest: $(O)/benchmarks/masstree/kvtest.o $(OBJFILES) $(DBCORE_OBJFILES) $(BENCH_OBJFILES)
	$(CXX) -o $(O)/benchmarks/masstree/kvtest $^ $(BENCH_LDFLAGS)

.PHONY: newdbtest
newdbtest: $(O)/new-benchmarks/dbtest

$(O)/new-benchmarks/dbtest: $(O)/new-benchmarks/dbtest.o $(OBJFILES) $(DBCORE_OBJFILES) $(MASSTREE_OBJFILES) $(NEWBENCH_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/new-benchmarks/dbtest $^ $(LDFLAGS) $(LZ4LDFLAGS)

DEPFILES := $(wildcard $(O)/*.d $(O)/*/*.d $(O)/*/*/*.d masstree/_masstree_config.d)
ifneq ($(DEPFILES),)
-include $(DEPFILES)
endif

ifeq ($(wildcard masstree/GNUmakefile.in),)
#INSTALL_MASSTREE := $(shell git submodule init; git submodule update)
endif

ifeq ($(MASSTREE_S),1)
#UPDATE_MASSTREE := $(shell cd ./`git rev-parse --show-cdup` && cur=`git submodule status --cached masstree | head -c 41 | tail -c +2` && if test -z `cd masstree; git rev-list -n1 $$cur^..HEAD 2>/dev/null`; then (echo Updating masstree... 1>&2; cd masstree; git checkout -f master >/dev/null; git pull; cd ..; git submodule update masstree); fi)
endif

ifneq ($(strip $(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)),$(strip $(DEP_MAIN_CONFIG)))
DEP_MAIN_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp; echo "DEP_MAIN_CONFIG:=$(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)" >$(O)/_main_config.d)
endif

ifneq ($(strip $(MYSQL_S)),$(strip $(DEP_BENCH_CONFIG)))
DEP_BENCH_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp.bench; echo "DEP_BENCH_CONFIG:=$(MYSQL_S)" >$(O)/_bench_config.d)
endif

ifneq ($(strip $(MASSTREE_CONFIG)),$(strip $(DEP_MASSTREE_CONFIG)))
DEP_MASSTREE_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp.masstree; echo "DEP_MASSTREE_CONFIG:=$(MASSTREE_CONFIG)" >masstree/_masstree_config.d)
endif

$(O)/buildstamp $(O)/buildstamp.bench $(O)/buildstamp.masstree:
	@mkdir -p $(@D)
	@echo >$@

.PHONY: clean
clean:
	rm -rf out-*
	make -C third-party/lz4 clean
